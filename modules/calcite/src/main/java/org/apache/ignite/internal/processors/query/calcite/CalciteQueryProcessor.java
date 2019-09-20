/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite;

import java.util.Collections;
import java.util.List;
import org.apache.calcite.config.Lex;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.fun.SqlLibrary;
import org.apache.calcite.sql.fun.SqlLibraryOperatorTableFactory;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryContext;
import org.apache.ignite.internal.processors.query.QueryEngine;
import org.apache.ignite.internal.processors.query.calcite.prepare.DistributedExecution;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlannerImpl;
import org.apache.ignite.internal.processors.query.calcite.prepare.Query;
import org.apache.ignite.internal.processors.query.calcite.prepare.QueryExecution;
import org.apache.ignite.internal.processors.query.calcite.rule.IgniteRules;
import org.apache.ignite.internal.processors.query.calcite.schema.CalciteSchemaProvider;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class CalciteQueryProcessor implements QueryEngine {
    /** */
    private final CalciteSchemaProvider schemaProvider = new CalciteSchemaProvider();

    /** */
    private GridKernalContext ctx;
    /** */
    private FrameworkConfig config;
    /** */
    private IgniteLogger log;

    /** */
    @Override public void start(GridKernalContext ctx) {
        log = ctx.log(CalciteQueryProcessor.class);

        config = Frameworks.newConfigBuilder()
                .parserConfig(SqlParser.configBuilder()
                        // Lexical configuration defines how identifiers are quoted, whether they are converted to upper or lower
                        // case when they are read, and whether identifiers are matched case-sensitively.
                        .setLex(Lex.MYSQL)
                        .build())
                // Dialects support.
                .operatorTable(SqlLibraryOperatorTableFactory.INSTANCE
                        .getOperatorTable(
                                SqlLibrary.STANDARD,
                                SqlLibrary.MYSQL))
                // Context provides a way to store data within the planner session that can be accessed in planner rules.
                .context(Contexts.of(ctx, log, this))
                // Create transform sequence.
                .programs(IgniteRules.program())
                // Custom cost factory to use during optimization
                .costFactory(null)
                .typeSystem(RelDataTypeSystem.DEFAULT)
                .build();

        ctx.internalSubscriptionProcessor()
            .registerSchemaChangeListener(schemaProvider);

        this.ctx = ctx;
    }

    @Override public void stop() {
    }

    @Override public List<FieldsQueryCursor<List<?>>> query(@Nullable QueryContext ctx, String query, Object... params) throws IgniteSQLException {
        return Collections.singletonList(prepare(context(Commons.convert(ctx), query, params)).execute());
    }

    public GridKernalContext context() {
        return ctx;
    }

    public FrameworkConfig config() {
        return config;
    }

    public IgniteLogger log() {
        return log;
    }

    /** */
    public Planner planner(RelTraitDef[] traitDefs, Context ctx) {
        FrameworkConfig cfg = Frameworks.newConfigBuilder(config())
                .defaultSchema(ctx.unwrap(SchemaPlus.class))
                .traitDefs(traitDefs)
                .context(ctx)
                .build();

        return new PlannerImpl(cfg);
    }

    private QueryExecution prepare(Context ctx) {
        return new DistributedExecution(ctx);
    }

    /** */
    private Context context(@NotNull Context ctx, String query, Object[] params) {
        Context parent = config.getContext();
        SchemaPlus schema = schemaProvider.schema();
        return Contexts.chain(parent, Contexts.of(schema, new Query(query, params)), ctx);
    }
}
