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

package org.apache.ignite.internal.processors.query.calcite.prepare;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import org.apache.calcite.interpreter.BindableConvention;
import org.apache.calcite.interpreter.BindableRel;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor;
import org.apache.ignite.internal.processors.query.calcite.rule.IgniteRules;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.processors.query.calcite.util.ListFieldsQueryCursor;

/**
 *
 */
class BindableExecution implements QueryExecution {
    /** */
    private final Context ctx;

    /** */
    BindableExecution(Context ctx) {
        this.ctx = ctx;
    }

    /** {@inheritDoc} */
    @Override public FieldsQueryCursor<List<?>> execute() {
        CalciteQueryProcessor proc = Objects.requireNonNull(ctx.unwrap(CalciteQueryProcessor.class));
        Query query = Objects.requireNonNull(ctx.unwrap(Query.class));

        RelTraitDef[] traitDefs = {
            ConventionTraitDef.INSTANCE,
            RelCollationTraitDef.INSTANCE
        };

        RelRoot relRoot;

        try (Planner planner = proc.planner(traitDefs, ctx)) {
            SqlNode sqlNode = planner.parse(query.sql());
            sqlNode = planner.validate(sqlNode);
            relRoot = planner.rel(sqlNode);
            RelNode rel = relRoot.rel;

            RelTraitSet desired = rel.getTraitSet()
                .replace(relRoot.collation)
                .replace(BindableConvention.INSTANCE)
                .simplify();

            rel = planner.transform(IgniteRules.IGNITE_BINDABLE_PROGRAM_IDX, desired, rel);

            relRoot = relRoot.withRel(rel).withKind(sqlNode.getKind());
        } catch (SqlParseException | ValidationException e) {
            String msg = "Failed to parse query.";

            Commons.log(ctx).error(msg, e);

            throw new IgniteSQLException(msg, IgniteQueryErrorCode.PARSING, e);
        } catch (RelConversionException e) {
            String msg = "Failed to create logical query execution tree.";

            Commons.log(ctx).error(msg, e);

            throw new IgniteSQLException(msg, IgniteQueryErrorCode.UNKNOWN, e);
        }

        BindableRel rel = (BindableRel) relRoot.rel;

        LinkedHashMap<String, Object> params = new LinkedHashMap<>();

        params.put("_conformance", proc.config().getParserConfig().conformance());

        return new ListFieldsQueryCursor<>(rel.getRowType(),
            rel.bind(new DataContextImpl(query.params(params), ctx)), Arrays::asList);
    }
}
