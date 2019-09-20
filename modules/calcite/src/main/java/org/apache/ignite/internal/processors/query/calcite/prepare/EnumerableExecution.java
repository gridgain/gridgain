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
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableInterpretable;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.runtime.Bindable;
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

import static org.apache.calcite.adapter.enumerable.EnumerableRel.Prefer.ARRAY;

/**
 *
 */
public class EnumerableExecution implements QueryExecution {
    private final Context ctx;

    public EnumerableExecution(Context ctx) {
        this.ctx = ctx;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public FieldsQueryCursor<List<?>> execute() {
        CalciteQueryProcessor proc = ctx.unwrap(CalciteQueryProcessor.class);
        Query query = ctx.unwrap(Query.class);

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
                .replace(EnumerableConvention.INSTANCE)
                .simplify();

            rel = planner.transform(IgniteRules.IGNITE_ENUMERABLE_PROGRAM_IDX, desired, rel);
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

        EnumerableRel rel = (EnumerableRel) relRoot.rel;

        LinkedHashMap<String, Object> params = new LinkedHashMap<>();

        params.put("_conformance", proc.config().getParserConfig().conformance());

        Bindable<Object[]> bindable = EnumerableInterpretable.toBindable(params, null, rel, ARRAY);

        return new ListFieldsQueryCursor<>(rel.getRowType(),
            bindable.bind(new DataContextImpl(query.params(params), ctx)), Arrays::asList);
    }
}
