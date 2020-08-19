/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.compatibility.sql.randomsql;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.ignite.compatibility.sql.randomsql.ast.Ast;
import org.apache.ignite.compatibility.sql.randomsql.ast.BiCondition;
import org.apache.ignite.compatibility.sql.randomsql.ast.ColumnRef;
import org.apache.ignite.compatibility.sql.randomsql.ast.Const;
import org.apache.ignite.compatibility.sql.randomsql.ast.InnerJoin;
import org.apache.ignite.compatibility.sql.randomsql.ast.Operator;
import org.apache.ignite.compatibility.sql.randomsql.ast.Select;
import org.apache.ignite.compatibility.sql.randomsql.ast.TableList;
import org.apache.ignite.compatibility.sql.randomsql.ast.TableRef;
import org.apache.ignite.compatibility.sql.runner.QueryWithParams;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;

/**
 * Supplier which generates random SELECT queries.
 */
public class RandomQuerySupplier implements Supplier<QueryWithParams> {
    /** Generator of unique alias ID. */
    private final AtomicLong aliasIdGen = new AtomicLong();

    /** */
    private final Schema schema;

    /**
     * Instance of {@link Random}. Should be used for getting every random
     * value to ensure repeatability in troubleshooting.
     */
    private final Random rnd;

    /**
     * @param schema Schema based on which query will be generated.
     * @param seed Seed used to initialize random value generator.
     */
    public RandomQuerySupplier(Schema schema, int seed) {
        A.notNull(schema, "schema");

        this.schema = schema;

        rnd = new Random(seed);
    }

    /** {@inheritDoc} */
    @Override public QueryWithParams get() {
        RandomisedQueryContext rndQryCtx = new RandomisedQueryContext(schema);

        Select select = rndSelect(rndQryCtx);

        StringBuilder sb = new StringBuilder();

        select.writeTo(sb);

        return F.isEmpty(rndQryCtx.queryParams())
            ? new QueryWithParams(sb.toString())
            : new QueryWithParams(sb.toString(), rndQryCtx.queryParams());
    }

    /**
     * Pick up to 4 random tables from provided schema. Some tables in
     * result list may appear twice.
     *
     * @param schema Schema.
     * @return List of random tables from schema (perhaps with duplicates).
     */
    private List<TableRef> rndTables(Schema schema) {
        int tblCnt = 1;

        for (int i = 0; i < 3; i++) {
            if (rndWithRatio(2))
                tblCnt++;
        }

        List<Table> tbls = new ArrayList<>(schema.tables());

        tbls.addAll(schema.tables()); // add all tables again to get duplicates

        Collections.shuffle(tbls, rnd);

        List<TableRef> res = new ArrayList<>(tblCnt);

        for (Table tbl : tbls.subList(0, tblCnt)) {
            String alias = String.valueOf(tbl.name().toLowerCase().charAt(0)) + aliasIdGen.incrementAndGet();

            res.add(new TableRef(tbl, alias));
        }

        return res;
    }

    /**
     * Generates random WHERE expression for provided tables.
     *
     * @param rndQryCtx Context of randomised query.
     * @return Ast representing WHERE expression.
     */
    private Ast rndWhereClause(RandomisedQueryContext rndQryCtx) {
        List<TableRef> tbls = rndQryCtx.scopeTables();

        assert !tbls.isEmpty();

        List<TableRef> tbls0 = new ArrayList<>(tbls);

        Ast cond;

        if (rndWithRatio(4))
            cond = new Const("TRUE");
        else {
            TableRef tbl = tbls0.get(rnd.nextInt(tbls0.size()));

            Ast rightOp;
            if (rndWithRatio(5))
                rightOp = new Const(Integer.toString(rnd.nextInt(100)));
            else {
                rightOp = new Const("?");

                rndQryCtx.addQueryParam(rnd.nextInt(100));
            }

            cond = new BiCondition(
                rndNumericColumn(tbl),
                rightOp,
                Operator.EQUALS
            );
        }

        // all tables connected one by one with an AND expression
        while (tbls0.size() >= 2) {
            TableRef left = tbls0.remove(0);
            TableRef right = tbls0.get(0);

            cond = new BiCondition(
                cond,
                new BiCondition(rndNumericColumn(left), rndNumericColumn(right), Operator.EQUALS),
                Operator.AND
            );
        }

        return cond;
    }

    /**
     * Generates random SELECT expression.
     *
     * @param rndQryCtx Context of randomised query.
     * @return Ast representing SELECT expression.
     */
    private Select rndSelect(RandomisedQueryContext rndQryCtx) {
        Ast from = rndFrom(rndQryCtx);
        Ast where = from instanceof TableList ? rndWhereClause(rndQryCtx) : new Const("TRUE");

        return new Select().from(from).where(where);
    }

    /**
     * Generates random FROM expression.
     *
     * @param rndQryCtx Context of randomised query.
     * @return Ast representing FROM expression.
     */
    private Ast rndFrom(RandomisedQueryContext rndQryCtx) {
        List<TableRef> tbls = rndTables(rndQryCtx.schema());

        assert !F.isEmpty(tbls);

        if (tbls.size() == 1 || rndWithRatio(5)) {
            tbls.forEach(rndQryCtx::addScopeTable);

            return new TableList(tbls);
        }

        TableRef leftTbl = tbls.get(0);

        Ast from = leftTbl;

        rndQryCtx.addScopeTable(leftTbl);

        for (int i = 1; i < tbls.size(); i++) {
            TableRef rightTbl = tbls.get(i);

            from = new InnerJoin(from, rightTbl,
                new BiCondition(rndNumericColumn(rndScopeTable(rndQryCtx)), rndNumericColumn(rightTbl), Operator.EQUALS));

            rndQryCtx.addScopeTable(rightTbl); // add a table to the scope AFTER we created the join condition
        }

        return from;
    }

    /**
     * Returns random table which was used in FROM expression.
     *
     * @param rndQryCtx Context of randomised query.
     */
    private TableRef rndScopeTable(RandomisedQueryContext rndQryCtx) {
        if (F.isEmpty(rndQryCtx.scopeTables()))
            return null;

        List<TableRef> tbls = new ArrayList<>(rndQryCtx.scopeTables());

        if (tbls.size() > 1)
            Collections.shuffle(tbls, rnd);

        return tbls.get(0);
    }

    /**
     * Returns random column from provided table.
     *
     * @param tbl Table.
     * @return Random column of numeric type.
     */
    private ColumnRef rndNumericColumn(TableRef tbl) {
        // for now we could work only with numeric columns
        List<ColumnRef> numCols = tbl.cols().stream()
            .filter(col -> Number.class.isAssignableFrom(col.typeClass()))
            .collect(Collectors.toList());

        assert !numCols.isEmpty();

        if (numCols.size() > 1)
            Collections.shuffle(numCols, rnd);

        return numCols.get(0);
    }

    /**
     * Returns {@code true} with probability that calculated
     * as {@code 1/r} where {@code r} - provided ratio.
     *
     * @param r Ratio.
     * @return {@code true} with desired probability.
     */
    private boolean rndWithRatio(int r) {
        return rnd.nextInt(r) == 0;
    }
}
