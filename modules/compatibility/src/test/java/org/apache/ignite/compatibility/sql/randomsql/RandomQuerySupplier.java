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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.ignite.compatibility.sql.randomsql.ast.Ast;
import org.apache.ignite.compatibility.sql.randomsql.ast.BiCondition;
import org.apache.ignite.compatibility.sql.randomsql.ast.ColumnRef;
import org.apache.ignite.compatibility.sql.randomsql.ast.Const;
import org.apache.ignite.compatibility.sql.randomsql.ast.ExistsCondition;
import org.apache.ignite.compatibility.sql.randomsql.ast.InCondition;
import org.apache.ignite.compatibility.sql.randomsql.ast.InnerJoin;
import org.apache.ignite.compatibility.sql.randomsql.ast.Operator;
import org.apache.ignite.compatibility.sql.randomsql.ast.Select;
import org.apache.ignite.compatibility.sql.randomsql.ast.SubSelect;
import org.apache.ignite.compatibility.sql.randomsql.ast.TableList;
import org.apache.ignite.compatibility.sql.randomsql.ast.TableRef;
import org.apache.ignite.compatibility.sql.runner.QueryWithParams;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;

/**
 * Supplier which generates random SELECT queries.
 */
public class RandomQuerySupplier implements Supplier<QueryWithParams> {
    /** */
    private static final Predicate<ColumnRef> NUMERICAL_COLUMN_FILTER = col -> Number.class.isAssignableFrom(col.typeClass());

    /** */
    private static final Ast[] EMPTY_AST_ARR = new Ast[0];

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
        RandomisedQueryContext rndQryCtx = new RandomisedQueryContext();

        Select select = rndSelect(rndQryCtx);

        StringBuilder sb = new StringBuilder();

        select.writeTo(sb);

        return F.isEmpty(rndQryCtx.queryParams())
            ? new QueryWithParams(sb.toString())
            : new QueryWithParams(sb.toString(), rndQryCtx.queryParams());
    }

    /**
     * Generates random SELECT expression.
     *
     * @param rndQryCtx Context of randomised query.
     * @return Ast representing SELECT expression.
     */
    private Select rndSelect(RandomisedQueryContext rndQryCtx) {
        return rndWithBound(2) == 1
            ? rndSingleTableSelect(rndQryCtx)
            : rndMultiTableSelect(rndQryCtx);
    }

    /**
     * Generates random SELECT expression from several tables.
     *
     * @param rndQryCtx Context of randomised query.
     * @return Ast representing SELECT expression.
     */
    private Select rndMultiTableSelect(RandomisedQueryContext rndQryCtx) {
        pickRndTables(
            rndQryCtx,
            2,
            true
        );

        assert rndQryCtx.scopeTables().size() > 1;

        Ast from;
        Ast where = null;

        boolean tblList = rndWithBound(2) == 1;
        from = tblList ? new TableList(rndQryCtx.scopeTables()) : rndQryCtx.scopeTables().get(0);

        List<TableRef> connScope = new ArrayList<>(rndQryCtx.scopeTables().size());

        // scope of tables that should be connected to each other
        connScope.add(rndQryCtx.scopeTables().get(0));

        for (int i = 1; i < rndQryCtx.scopeTables().size(); i++) {
            TableRef anotherTbl = rndQryCtx.scopeTables().get(i);

            Ast cond = new BiCondition(
                pickRndItem(pickRndItem(connScope).cols(), NUMERICAL_COLUMN_FILTER),
                pickRndItem(anotherTbl.cols(), NUMERICAL_COLUMN_FILTER),
                Operator.EQUALS
            );

            connScope.add(anotherTbl); // add a table to the connection scope AFTER we created the condition

            if (tblList)
                where = where != null ? new BiCondition(where, cond, Operator.AND) : cond;
            else
                from = new InnerJoin(from, anotherTbl, cond);
        }

        if (!tblList)
            where = new Const("TRUE");

        return new Select(rndColList(rndQryCtx))
            .from(from)
            .where(new BiCondition(where, rndQueryFilter(rndQryCtx), Operator.AND));
    }

    /**
     * Generates random column list.
     *
     * @param rndQryCtx Context of randomised query.
     * @return Ast representing column list.
     */
    private Ast[] rndColList(RandomisedQueryContext rndQryCtx) {
        Ast[] cols = EMPTY_AST_ARR;

        if (rndWithBound(6) == 1)
            return cols; // select * from...

        List<ColumnRef> cols0 = new ArrayList<>();

        rndQryCtx.scopeTables().forEach(t -> cols0.addAll(t.cols()));

        Collections.shuffle(cols0, rnd);

        cols = cols0.subList(0, 1 + rnd.nextInt(2)).toArray(cols);

        return cols;
    }

    /**
     * Generates random SELECT expression from single tables.
     *
     * @param rndQryCtx Context of randomised query.
     * @return Ast representing SELECT expression.
     */
    private Select rndSingleTableSelect(RandomisedQueryContext rndQryCtx) {
        pickRndTables(rndQryCtx, 1);

        assert rndQryCtx.scopeTables().size() == 1;

        return new Select(rndColList(rndQryCtx))
            .from(new TableList(rndQryCtx.scopeTables()))
            .where(rndQueryFilter(rndQryCtx));
    }

    /**
     * Generates random query filter.
     *
     * @param rndQryCtx Context of randomised query.
     * @return Ast representing query filter.
     */
    private Ast rndQueryFilter(RandomisedQueryContext rndQryCtx) {
        List<TableRef> scopeTbls = rndQryCtx.scopeTables();

        // if current query is a subquery, then let's make it correlated,
        // because for now we are interested only in corellated subqueries
        RandomisedQueryContext parentCtx = rndQryCtx.parentContext();
        if (parentCtx != null) {
            return new BiCondition(
                pickRndItem(pickRndItem(scopeTbls).cols(), NUMERICAL_COLUMN_FILTER),
                pickRndItem(pickRndItem(parentCtx.scopeTables()).cols(), NUMERICAL_COLUMN_FILTER),
                Operator.EQUALS
            );
        }

        Ast cond1 = rndTableFilter(rndQryCtx, pickRndItem(scopeTbls));

        if (rndWithBound(3) == 1)
            return cond1;

        Ast cond2 = rndTableFilter(rndQryCtx, pickRndItem(scopeTbls));

        return new BiCondition(cond1, cond2, rndWithBound(3) == 1 ? Operator.AND : Operator.OR);
    }

    /**
     * Generates random filter for specified table.
     *
     * @param rndQryCtx Context of randomised query.
     * @param tbl Table that requires filter.
     * @return Ast representing table filter.
     */
    private Ast rndTableFilter(RandomisedQueryContext rndQryCtx, TableRef tbl) {
        switch (rndWithBound(4)) {
            case 0: {
                Ast rigthOp;
                if (rndWithBound(6) == 1)
                    rigthOp = new Const(Integer.toString(rndParamValue()));
                else {
                    rigthOp = new Const("?");

                    rndQryCtx.addQueryParam(rndParamValue());
                }

                return new BiCondition(
                    pickRndItem(tbl.cols(), NUMERICAL_COLUMN_FILTER),
                    rigthOp,
                    Operator.EQUALS
                );
            }
            case 1: {
                Ast[] conds = new Ast[2];
                Ast leftOp = pickRndItem(tbl.cols(), NUMERICAL_COLUMN_FILTER);

                for (int i = 0; i < 2; i++) {
                    Ast rigthOp;
                    if (rndWithBound(6) == 1)
                        rigthOp = new Const(Integer.toString(rndParamValue()));
                    else {
                        rigthOp = new Const("?");

                        rndQryCtx.addQueryParam(rndParamValue());
                    }

                    conds[i] = new BiCondition(
                        leftOp,
                        rigthOp,
                        Operator.EQUALS
                    );
                }

                return new BiCondition(
                    conds[0],
                    conds[1],
                    Operator.OR
                );
            }
            case 2: {
                int inArrSize = 1 + rndWithBound(6);

                Ast[] inArr = new Ast[inArrSize];

                for (int i = 0; i < inArrSize; i++)
                    inArr[i] = new Const(Integer.toString(rndParamValue()));

                return new InCondition(
                    pickRndItem(tbl.cols(), NUMERICAL_COLUMN_FILTER),
                    inArr
                );
            }
            case 3:
                return new ExistsCondition(
                    new SubSelect(rndSingleTableSelect(new RandomisedQueryContext(rndQryCtx)))
                );

            default:
                throw new IllegalStateException();
        }
    }

    /** */
    private int rndParamValue() {
        return rnd.nextInt(100);
    }

    /**
     * Picks N random tables (or less if there are not enough tables in the schema)
     * from the schema and adds them to the scope.
     *
     * @param rndQryCtx Random query context.
     * @param tblCnt Tables count.
     */
    private void pickRndTables(RandomisedQueryContext rndQryCtx, int tblCnt) {
        pickRndTables(rndQryCtx, tblCnt, false);
    }

    /**
     * Picks N random tables (or less if there are not enough tables in the schema)
     * from the schema and adds them to the scope.
     *
     * @param rndQryCtx Random query context.
     * @param tblCnt Tables count.
     * @param allowDublicates Whether to allow dublicates or not.
     */
    private void pickRndTables(RandomisedQueryContext rndQryCtx, int tblCnt, boolean allowDublicates) {
        List<Table> tbls = new ArrayList<>(schema.tables());

        if (allowDublicates)
            tbls.addAll(schema.tables()); // add all tables again to get duplicates

        Collections.shuffle(tbls, rnd);

        for (Table tbl : tbls.subList(0, Math.min(tblCnt, tbls.size()))) {
            String alias = String.valueOf(tbl.name().toLowerCase().charAt(0)) + aliasIdGen.incrementAndGet();

            rndQryCtx.addScopeTable(new TableRef(tbl, alias));
        }
    }

    /**
     * Picks random item from provided collection that meet specified condition.
     *
     * @param items Collection for picking item.
     * @param filter Required condition.
     * @return Random item or {@code null} if collection is empty or there is no
     * item that meet condition.
     */
    private <T> T pickRndItem(Collection<T> items, Predicate<T> filter) {
        if (F.isEmpty(items))
            return null;

        List<T> filteredItems = items.stream()
            .filter(filter)
            .collect(Collectors.toList());

        if (F.isEmpty(filteredItems))
            return null;

        return filteredItems.get(rnd.nextInt(filteredItems.size()));
    }

    /**
     * Picks random item from provided collection.
     *
     * @param items Collection for picking item.
     * @return Random item or {@code null} if collection is empty.
     */
    private <T> T pickRndItem(Collection<T> items) {
        return pickRndItem(items, i -> true);
    }

    /** */
    private int rndWithBound(int bound) {
        return rnd.nextInt(bound);
    }
}
