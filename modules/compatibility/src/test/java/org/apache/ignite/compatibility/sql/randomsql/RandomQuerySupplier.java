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

    /** Helps to take hard decisions. */
    private final Dice dice;

    /**
     * @param schema Schema based on which query will be generated.
     * @param seed Seed used to initialize random value generator.
     */
    public RandomQuerySupplier(Schema schema, int seed) {
        A.notNull(schema, "schema");

        this.schema = schema;

        rnd = new Random(seed);
        dice = new Dice(rnd);
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
        return dice.roll() <= 3
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
            2 /* at least 2 tables */ + rnd.nextInt(3) /* and 2 more optional */,
            true
        );

        assert rndQryCtx.scopeTables().size() > 1;

        Ast from;
        Ast where = null;

        boolean tblList = dice.roll() <= 3;
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

        if (dice.roll() == 1)
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

        Ast cond1 = rndTableFilter(rndQryCtx, scopeTbls.get(rnd.nextInt(scopeTbls.size())));

        if (dice.roll() <= 2)
            return cond1;

        Ast cond2 = rndTableFilter(rndQryCtx, scopeTbls.get(rnd.nextInt(scopeTbls.size())));

        return new BiCondition(cond1, cond2, dice.roll() <= 2 ? Operator.AND : Operator.OR);
    }

    /**
     * Generates random filter for specified table.
     *
     * @param rndQryCtx Context of randomised query.
     * @param tbl Table that requires filter.
     * @return Ast representing table filter.
     */
    private Ast rndTableFilter(RandomisedQueryContext rndQryCtx, TableRef tbl) {
        Ast rigthOp;
        if (dice.roll() == 1)
            rigthOp = new Const(Integer.toString(rnd.nextInt(100)));
        else {
            rigthOp = new Const("?");

            rndQryCtx.addQueryParam(rnd.nextInt(100));
        }

        return new BiCondition(
            rndNumericColumn(tbl),
            rigthOp,
            Operator.EQUALS
        );
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

        if (filteredItems.size() > 1)
            Collections.shuffle(filteredItems, rnd);

        return filteredItems.get(0);
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
}
