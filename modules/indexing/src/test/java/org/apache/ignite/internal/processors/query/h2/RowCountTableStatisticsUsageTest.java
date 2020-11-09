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

package org.apache.ignite.internal.processors.query.h2;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.gridgain.internal.h2.util.Permutations;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 * Test cases to ensure that proper join order is chosen by H2 optimizer when row count statistics is collected.
 */
@RunWith(Parameterized.class)
public class RowCountTableStatisticsUsageTest extends TableStatisticsAbstractTest {
    /** */
    @Parameterized.Parameter(0)
    public CacheMode cacheMode;

    /**
     * @return Test parameters.
     */
    @Parameterized.Parameters(name = "cacheMode={0}")
    public static Collection parameters() {
        return Arrays.asList(new Object[][] {
            { REPLICATED },
           // { PARTITIONED }, // TBD!!!
        });
    }

    @Override protected void beforeTestsStarted() throws Exception {
        Ignite node = startGridsMultiThreaded(1); // TBD 1!!!!!

        node.getOrCreateCache(DEFAULT_CACHE_NAME);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        runSql("DROP TABLE IF EXISTS big");
        runSql("DROP TABLE IF EXISTS med");
        runSql("DROP TABLE IF EXISTS small");

        runSql("CREATE TABLE big (a INT PRIMARY KEY, b INT, c INT) WITH \"TEMPLATE=" + cacheMode + "\"");
        runSql("CREATE TABLE med (a INT PRIMARY KEY, b INT, c INT) WITH \"TEMPLATE=" + cacheMode + "\"");
        runSql("CREATE TABLE small (a INT PRIMARY KEY, b INT, c INT) WITH \"TEMPLATE=" + cacheMode + "\"");

        runSql("CREATE INDEX big_b ON big(b)");
        runSql("CREATE INDEX med_b ON med(b)");
        runSql("CREATE INDEX small_b ON small(b)");

        runSql("CREATE INDEX big_c ON big(c)");
        runSql("CREATE INDEX med_c ON med(c)");
        runSql("CREATE INDEX small_c ON small(c)");

        for (int i = 0; i < BIG_SIZE; i++)
            runSql("INSERT INTO big(a, b, c) VALUES(" + i + "," + i + "," + i % 10 + ")");

        for (int i = 0; i < MED_SIZE; i++)
            runSql("INSERT INTO med(a, b, c) VALUES(" + i + "," + i + "," + i % 10 + ")");

        for (int i = 0; i < SMALL_SIZE; i++)
            runSql("INSERT INTO small(a, b, c) VALUES(" + i + "," + i + "," + i % 10 + ")");

        updateStatistics("BIG", "MED", "SMALL");
    }

    /**
     *
     */
    @Test
    public void compareJoinsWithConditionsOnBothTables() {
        String sql = "SELECT COUNT(*) FROM t1 JOIN t2 ON t1.c = t2.c " +
            "WHERE t1.b >= 0 AND t2.b >= 0";

        checkOptimalPlanChosenForDifferentJoinOrders(grid(0), sql, "small", "big");
    }

    /**
     *
     */
    @Test
    public void compareJoinsWithoutConditions() {
        //runSql("select * from BIG");
        String sql = "SELECT COUNT(*) FROM t1 JOIN t2 ON t1.c = t2.c";

        checkOptimalPlanChosenForDifferentJoinOrders(grid(0), sql, "big", "small");
    }

    @Test
    public void compareJoinsWirhoutCond() {
        runLocalExplainAnalyze(grid(0), false, "SELECT COUNT(*) FROM small JOIN big ON small.c = big.c");
    }

    @Test
    public void selectWithCond() {
        runLocalExplainAnalyze(grid(0), false, "SELECT COUNT(*) FROM small " +
                "where not c is null");
                //"where c > 23 and c < 233 and c is null and c = null");
    }

    @Test
    public void selectWithCond2() {
        runSql("update small set c = null where a < 3");

        System.err.println("c = null");
        String sql = "SELECT COUNT(*) FROM small " +
                "where  c = null";

        printQueryResults(runSqlRes(sql));

        System.err.println("c is null");
        String sql2 = "SELECT COUNT(*) FROM small " +
                "where  c is null";
        printQueryResults(runSqlRes(sql2));

        System.err.println("1=1");
        String sql3 = "SELECT COUNT(*) FROM small " +
                "where  1=1";

        printQueryResults(runSqlRes(sql3));
        //runLocalExplainAnalyze(grid(0), false, "SELECT COUNT(*) FROM small " +
        //        "where c > 23 and c < 233 and c is null and c = null");
    }

    private List<?> runSqlRes(String sql) {
        return grid(0).cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery(sql)).getAll();
    }

    public static void printQueryResults(List<?> res) {
        System.out.println("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
        if (res == null || res.isEmpty())
            System.out.println("Query result set is empty.");
        else {
            for (Object row : res) {
                if (row instanceof List) {
                    System.out.print("(");

                    List<?> l = (List)row;

                    for (int i = 0; i < l.size(); i++) {
                        Object o = l.get(i);

                        if (o instanceof Double || o instanceof Float)
                            System.out.printf("%.2f", o);
                        else
                            System.out.print(l.get(i));

                        if (i + 1 != l.size())
                            System.out.print(',');
                    }

                    System.out.println(')');
                }
                else
                    System.out.println("  " + row);
            }
        }
        System.out.println("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
    }


    /**
     *
     */
    @Test
    public void compareJoinsConditionSingleTable() {
        final String sql = "SELECT * FROM t1 JOIN t2 ON t1.c = t2.c WHERE t1.b >= 0";

        checkOptimalPlanChosenForDifferentJoinOrders(grid(0), sql, "big", "small");
    }

    /**
     *
     */
    @Test
    public void compareJoinsThreeTablesNoConditions() {
        String sql = "SELECT * FROM t1 JOIN t2 ON t1.c = t2.c JOIN t3 ON t3.c = t2.c ";

        checkOptimalPlanChosenForDifferentJoinOrders(grid(0), sql, "big", "med", "small");
        checkOptimalPlanChosenForDifferentJoinOrders(grid(0), sql, "small", "big", "med");
        checkOptimalPlanChosenForDifferentJoinOrders(grid(0), sql, "small", "med", "big");
        checkOptimalPlanChosenForDifferentJoinOrders(grid(0), sql, "med", "big", "small");
    }

    /**
     *
     */
    @Test
    public void compareJoinsThreeTablesConditionsOnAllTables() {
        String sql = "SELECT * FROM t1 JOIN t2 ON t1.c = t2.c JOIN t3 ON t3.c = t2.c " +
            " WHERE t1.b >= 0 AND t2.b >= 0 AND t3.b >= 0";

        checkOptimalPlanChosenForDifferentJoinOrders(grid(0), sql, "big", "med", "small");
        checkOptimalPlanChosenForDifferentJoinOrders(grid(0), sql, "small", "big", "med");
        checkOptimalPlanChosenForDifferentJoinOrders(grid(0), sql, "small", "med", "big");
        checkOptimalPlanChosenForDifferentJoinOrders(grid(0), sql, "med", "big", "small");
    }

    /**
     * Checks if statistics is updated when table size is changed.
     */
    @Test
    public void checkUpdateStatisticsOnTableSizeChange() {
        // t2 size is bigger than t1
        String sql = "SELECT COUNT(*) FROM t2 JOIN t1 ON t1.c = t2.c " +
            "WHERE t1.b > " + 0 + " AND t2.b > " + 0;

        checkOptimalPlanChosenForDifferentJoinOrders(grid(0), sql, "small", "big");

        // Make small table bigger than a big table
        for (int i = SMALL_SIZE; i < BIG_SIZE * 2; i++)
            runSql("INSERT INTO small(a, b, c) VALUES(" + i + "," + i + "," + i % 10 + ")");

        // TODO implement some auto update mechanism
        updateStatistics("BIG", "MED", "SMALL");

        // t1 size is now bigger than t2
        sql = "SELECT COUNT(*) FROM t1 JOIN t2 ON t1.c = t2.c " +
            "WHERE t1.b > " + 0 + " AND t2.b > " + 0;

        checkOptimalPlanChosenForDifferentJoinOrders(grid(0), sql, "small", "big");
    }

    /**
     *
     */
    @Test
    public void testStatisticsAfterRebalance() throws Exception {
        String sql = "SELECT COUNT(*) FROM t1 JOIN t2 ON t1.c = t2.c";

        checkOptimalPlanChosenForDifferentJoinOrders(grid(0), sql, "big", "small");

        startGrid(3);

        try {
            awaitPartitionMapExchange();

            grid(3).context()
                .cache()
                .context()
                .cacheContext(CU.cacheId("SQL_PUBLIC_BIG"))
                .preloader()
                .rebalanceFuture()
                .get(10_000);

            // TODO implement update on rebalance
            updateStatistics("BIG", "MED", "SMALL");

            checkOptimalPlanChosenForDifferentJoinOrders(grid(1), sql, "big", "small");
            checkOptimalPlanChosenForDifferentJoinOrders(grid(0), sql, "big", "small");
        }
        finally {
            stopGrid(3);
        }
    }
}