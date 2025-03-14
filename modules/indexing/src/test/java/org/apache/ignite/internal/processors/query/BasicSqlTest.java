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

package org.apache.ignite.internal.processors.query;

import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.junit.Test;

import static java.util.Arrays.asList;

/**
 * Basic simple tests for SQL.
 */
public class BasicSqlTest extends AbstractIndexingCommonTest {
    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        for (String cacheName : grid(0).cacheNames())
            grid(0).cache(cacheName).destroy();
    }

    /**
     * Check correct split sort expression that contains CAST function.

     * Steps:
     * - Creates test table and fill test data.
     * - Executes query that contains sort expression with CAST function.
     * - The query must be executed successfully.
     * - Checks query results.
     */
    @Test
    public void testSplitCastFunctionInSortExpression() {
        sql("CREATE TABLE Person (ID INTEGER PRIMARY KEY, NAME VARCHAR(100))");
        sql("INSERT INTO Person (ID, NAME) VALUES (3, 'Emma'), (2, 'Ann'), (1, 'Ed')");

        List<List<?>> res = sql("SELECT NAME, ID FROM Person ORDER BY CAST(ID AS LONG)").getAll();

        assertEquals(3, res.size());
        assertEquals("Ed", res.get(0).get(0));
        assertEquals("Ann", res.get(1).get(0));
        assertEquals("Emma", res.get(2).get(0));
    }

    /**
     */
    @Test
    public void testChooseCorrectIndexByOrderByExpression() {
        sql("CREATE TABLE TEST (ID INT PRIMARY KEY, VAL0 INT, VAL1 INT, VAL2 VARCHAR)");
        sql("CREATE INDEX IDX_VAL0 ON TEST(VAL0)");
        sql("CREATE INDEX IDX_VAL0_VAL1 ON TEST(VAL0, VAL1)");

        String plan = null;
        plan = (String)execute(new SqlFieldsQuery(
            "EXPLAIN SELECT VAL0, VAL1, VAL2 FROM TEST " +
            "WHERE VAL0 = 0 " +
            "ORDER BY VAL0, VAL1").setLocal(true)
        ).getAll().get(0).get(0);

        assertTrue("Unexpected plan: " + plan, plan.contains("IDX_VAL0_VAL1"));

        plan = (String)execute(new SqlFieldsQuery(
            "EXPLAIN SELECT VAL0, VAL1, VAL2 FROM TEST " +
            "WHERE VAL0 = 0 " +
            "ORDER BY 1, 2").setLocal(true)
        ).getAll().get(0).get(0);

        assertTrue("Unexpected plan: " + plan, plan.contains("IDX_VAL0_VAL1"));

        plan = (String)execute(new SqlFieldsQuery(
            "EXPLAIN SELECT VAL0, VAL1, VAL2 FROM TEST " +
                "WHERE VAL0 = 0 " +
                "ORDER BY VAL0, VAL1")
        ).getAll().get(0).get(0);

        assertTrue("Unexpected plan: " + plan, plan.contains("IDX_VAL0_VAL1"));
    }

    /**
     */
    @Test
    public void testIntervalOperation() {
        sql("CREATE TABLE TEST (ID INT PRIMARY KEY, VAL_INT INT, VAL_TS INT)");
        sql("CREATE INDEX IDX_VAL_TS ON TEST(VAL_TS)");

        int rows = 10;
        for (int i = 0; i < rows; ++i) {
            sql("INSERT INTO TEST (ID, VAL_INT, VAL_TS) VALUES " +
                    "(?, ?, TRUNCATE(TIMESTAMP '2015-12-31 23:59:59') - CAST(TRUNC(?) AS TIMESTAMP))",
                i, i, new Date(2015 - 1900, 11, 31 - i, 12, i, i));
        }

        List<List<?>> res = sql("SELECT ID, VAL_TS FROM TEST").getAll();

        assertEquals(rows, res.size());

        for (List<?> r : res)
            assertEquals(r.get(0), r.get(1));
    }

    /**
     */
    @Test
    public void testSysdate() {
        sql("CREATE TABLE TEST (ID INT PRIMARY KEY, VAL_INT INT, VAL_TS TIMESTAMP)");

        int rows = 100;
        for (int i = 0; i < rows; ++i) {
            sql("INSERT INTO TEST VALUES " +
                    "(?, ?, ?)",
                i, i, new Timestamp(ThreadLocalRandom.current().nextLong()));
        }

        List<List<?>> res0 = sql("SELECT ID, SYSDATE, SYSDATE() FROM TEST").getAll();

        assertEquals(rows, res0.size());

        List<List<?>> res1 = sql("SELECT VAL_TS - SYSDATE() FROM TEST").getAll();

        assertEquals(rows, res1.size());

        res1.forEach(r -> assertTrue("Invalid result type: " +
            r.get(0) + ",\n at results: " + res1, r.get(0) instanceof Long));

        List<List<?>> res2 = execute(new SqlFieldsQuery("SELECT VAL_TS - SYSDATE() FROM TEST").setLocal(true)).getAll();

        assertTrue(!res2.isEmpty());

        res2.forEach(r -> assertTrue("Invalid result type: " +
            r.get(0) + ",\n at results: " + res2, r.get(0) instanceof Long));
    }

    /**
     */
    @Test
    public void testCastIntToInterval() {
        sql("CREATE TABLE TEST (ID INT PRIMARY KEY, VAL_INT INT, VAL_TS TIMESTAMP)");

        int rows = 100;
        for (int i = 0; i < rows; ++i) {
            sql("INSERT INTO TEST VALUES " +
                    "(?, ?, ?)",
                i, i, new Timestamp(System.currentTimeMillis() + 3600 * 24 + 3600 * i));
        }

        List<List<?>> res0 = sql("SELECT CASE WHEN (VAL_TS - SYSDATE) > 0 THEN 'A' END FROM TEST").getAll();

        assertEquals(rows, res0.size());

        res0.forEach(r -> assertEquals("Invalid result type: " +
            r.get(0) + ",\n at results: " + res0, "A", r.get(0)));
    }

    /**
     */
    @Test
    public void compareDifferentTypesInInlineIndex() {
        sql("CREATE TABLE TEST0 (ID0 INT, ID1 INT, VAL INT, PRIMARY KEY(ID0, ID1)) WITH\"AFFINITY_KEY=ID0\"");
        sql("CREATE TABLE TEST1 (ID0 INT, ID1 VARCHAR, VAL INT, PRIMARY KEY(ID0, ID1)) WITH\"AFFINITY_KEY=ID0\"");
        sql("INSERT INTO TEST0 VALUES (1,  1, 0)");
        sql("INSERT INTO TEST1 VALUES (1, '1', 0)");
        sql("INSERT INTO TEST0 VALUES (2,  10000000, 0)");
        sql("INSERT INTO TEST1 VALUES (2, '10000000', 0)");

        List<List<?>> res;

        res = sql("SELECT * FROM TEST1 WHERE ID0 = 2 AND ID1 = 10000000").getAll();

        assertEquals(1, res.size());

        res = grid(0).context().query().querySqlFields(
            new SqlFieldsQuery("SELECT * FROM TEST0, TEST1 WHERE TEST0.ID0 = TEST1.ID0 AND TEST0.ID1 = TEST1.ID1")
                .setEnforceJoinOrder(true), false).getAll();

        assertEquals(2, res.size());

        res = grid(0).context().query().querySqlFields(
            new SqlFieldsQuery("SELECT * FROM TEST1, TEST0 WHERE TEST0.ID0 = TEST1.ID0 AND TEST0.ID1 = TEST1.ID1")
                .setEnforceJoinOrder(true), false).getAll();

        assertEquals(2, res.size());
    }

    @Test
    public void testOuterJoinAfterAddColumn() {
        sql("CREATE TABLE t2 (col1 INTEGER PRIMARY KEY, col2 INTEGER)");
        sql("CREATE TABLE t1( col1 INTEGER PRIMARY KEY, col2 INTEGER)");
        sql("INSERT INTO t2(col1, col2) VALUES (1,2), (2,3)");
        sql("INSERT INTO t1(col1, col2) VALUES (1,2)");

        //need to do this exucute before alteration of table to warm up all caches.
        sql("SELECT t1.col1, t2.col1 FROM t2 LEFT OUTER JOIN t1 ON t1.col1 = t2.col1 ORDER BY t2.col1").getAll();

        for (int i = 0; i < 5; i++) {
            sql("ALTER TABLE t1 ADD COLUMN col3 INTEGER");

            String query = "SELECT t1.col1, t1.col2, t1.col3, t2.col1, t2.col2 FROM t2 LEFT OUTER JOIN t1 "
                    + "ON t1.col1 = t2.col1 ORDER BY t2.col1";
            List<List<?>> all = sql(query).getAll();
            assertEquals(2, all.size());
            assertEqualsCollections(asList(1, 2, null, 1, 2), all.get(0));
            assertEqualsCollections(asList(null, null, null, 2, 3), all.get(1));

            sql("ALTER TABLE t1 DROP COLUMN col3");
        }
    }

    /**
     * @param sql SQL query.
     * @param args Query parameters.
     * @return Results cursor.
     */
    private FieldsQueryCursor<List<?>> sql(String sql, Object... args) {
        return execute(new SqlFieldsQuery(sql).setArgs(args));
    }

    /**
     * @param qry Query.
     * @return Results cursor.
     */
    private FieldsQueryCursor<List<?>> execute(SqlFieldsQuery qry) {
        return grid(0).context().query().querySqlFields(qry, false);
    }
}
