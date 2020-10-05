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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Test;

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
    public void testDbg() throws Exception {
        sql("CREATE TABLE TEST2 (ID0 INT, ID1 INT, VALINT INT, VALSTR VARCHAR, " +
            "PRIMARY KEY (ID0, ID1))");

        sql("CREATE TABLE TEST8 (ID0 INT, ID1 INT, ID2 INT, " +
            "ID3 INT, ID4 INT, ID5 INT, ID6 INT, ID7 INT, VALINT INT, VALSTR VARCHAR, " +
            "PRIMARY KEY (ID0, ID1, ID2, ID3, ID4, ID5, ID6, ID7)) ");

        sql("INSERT INTO TEST2 (ID0, ID1, VALINT, VALSTR) VALUES (0, 0, 0, '0')");
        sql("INSERT INTO TEST2 (ID0, ID1, VALINT, VALSTR) VALUES (1, 1, 1, '1')");
        sql("MERGE INTO TEST2 (ID0, ID1, VALINT, VALSTR) VALUES (0, 0, 2, '2')");

        sql("INSERT INTO TEST8 (ID0, ID1, ID2, ID3, ID4, ID5, ID6, ID7, VALINT, VALSTR) VALUES (0, 0, 0, 0, 0, 0, 0, 0,  0, '0')");
        sql("INSERT INTO TEST8 (ID0, ID1, ID2, ID3, ID4, ID5, ID6, ID7, VALINT, VALSTR) VALUES (1, 1, 1, 1, 1, 1, 1, 1,  1, '1')");
        sql("MERGE INTO TEST8 (ID0, ID1, ID2, ID3, ID4, ID5, ID6, ID7, VALINT, VALSTR) VALUES (1, 1, 1, 1, 1, 1, 1, 1,  2, '2')");

        List<BinaryObject> objs = new ArrayList<>();
        Set<Integer> hashes2 = new HashSet<>();
        Set<Integer> hashes8 = new HashSet<>();

        for (int i = 0; i < 1000_000; ++i) {
            BinaryObject bob2 = grid(0).binary().toBinary(new TestKey2Integers(i));
            BinaryObject bob8 = grid(0).binary().toBinary(new TestKey8Integers(i));

            hashes2.add(bob2.hashCode());
            hashes8.add(bob8.hashCode());
        }

        System.out.println("+++ 2 " + hashes2.size());
        System.out.println("+++ 8 " + hashes8.size());
    }

    /**
     * @param sql SQL query.
     * @param args Query parameters.
     * @return Results cursor.
     */
    private FieldsQueryCursor<List<?>> sql(String sql, Object... args) {
        return grid(0).context().query().querySqlFields(new SqlFieldsQuery(sql)
            .setArgs(args), false);
    }

    /**
     * @param qry Query.
     * @return Results cursor.
     */
    private FieldsQueryCursor<List<?>> execute(SqlFieldsQuery qry) {
        return grid(0).context().query().querySqlFields(qry, false);
    }

    /** */
    public static class TestKey2Integers {
        /** */
        @QuerySqlField
        private final int id0;

        @QuerySqlField
        private final int id1;

        /** */
        public TestKey2Integers(int key) {
            this.id0 = key / 1000;
            this.id1 = key;
        }
    }

    /** */
    public static class TestKeyHugeStringAndInteger {
        /** Prefix. */
        private static final String PREFIX = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" +
            "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" +
            "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

        /** */
        @QuerySqlField
        private final String id0;

        @QuerySqlField
        private final int id1;

        /** */
        public TestKeyHugeStringAndInteger(int key) {
            this.id0 = PREFIX + key;
            this.id1 = key;
        }
    }

    /** */
    public static class TestKey8Integers {
        /** */
        @QuerySqlField
        private final int id0;

        @QuerySqlField
        private final int id1;

        @QuerySqlField
        private final int id2;

        @QuerySqlField
        private final int id3;

        @QuerySqlField
        private final int id4;

        @QuerySqlField
        private final int id5;

        @QuerySqlField
        private final int id6;

        @QuerySqlField
        private final int id7;

        /** */
        public TestKey8Integers(int key) {
            this.id0 = 0;
            this.id1 = key / 100_000;
            this.id2 = key / 10_000;
            this.id3 = key / 1_000;
            this.id4 = key / 1000;
            this.id5 = key / 100;
            this.id6 = key / 10;
            this.id7 = key;
        }
    }
}
