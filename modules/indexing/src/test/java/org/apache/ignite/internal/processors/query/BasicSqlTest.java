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

import java.util.Collections;
import java.util.List;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
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
    public void testQ() {
        grid(0).createCache(new CacheConfiguration("INT2")
            .setSqlSchema("PUBLIC")
            .setQueryEntities(Collections.singleton(
                new QueryEntity(TestKey2Integers.class, Value.class)
                    .setTableName("TEST2INT")
                )
            )
        );

        grid(0).createCache(new CacheConfiguration("INT8")
            .setSqlSchema("PUBLIC")
            .setQueryEntities(Collections.singleton(
                new QueryEntity(TestKey8Integers.class, Value.class)
                    .setTableName("TEST8INT")
                )
            )
        );

        System.out.println("asd");
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

    public static class TestKey2Integers {
        /** */
        @QuerySqlField
        private final int id0;

        @QuerySqlField
        private final int id1;

        /** */
        public TestKey2Integers(int key) {
            this.id0 = 0;
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

    /** */
    public static class Value {
        /** */
        @QuerySqlField
        private final int valInt;

        @QuerySqlField
        private final String valStr;

        /** */
        public Value(int key) {
            this.valInt = key;
            this.valStr = "val_str" + key;
        }
    }

    /** */
    public static class ValueIndexed {
        /** */
        @QuerySqlField(index = true)
        private final int valInt;

        @QuerySqlField(index = true)
        private final String valStr;

        /** */
        public ValueIndexed(int key) {
            this.valInt = key;
            this.valStr = "val_str" + key;
        }
    }
}
