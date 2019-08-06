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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Test;

/**
 * Tests by simple benchmark hash join.
 */
public class HashJoinQueryTest extends AbstractIndexingCommonTest {
    /** Keys counts at the RIGHT table. */
    private static final int RIGHT_CNT = 100;

    /** Multiplier: one row at the RIGHT table is related to MULT rows at the LEFT table. */
    private static final int MULT = 500;

    /** Keys counts at the LEFT table. */
    private static final int LEFT_CNT = RIGHT_CNT * MULT;

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGrids(1);

        IgniteCache cacheA = grid(0).createCache(new CacheConfiguration<Long, Long>()
            .setName("A")
            .setSqlSchema("TEST")
            .setQueryEntities(Collections.singleton(new QueryEntity(Long.class.getTypeName(), "A_VAL")
                .setTableName("A")
                .addQueryField("ID", Long.class.getName(), null)
                .addQueryField("JID", Long.class.getName(), null)
                .addQueryField("VAL", Long.class.getName(), null)
                .setKeyFieldName("ID")
            )));

        IgniteCache cacheB = grid(0).createCache(new CacheConfiguration()
            .setCacheMode(CacheMode.REPLICATED)
            .setName("B")
            .setSqlSchema("TEST")
            .setQueryEntities(Collections.singleton(new QueryEntity(Long.class.getName(), "B_VAL")
                .setTableName("B")
                .addQueryField("ID", Long.class.getName(), null)
                .addQueryField("A_JID", Long.class.getName(), null)
                .addQueryField("VAL0", String.class.getName(), null)
                .setKeyFieldName("ID")
            )));

        IgniteCache cacheC = grid(0).createCache(new CacheConfiguration()
            .setCacheMode(CacheMode.REPLICATED)
            .setName("C")
            .setSqlSchema("TEST")
            .setQueryEntities(Collections.singleton(new QueryEntity(Long.class.getName(), "C_VAL")
                .setTableName("C")
                .addQueryField("ID", Long.class.getName(), null)
                .addQueryField("A_JID", Long.class.getName(), null)
                .addQueryField("VAL0", String.class.getName(), null)
                .setKeyFieldName("ID")
            )));

        Map<Long, BinaryObject> batch = new HashMap<>();

        for (long i = 0; i < LEFT_CNT; ++i) {
            batch.put(i, grid(0).binary().builder("A_VAL")
                .setField("JID", i % RIGHT_CNT)
                .setField("VAL", i)
                .build());

            if (batch.size() > 1000) {
                cacheA.putAll(batch);

                batch.clear();
            }
        }

        if (!F.isEmpty(batch)) {
            cacheA.putAll(batch);

            batch.clear();
        }

        for (long i = 0; i < RIGHT_CNT; ++i) {
            cacheB.put(i, grid(0).binary().builder("B_VAL")
                .setField("A_JID", i)
                .setField("VAL0", String.format("val%03d", i))
                .build());
        }

        for (long i = 0; i < RIGHT_CNT; ++i) {
            cacheC.put(i, grid(0).binary().builder("C_VAL")
                .setField("A_JID", i)
                .setField("VAL0", String.format("val%03d", i))
                .build());
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * Test local query execution.
     */
    @Test
    public void testHashJoin() {
        assertEquals(LEFT_CNT, sql(true,
            "SELECT * FROM A, B USE INDEX(HASH_JOIN_IDX) " +
                "WHERE A.JID = B.A_JID").getAll().size());

        assertEquals((RIGHT_CNT - 10) * MULT, sql(true,
            "SELECT * FROM A, B USE INDEX(HASH_JOIN_IDX) " +
                "WHERE A.JID = B.A_JID AND B.VAL0 > 'val009'").getAll().size());

        assertEquals(10 * MULT, sql(true,
            "SELECT * FROM A, B USE INDEX(HASH_JOIN_IDX) " +
                "WHERE A.JID = B.A_JID AND B.VAL0 > 'val009' and B.ID < 20").getAll().size());

        assertEquals(12 * MULT, sql(true,
            "SELECT * FROM A, B USE INDEX(HASH_JOIN_IDX) " +
                "WHERE A.JID = B.A_JID AND B.VAL0 >= 'val009' AND B.ID <= 20").getAll().size());

        assertEquals(MULT, sql(true,
            "SELECT * FROM A, B USE INDEX(HASH_JOIN_IDX) " +
                "WHERE A.JID = B.A_JID AND B.VAL0 = 'val009'").getAll().size());

        assertEquals(MULT, sql(true,
            "SELECT * FROM A, B USE INDEX(HASH_JOIN_IDX) " +
                "WHERE A.JID = B.A_JID AND B.VAL0 = 'val009'").getAll().size());
    }

    /**
     * Test local query execution.
     */
    @Test
    public void testSimpleBenchmarkJoinTwoTables() {
        long tHashJoin = sqlDuration(true, 10,
            "SELECT * FROM A, B USE INDEX(HASH_JOIN_IDX) " +
                "WHERE A.JID = B.A_JID");

        // Switch off hash join index by hint
        long tNestedLoops = sqlDuration(false, 3,
            "SELECT * FROM A, B " +
                "WHERE A.JID = B.A_JID");

        assertTrue("Hash join is slow than nested loops: [HJ time=" + tHashJoin + ", NL time=" + tNestedLoops + ']',
            tNestedLoops > tHashJoin);

        log.info("Query duration: [HJ time=" + tHashJoin + ", NL time=" + tNestedLoops + ']');
    }

    /**
     * Test local query execution.
     */
    @Test
    public void testSimpleBenchmarkJoinThreeTables() {
        long tHashJoin = sqlDuration(true, 10,
            "SELECT * FROM A, B USE INDEX(HASH_JOIN_IDX), C USE INDEX(HASH_JOIN_IDX) " +
                "WHERE A.JID = B.A_JID AND A.JID=C.A_JID");

        long tNestedLoops = sqlDuration(false, 1,
            "SELECT * FROM A, B, C " +
                "WHERE A.JID = B.A_JID AND A.JID=C.A_JID");

        assertTrue("Hash join is slow than nested loops: [HJ time=" + tHashJoin + ", NL time=" + tNestedLoops + ']',
            tNestedLoops > tHashJoin);

        log.info("Query duration: [HJ time=" + tHashJoin + ", NL time=" + tNestedLoops + ']');
    }

    /**
     * Executes SQL statement 'count' times and calculate average duration.
     *
     * @param enforceJoinOrder Force join order flag.
     * @param count Count of the query runs.
     * @param sql Query.
     * @param args Parameters.
     * @return Average duration of SQL statement.
     */
    public long sqlDuration(boolean enforceJoinOrder, int count, String sql, Object... args) {
        long t0 = U.currentTimeMillis();

        for (int i = 0; i < count; ++i) {
            Iterator it = sql(enforceJoinOrder, sql).iterator();

            while (it.hasNext())
                it.next();
        }

        return (U.currentTimeMillis() - t0) / count;
    }

    /**
     * @param enforceJoinOrder Enforce join order mode.
     * @param sql SQL query.
     * @param args Query parameters.
     * @return Results cursor.
     */
    private FieldsQueryCursor<List<?>> sql(boolean enforceJoinOrder, String sql, Object... args) {
        return grid(0).context().query().querySqlFields(new SqlFieldsQuery(sql)
            .setSchema("TEST")
            .setLazy(true)
            .setEnforceJoinOrder(enforceJoinOrder)
            .setArgs(args), false);
    }
}
