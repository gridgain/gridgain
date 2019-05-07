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

package org.apache.ignite.internal.processors.query.oom;

import java.util.List;
import java.util.UUID;
import javax.cache.CacheException;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.processors.cache.query.SqlFieldsQueryEx;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests for query memory manager.
 */
public class QueryMemoryTrackerSelfTest extends GridCommonAbstractTest {
    /** Row count. */
    private static final int ROW_CNT = 1000;

    /** Work mem. */
    public static final int WORK_MEM = 1024 * 1024;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(0);

        populateData();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     *
     */
    private void populateData() {
        execSql("create table T (id int primary key, ref_key int, name varchar)");
        execSql("create table K (id int primary key, indexed int, grp int, name varchar)");
        execSql("create index K_IDX on K(indexed)");

        for (int i = 0; i < ROW_CNT; ++i)
            execSql("insert into T VALUES (?, ?, ?)", i, i, UUID.randomUUID().toString());

        for (int i = 0; i < 10 * ROW_CNT; ++i)
            execSql("insert into K VALUES (?, ?, ?, ?)", i, i, i % 100, UUID.randomUUID().toString());
    }

    /**
     * Check Joins.
     */
    @Test
    public void testSimpleJoins() {
        // Simple queries with tiny local results.
        execQuery("select * from T", false);
        execQuery("select * from T as T0, T as T1 where T0.id < 2", false);
        execQuery("select * from T as T0, T as T1 where T0.id >= 2 AND T0.id < 4", false);
        execQuery("select * from T as T0, T as T1", true);

        // Query with single huge local result.
        GridTestUtils.assertThrows(log, () -> {
            execQuery("select * from T as T0, T as T1", false);

            return null;
        }, CacheException.class, "IgniteOutOfMemoryException: SQL query out of memory");

        // Query with huge local result.
        GridTestUtils.assertThrows(log, () -> {
            execQuery("select * from T as T0, T as T1 ORDER BY T0.id", true);

            return null;
        }, CacheException.class, "IgniteOutOfMemoryException: SQL query out of memory");
    }

    /**
     * Check UNION operation.
     */
    @Test
    public void testUnion() {
        // Check query that is mapped to two map queries.
        GridTestUtils.assertThrows(log, () -> {
            execQuery("select * from T as T0, T as T1 where T0.id < 2 " +
                "UNION " +
                "select * from T as T0, T as T1 where T0.id >= 2 AND T0.id < 4", false);

            return null;
        }, CacheException.class, "IgniteOutOfMemoryException: SQL query out of memory");
    }

    /**
     * Check ORDER BY operation.
     */
    @Test
    public void testOrderBy() {
        execQuery("select * from K", true);

        // Order by indexed field.
        // TODO: IGNITE-9933: OOM on reducer.
        GridTestUtils.assertThrows(log, () -> {
            execQuery("select * from K ORDER BY K.indexed", true);

            return null;
        }, CacheException.class, "IgniteOutOfMemoryException: SQL query out of memory");

        // Order by indexed field.
        GridTestUtils.assertThrows(log, () -> {
            execQuery("select * from K ORDER BY K.indexed", false);

            return null;
        }, CacheException.class, "IgniteOutOfMemoryException: SQL query out of memory");

        // Order by non-indexed field.
        GridTestUtils.assertThrows(log, () -> {
            execQuery("select * from K ORDER BY K.grp", true);

            return null;
        }, CacheException.class, "IgniteOutOfMemoryException: SQL query out of memory");

        // Order by non-indexed field.
        GridTestUtils.assertThrows(log, () -> {
            execQuery("select * from K ORDER BY K.grp", false);

            return null;
        }, CacheException.class, "IgniteOutOfMemoryException: SQL query out of memory");
    }

    /**
     * Check GROUP BY operation.
     */
    @Test
    public void testGroupBy() {
        execQuery("select K.grp, sum(K.id) from K GROUP BY K.grp", false); // Tiny local result.
        execQuery("select K.id, sum(K.grp) from K GROUP BY K.id", false); // Sorted grouping.

        // Group by non-indexed field.
        GridTestUtils.assertThrows(log, () -> {
            execQuery("select K.name from K GROUP BY K.name", true);

            return null;
        }, CacheException.class, "IgniteOutOfMemoryException: SQL query out of memory");

        // Group by with distinct.
        GridTestUtils.assertThrows(log, () -> {
            execQuery("select DISTINCT K.id from K GROUP BY K.id", true);

            return null;
        }, CacheException.class, "IgniteOutOfMemoryException: SQL query out of memory");
    }

    /**
     * Check simple query with DISTINCT constraint.
     */
    @Test
    public void testSimpleDistinct() {
        execQuery("select DISTINCT K.grp from K", false);

        // Group by with distinct.
        GridTestUtils.assertThrows(log, () -> {
            execQuery("select DISTINCT K.id from K", true);

            return null;
        }, CacheException.class, "IgniteOutOfMemoryException: SQL query out of memory");
    }

    /**
     * @param sql SQL query
     * @param args Query parameters.
     * @return Results set.
     */
    private List<List<?>> execQuery(String sql, boolean lazy, Object... args) {
        return grid(0).context().query().querySqlFields(
            new SqlFieldsQueryEx(sql, null).setArgs(args).workMemory(WORK_MEM)
                .setLazy(lazy).setPageSize(100), false).getAll();
    }

    /**
     * @param sql SQL query
     * @param args Query parameters.
     */
    private void execSql(String sql, Object... args) {
        grid(0).context().query().querySqlFields(
            new SqlFieldsQuery(sql).setArgs(args), false).getAll();
    }
}
