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
        sql("create table T (id int primary key, ref_key int, name varchar)", false);

        for (int i = 0; i < ROW_CNT; ++i)
            sql("insert into T VALUES (?, ?, ?)", false, i, i, UUID.randomUUID().toString());
    }

    /**
     * Check simple query with huge local result.
     */
    @Test
    public void testHugeLocalResult() {
        sql("select * from T as T0, T as T1", true);

        // Query with single huge local result.
        GridTestUtils.assertThrows(log, () -> {
            sql("select * from T as T0, T as T1", false);

            return null;
        }, CacheException.class, "IgniteOutOfMemoryException: SQL query out of memory");

        GridTestUtils.assertThrows(log, () -> {
            sql("select * from T as T0, T as T1 ORDER BY T0.id", true);

            return null;
        }, CacheException.class, "IgniteOutOfMemoryException: SQL query out of memory");
    }

    /**
     * Check simple query with tiny local result.
     */
    @Test
    public void testSimpleQueries() {
        sql("select * from T", false);
        sql("select * from T as T0, T as T1 where T0.id < 2", false);
        sql("select * from T as T0, T as T1 where T0.id >= 2 AND T0.id < 4", false);
        sql("select * from T as T0, T as T1", true);
    }

    /**
     * Check UNION operation with huge local results.
     */
    @Test
    public void testUnion() {
        // Check query that is mapped to two map queries.
        GridTestUtils.assertThrows(log, () -> {
            sql("select * from T as T0, T as T1 where T0.id < 2 " +
                "UNION " +
                "select * from T as T0, T as T1 where T0.id >= 2 AND T0.id < 4", false);

            return null;
        }, CacheException.class, "IgniteOutOfMemoryException: SQL query out of memory");
    }

    /**
     * @param sql SQL query
     * @param args Query parameters.
     * @return Results set.
     */
    private List<List<?>> sql(String sql, boolean lazy, Object... args) {
        return grid(0).context().query().querySqlFields(
            new SqlFieldsQueryEx(sql, null).setArgs(args).workMemory(WORK_MEM).setLazy(lazy), false).getAll();
    }
}
