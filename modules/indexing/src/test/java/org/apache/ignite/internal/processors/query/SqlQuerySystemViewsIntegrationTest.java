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

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.processors.cache.query.SqlFieldsQueryEx;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Integration test for query ssystem views.
 */
@RunWith(Parameterized.class)
public class SqlQuerySystemViewsIntegrationTest extends AbstractIndexingCommonTest {
    /** */
    private static final int TBL_SIZE = 100;

    /** */
    private static final int QUERY_WAIT_TIMEOUT = 5_000;

    /** */
    private static final Semaphore SEMAPHORE = new Semaphore(0);

    /** */
    @Parameterized.Parameter(1)
    public boolean lazy;

    /** */
    @Parameterized.Parameter
    public boolean loc;

    /** */
    private static final AtomicInteger LOC_QRY_ID_GEN = new AtomicInteger();

    /**
     * @return Test parameters.
     */
    @Parameterized.Parameters(name = "loc={0}, lazy={1}")
    public static Collection<?> parameters() {
        return Arrays.asList(new Object[][] {
            //local, lazy
            { true, true },
            { false, true },
            { true, false },
            { false, false },
        });
    }

    /** */
    @Before
    public void before() {
        SEMAPHORE.drainPermits();
    }

    /** */
    @After
    public void after() throws IgniteInterruptedCheckedException {
        SEMAPHORE.release(TBL_SIZE);

        waitUntilQueriesCompletes();
    }

    /** */
    private void waitUntilQueriesCompletes() throws IgniteInterruptedCheckedException {
        assertTrue(GridTestUtils.waitForCondition(() -> (Long)runSql(
            "select count(*) from ignite.local_sql_running_queries",
            -1
        ).get(0).get(0) == 1L, QUERY_WAIT_TIMEOUT));
    }


    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(defaultCacheConfiguration())
            .setSqlConfiguration(new SqlConfiguration()
                .setSqlGlobalMemoryQuota("0")
                .setSqlQueryMemoryQuota("0")
                .setSqlOffloadingEnabled(true));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        cleanPersistenceDir();

        startGrid(0);

        @SuppressWarnings("unchecked")
        CacheConfiguration<?,?> personCache = defaultCacheConfiguration()
            .setName("test_cache")
            .setCacheMode(CacheMode.PARTITIONED)
            .setSqlFunctionClasses(SqlQuerySystemViewsIntegrationTest.class)
            .setBackups(1);

        grid(0).addCacheConfiguration(personCache);

        prepareTable();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /**
     * Ensure start time and duration fields reflects query execution process
     */
    @Test
    public void testMemoryMetricForRunningQuery() throws Exception {
        final String locQryId = "locQryId_" + LOC_QRY_ID_GEN.incrementAndGet();

        SEMAPHORE.release(1);

        IgniteInternalFuture<?> fut = multithreadedAsync(() -> runSql(
            "select delay('" + locQryId + "') as qryId, id, value from test_table",
            -1
        ), 1);

        long prevSize = -1;

        // wait while the first row will be fetched
        assertTrue(GridTestUtils.waitForCondition(() -> SEMAPHORE.availablePermits() == 0, QUERY_WAIT_TIMEOUT));

        for (int i = 0; i < 3; i++) {
            List<List<?>> res = runSql(
                "select memory_current, disk_allocation_total from ignite.local_sql_running_queries where sql like '%"
                    + locQryId + "%' order by start_time",
                -1
            );

            assertEquals(2, res.size()); // original query + query to sql running query view
            assertTrue(prevSize <= (Long)res.get(0).get(0));
            assertEquals(0L, res.get(0).get(1));

            prevSize = (Long)res.get(0).get(0);
            SEMAPHORE.release(TBL_SIZE / 4);

            // wait while query drains permissions
            assertTrue(GridTestUtils.waitForCondition(() -> SEMAPHORE.availablePermits() == 0, QUERY_WAIT_TIMEOUT));
        }

        SEMAPHORE.release(TBL_SIZE);

        fut.get(getTestTimeout());
    }

    /**
     * Ensure start time and duration fields reflects query execution process
     */
    @Test
    public void testDiskMetricForRunningQuery() throws Exception {
        final String locQryId = "locQryId_" + LOC_QRY_ID_GEN.incrementAndGet();

        SEMAPHORE.release(1);

        IgniteInternalFuture<?> fut = multithreadedAsync(() -> runSql(
            "select delay('" + locQryId + "') as qryId, id, value from test_table",
            10L
        ), 1);

        long prevCurrSize = -1;
        long prevTotalSize = -1;

        // wait while the first row will be fetched
        assertTrue(GridTestUtils.waitForCondition(() -> SEMAPHORE.availablePermits() == 0, QUERY_WAIT_TIMEOUT));

        for (int i = 0; i < 3; i++) {
            List<List<?>> res = runSql(
                "select disk_allocation_current, disk_allocation_total from ignite.local_sql_running_queries where sql like '%"
                    + locQryId + "%' order by start_time",
                -1
            );

            assertEquals(2, res.size()); // original query + query to sql running query view
            assertTrue(prevCurrSize <= (Long)res.get(0).get(0));
            assertTrue(prevTotalSize <= (Long)res.get(0).get(1));

            prevCurrSize = (Long)res.get(0).get(0);
            prevTotalSize = (Long)res.get(0).get(1);
            SEMAPHORE.release(TBL_SIZE / 4);

            // wait while query drains permissions
            assertTrue(GridTestUtils.waitForCondition(() -> SEMAPHORE.availablePermits() == 0, QUERY_WAIT_TIMEOUT));
        }

        SEMAPHORE.release(TBL_SIZE);

        fut.get(getTestTimeout());
    }

    /** */
    @Test
    public void testMemoryAndDiskMetricForQueryHistory() {
        final String locQryId = "locQryId_" + LOC_QRY_ID_GEN.incrementAndGet();

        for (int i = 0; i < 3; i++)
            runSql(
                "select '" + locQryId + "' as qryId, id, value from test_table",
                (long)Math.pow(2, (i + 10))
            );

        List<List<?>> res = runSql(
            "select memory_min, memory_max, disk_allocation_min, disk_allocation_max from ignite.local_sql_query_history where sql like '%"
                + locQryId + "%'",
            -1
        );

        assertEquals(1, res.size()); // original query

        if (lazy) { // there is no tracking for lazy queries
            assertEquals(0L, res.get(0).get(0));
            assertEquals(0L, res.get(0).get(1));
            assertEquals(0L, res.get(0).get(2));
            assertEquals(0L, res.get(0).get(3));
        }
        else {
            assertTrue(0L < (Long)res.get(0).get(0));
            assertTrue((Long)res.get(0).get(0) < (Long)res.get(0).get(1));
            assertTrue(0L < (Long)res.get(0).get(2));
            assertTrue((Long)res.get(0).get(2) <= (Long)res.get(0).get(3));
        }
    }

    /** */
    private void prepareTable() {
        IgniteCache<Object, Object> cache = grid(0).cache(DEFAULT_CACHE_NAME);
        // Departments
        cache.query(new SqlFieldsQuery("CREATE TABLE test_table (" +
            "id INT PRIMARY KEY, " +
            "value VARCHAR) WITH \"TEMPLATE=test_cache\"")).getAll();

        for (int i = 0; i < TBL_SIZE; i++)
            cache.query(
                new SqlFieldsQuery("INSERT INTO test_table (id, value) VALUES (?, ?)").setArgs(i, "value_" + i)
            ).getAll();
    }

    /**
     * @param sql Sql.
     * @param memLimit Mem limit.
     */
    protected List<List<?>> runSql(String sql, long memLimit) {
        Ignite node = grid(0);

        return node.cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQueryEx(sql, null)
            .setMaxMemory(memLimit)
            .setLazy(lazy)
            .setLocal(loc)
        ).getAll();
    }

    /**
     * Custom function to introduce delays to query execution.
     *
     * @param val Value.
     */
    @QuerySqlFunction
    public static String delay(String val) throws InterruptedException {
        SEMAPHORE.acquire();

        return val;
    }
}
