/*
 * Copyright 2025 GridGain Systems, Inc. and Contributors.
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
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.communication.GridIoManager;
import org.apache.ignite.internal.managers.communication.PriorityWrapper;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.processors.pool.PoolProcessor;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** Sql priority related tests. */
@RunWith(Parameterized.class)
public class SqlQueryPriorityTest extends AbstractIndexingCommonTest {
    /** */
    private static IgniteEx client;

    /** Client or embeded query. */
    @Parameterized.Parameter
    public boolean isClient;

    /** Prioritized flag. */
    @Parameterized.Parameter(1)
    public boolean prioritized;

    /**
     * @return List of parameters to test.
     */
    @Parameterized.Parameters(name = "client={0}, prioritized={1}")
    public static Collection<Object[]> testData() {
        List<Object[]> res = new ArrayList<>();

        res.add(new Object[] {true, true});
        res.add(new Object[] {false, true});
        res.add(new Object[] {true, false});
        res.add(new Object[] {false, false});

        return res;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(2);

        client = startClientGrid(3);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        for (String cacheName : grid(0).cacheNames())
            grid(0).cache(cacheName).destroy();
    }

    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setQueryThreadPoolSize(1);

        return cfg;
    }

    /**
     * Test scenario: <br>
     * 1. Query processing pool is configured with 1. <br>
     * 2. Pull long running query with default priority (emulated through reflection and latch). <br>
     * 3. Pull one more long running query with same priority as first one. <br>
     * 4. Pull "real" queries with different but higher priority than previous.
     * <br><br>
     * Expectations: "real" queries will be executed according it`s priority after first long running is completed.
     *
     * @throws Exception
     */
    @Test
    public void testPrioritizedQuery() throws Exception {
        CacheConfiguration<Integer, Object> cacheCfg = new CacheConfiguration<Integer, Object>("cache")
                .setSqlSchema("PUBLIC");

        IgniteCache<Integer, Object> cache;

        if (isClient)
            cache = client.getOrCreateCache(cacheCfg);
        else
            cache = grid(0).getOrCreateCache(cacheCfg);

        GridIoManager mgr = grid(1).context().io();
        PoolProcessor poolProcessor = GridTestUtils.getFieldValue(mgr, GridIoManager.class, "pools");

        cache.query(new SqlFieldsQuery("CREATE TABLE Person (ID INTEGER PRIMARY KEY, NAME VARCHAR(100))"));
        cache.query(new SqlFieldsQuery("INSERT INTO Person VALUES(1, '1')"));
        cache.query(new SqlFieldsQuery("INSERT INTO Person VALUES(2, '2')"));

        CountDownLatch latch1 = new CountDownLatch(1);
        CountDownLatch latch2 = new CountDownLatch(1);

        IgniteThreadPoolExecutor executor = (IgniteThreadPoolExecutor)poolProcessor.getQueryExecutorService();

        poolProcessor.getQueryExecutorService().execute(new PriorityWrapper(() -> {
            try {
                latch1.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }, (byte)0));

        poolProcessor.getQueryExecutorService().execute(new PriorityWrapper(() -> {
            try {
                latch2.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }, (byte)0));

        waitForCondition(() -> executor.getQueue().size() == 1, 3_000);

        IgniteInternalFuture lowerPriority = GridTestUtils.runAsync(() -> {
            int priority = prioritized ? 5 : 0;
            List<List<?>> res = sql(priority, cache, "SELECT id FROM Person WHERE name='1'").getAll();
            assertEquals(res.get(0).get(0), 1);
        });

        waitForCondition(() -> executor.getQueue().size() == 2, 3_000);

        IgniteInternalFuture higherPriority = GridTestUtils.runAsync(() -> {
            int priority = prioritized ? 6 : 0;
            List<List<?>> res = sql(priority, cache, "SELECT id FROM Person WHERE name='2'").getAll();
            assertEquals(res.get(0).get(0), 2);
        });

        waitForCondition(() -> executor.getQueue().size() == 3, 3_000);

        latch1.countDown();

        if (prioritized) {
            // higher priority first
            higherPriority.get(1000);

            // lower priority last
            lowerPriority.get(1000);
        }
        else {
            try {
                higherPriority.get(1000);
                fail("Exception need to be raised");
            } catch (IgniteCheckedException ex) {
                // Expected, no op.
            }
        }

        latch2.countDown();
    }

    @Test
    public void testPrioritizedQueryParams() {
        IgniteCache<Object, Object> cache = isClient
                ? client.getOrCreateCache(DEFAULT_CACHE_NAME)
                : grid(0).getOrCreateCache(DEFAULT_CACHE_NAME);

        GridTestUtils.assertThrows(log, () -> {
                    sql(SqlFieldsQuery.MIN_PRIORITY - 1, cache, "SELECT 1").getAll();
                }, IllegalArgumentException.class,
                "Priority should be in range 0..10");

        GridTestUtils.assertThrows(log, () -> {
                    sql(SqlFieldsQuery.MAX_PRIORITY + 1, cache, "SELECT 1").getAll();
                }, IllegalArgumentException.class,
                "Priority should be in range 0..10");
    }

    /**
     * @param priority Query priority.
     * @param sql SQL query.
     * @return Results cursor.
     */
    private FieldsQueryCursor<List<?>> sql(int priority, IgniteCache<?, ?> cache, String sql) {
        return cache.query(new SqlFieldsQuery(sql).setPriority(priority));
    }
}
