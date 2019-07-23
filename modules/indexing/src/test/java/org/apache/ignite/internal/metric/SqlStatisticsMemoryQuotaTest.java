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

package org.apache.ignite.internal.metric;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.spi.metric.Metric;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for {@link SqlStatisticsHolderMemoryQuotas}.
 */
public class SqlStatisticsMemoryQuotaTest extends GridCommonAbstractTest {
    /**
     * Number of rows in the test table.
     */
    private static final int TABLE_SIZE = 10_000;

    /**
     * Timeout for each wait on sync operation in seconds.
     */
    public static final long WAIT_OP_TIMEOUT_SEC = 15;

    /**
     * Create cache with a test table.
     */
    private IgniteCache createCacheFrom(Ignite node) {
        CacheConfiguration<Integer, String> ccfg = new CacheConfiguration<Integer, String>("TestCache")
            .setSqlFunctionClasses(SuspendQuerySqlFunctions.class)
            .setQueryEntities(Collections.singleton(
                new QueryEntity(Integer.class.getName(), String.class.getName())
                    .setTableName("TAB")
                    .addQueryField("id", Integer.class.getName(), null)
                    .addQueryField("name", String.class.getName(), null)
                    .setKeyFieldName("id")
                    .setValueFieldName("name")
            ));

        IgniteCache<Integer, String> cache = node.createCache(ccfg);

        for (int i = 0; i < TABLE_SIZE; i++)
            cache.put(i, UUID.randomUUID().toString());

        return cache;
    }

    /**
     * Clean up.
     */
    @After
    public void cleanUp() {
        stopAllGrids();
    }

    /**
     * Clean up.
     */
    @Before
    public void setup() {
        stopAllGrids();

        SuspendQuerySqlFunctions.refresh();
    }


    /**
     * Check values of all sql memory metrics right after grid starts and no queries are executed.
     *
     * @throws Exception on fail.
     */
    @Test
    public void testAllMetricsValuesAfterGridStarts() throws Exception {
        startGrids(2);

        assertEquals(0, longMetricValue(0, "requests"));
        assertEquals(0, longMetricValue(1, "requests"));

        long dfltSqlGlobQuota = (long)(Runtime.getRuntime().maxMemory() * 0.6);

        assertEquals(dfltSqlGlobQuota, longMetricValue(0, "maxMem"));
        assertEquals(dfltSqlGlobQuota, longMetricValue(1, "maxMem"));

        assertEquals(dfltSqlGlobQuota, longMetricValue(0, "freeMem"));
        assertEquals(dfltSqlGlobQuota, longMetricValue(1, "freeMem"));
    }

    /**
     * Check metric for quota requests count is updated: <br/>
     * 1) On distributed query both nodes metrics are updated <br/>
     * 2) On local query, only one node metric is updated <br/>
     *
     * @throws Exception on fail.
     */
    @Test
    public void testRequestsMetricIsUpdatedAfterDistributedAndLocalQueries() throws Exception {
        startGrids(2);

        int connNodeIdx = 1;
        int otherNodeIdx = 0;

        IgniteCache cache = createCacheFrom(grid(connNodeIdx));

        int runCnt = 10;

        final String scanQry = "SELECT * FROM TAB WHERE ID <> 5";

        for (int i = 0; i < runCnt; i++)
            cache.query(new SqlFieldsQuery(scanQry)).getAll();

        long otherCntAfterDistQry = longMetricValue(otherNodeIdx, "requests");
        long connCntAfterDistQry = longMetricValue(connNodeIdx, "requests");

        assertTrue(otherCntAfterDistQry >= runCnt);
        assertTrue(connCntAfterDistQry >= runCnt);

        // And then run local query and check that only connect node metric has changed.

        for (int i = 0; i < runCnt; i++)
            cache.query(new SqlFieldsQuery(scanQry).setLocal(true)).getAll();

        long otherCntAfterLocQry = longMetricValue(otherNodeIdx, "requests");
        long connCntAfterLocQry = longMetricValue(connNodeIdx, "requests");

        assertTrue(otherCntAfterLocQry == otherCntAfterDistQry);
        assertTrue(connCntAfterLocQry >= connCntAfterDistQry + runCnt);
    }


    @Test
    public void testMemoryIsTrackedWhenQueryIsRunningAndReleasedOnFinish() throws Exception {
        startGrids(2);

        int connNodeIdx = 1;
        int otherNodeIdx = 0;

        MemValidator memoryIsUsed = (freeMem, maxMem) -> {
            if (freeMem == maxMem)
                fail("No memory is reserved during the query");
        };

        MemValidator memoryIsFree = (freeMem, maxMem) -> {
            if (freeMem < maxMem)
                fail(String.format("Expected no memory reserved: [freeMem=%d, maxMem=%d]", freeMem, maxMem));
        };

        IgniteCache cache = createCacheFrom(grid(connNodeIdx));

        final String scanQry = "SELECT * FROM TAB WHERE ID <> suspendHook(5)";

        IgniteInternalFuture distQryIsDone =
            runAsync0(() -> cache.query(new SqlFieldsQuery(scanQry)).getAll());

        SuspendQuerySqlFunctions.awaitQueryStopsInTheMiddle();

        validateMemoryUsageOn(connNodeIdx, memoryIsUsed);
        validateMemoryUsageOn(otherNodeIdx, memoryIsUsed);

        SuspendQuerySqlFunctions.resumeQueryExecution();

        distQryIsDone.get(WAIT_OP_TIMEOUT_SEC, TimeUnit.SECONDS);

        validateMemoryUsageOn(connNodeIdx, memoryIsFree);
        validateMemoryUsageOn(otherNodeIdx, memoryIsFree);

        // And for the local query:
        SuspendQuerySqlFunctions.refresh();

        IgniteInternalFuture locQryIsDone =
            runAsync0(() -> cache.query(new SqlFieldsQuery(scanQry).setLocal(true)).getAll());

        SuspendQuerySqlFunctions.awaitQueryStopsInTheMiddle();

        validateMemoryUsageOn(connNodeIdx, memoryIsUsed);
        validateMemoryUsageOn(otherNodeIdx, memoryIsFree);

        SuspendQuerySqlFunctions.resumeQueryExecution();

        locQryIsDone.get(WAIT_OP_TIMEOUT_SEC, TimeUnit.SECONDS);

        validateMemoryUsageOn(connNodeIdx, memoryIsFree);
        validateMemoryUsageOn(otherNodeIdx, memoryIsFree);
    }

    /**
     * Run async action and log if exception occured.
     *
     * @param action action to perform on other thread.
     * @return future object.
     */
    private IgniteInternalFuture runAsync0(Runnable action) {
        return GridTestUtils.runAsync(() -> {
            try {
                action.run();
            }
            catch (Throwable th) {
                log.error("Failed to perform async action.", th);
            }
        });
    }

    /**
     * Validate memory metrics freeMem and maxMem on the specified node.
     *
     * @param nodeIdx index of the node which metrics to validate.
     * @param validator function(freeMem, maxMem) that validates these values.
     */
    private void validateMemoryUsageOn(int nodeIdx, MemValidator validator) {
        long free = longMetricValue(0, "freeMem");
        long maxMem = longMetricValue(0, "maxMem");

        if (free > maxMem)
            fail(String.format("Illegal state: there's more free memory (%s) than " +
                "maximum available for sql (%s) on the node %d", free, maxMem, nodeIdx));

        validator.validate(free, maxMem);
    }


    /**
     * Finds LongMetric from sql memory registry by specified metric name and returns it's value.
     *
     * @param gridIdx index of a grid which metric value to find.
     * @param metricName short name of the metric from the "sql memory" metric registry.
     */
    private long longMetricValue(int gridIdx, String metricName) {
        MetricRegistry sqlMemReg = grid(gridIdx).context().metric().registry(SqlStatisticsHolderMemoryQuotas.SQL_QUOTAS_REG_NAME);

        Metric metric = sqlMemReg.findMetric(metricName);

        Assert.assertNotNull("Didn't find metric " + metricName, metric);

        Assert.assertTrue("Expected long metric, but got "+ metric.getClass(),  metric instanceof LongMetric);

        return ((LongMetric)metric).value();
    }

    /**
     *
     */
    public static class SuspendQuerySqlFunctions {
        /**
         * How many rows should be processed (by all nodes in total)
         */
        private static final int PROCESS_ROWS_TO_SUSPEND = TABLE_SIZE / 2;

        /**
         * Latch to await till full scan query that uses this class function have done some job, so some memory is
         * reserved.
         */
        public static volatile CountDownLatch qryIsInTheMiddle;

        /**
         * This latch is released when query should continue it's execution after stop in the middle.
         */
        private static volatile CountDownLatch resumeQryExec;

        static {
            refresh();
        }

        /**
         * Refresh syncs.
         */
        public static void refresh() {
            if (qryIsInTheMiddle != null) {
                for (int i = 0; i < qryIsInTheMiddle.getCount(); i++)
                    qryIsInTheMiddle.countDown();
            }

            if (resumeQryExec != null)
                resumeQryExec.countDown();

            qryIsInTheMiddle = new CountDownLatch(PROCESS_ROWS_TO_SUSPEND);

            resumeQryExec = new CountDownLatch(1);
        }

        /**
         * See {@link #qryIsInTheMiddle}.
         */
        public static void awaitQueryStopsInTheMiddle() throws InterruptedException {
            boolean reached = qryIsInTheMiddle.await(WAIT_OP_TIMEOUT_SEC, TimeUnit.SECONDS);

            if (!reached)
                throw new IllegalStateException("Unable to wait when query starts. Test is broken");
        }

        /**
         * See {@link #resumeQryExec}.
         */
        public static void resumeQueryExecution() {
            resumeQryExec.countDown();
        }

        /**
         * Sql function used to suspend query when half of the table is processed. Should be used in full scan queries.
         *
         * @param ret number to return.
         */
        @QuerySqlFunction
        public static long suspendHook(long ret) throws InterruptedException {
            qryIsInTheMiddle.countDown();

            if (qryIsInTheMiddle.getCount() == 0) {
                boolean reached = resumeQryExec.await(WAIT_OP_TIMEOUT_SEC, TimeUnit.SECONDS);

                if (!reached)
                    throw new IllegalStateException("Unable to wait when query starts. Test is broken");
            }

            return ret;
        }
    }

    /**
     * Functional interface to validate metrics values.
     */
    public static interface MemValidator {
        /**
         *
         * @param free freeMem metric value.
         * @param max maxMem metric value.
         */
        void validate(long free, long max);
    }
}
