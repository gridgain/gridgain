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

import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.query.h2.H2MemoryTracker;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.spi.metric.Metric;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for {@link SqlMemoryStatisticsHolder}. In this test we check that memory metrics reports plausible
 * values. Here we want to verify metrics based on the new framework work well, not {@link H2MemoryTracker}.
 */
public class SqlStatisticsMemoryQuotaTest extends SqlStatisticsAbstractTest {
    /**
     * This callback validates that some memory is reserved.
     */
    private static final MemValidator MEMORY_IS_USED = (freeMem, maxMem) -> {
        if (freeMem == maxMem)
            fail("Expected some memory reserved.");
    };

    /**
     * This callback validates that no "sql" memory is reserved.
     */
    private static final MemValidator MEMORY_IS_FREE = (freeMem, maxMem) -> {
        if (freeMem < maxMem)
            fail(String.format("Expected no memory reserved: [freeMem=%d, maxMem=%d]", freeMem, maxMem));
    };

    /**
     * Clean up.
     */
    @After
    public void cleanUp() {
        stopAllGrids();
    }

    /**
     * Set up.
     */
    @Before
    public void setup() {
        SqlStatisticsAbstractTest.SuspendQuerySqlFunctions.refresh();
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

        long dfltSqlGlobQuota = IgniteUtils.parseBytes("60%");

        assertTrue(almostEquals(dfltSqlGlobQuota, longMetricValue(0, "maxMem"), (long)(dfltSqlGlobQuota * 0.05)));
        assertTrue(almostEquals(dfltSqlGlobQuota, longMetricValue(1, "maxMem"), (long)(dfltSqlGlobQuota * 0.05)));

        assertEquals(longMetricValue(0, "maxMem"), longMetricValue(0, "freeMem"));
        assertEquals(longMetricValue(1, "maxMem"), longMetricValue(1, "freeMem"));
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
            cache.query(new SqlFieldsQuery(scanQry).setLazy(false)).getAll();

        long otherCntAfterDistQry = longMetricValue(otherNodeIdx, "requests");
        long connCntAfterDistQry = longMetricValue(connNodeIdx, "requests");

        assertTrue(otherCntAfterDistQry >= runCnt);
        assertTrue(connCntAfterDistQry >= runCnt);

        // And then run local query and check that only connect node metric has changed.

        for (int i = 0; i < runCnt; i++)
            cache.query(new SqlFieldsQuery(scanQry).setLocal(true).setLazy(false)).getAll();

        long otherCntAfterLocQry = longMetricValue(otherNodeIdx, "requests");
        long connCntAfterLocQry = longMetricValue(connNodeIdx, "requests");

        assertTrue(otherCntAfterLocQry == otherCntAfterDistQry);
        assertTrue(connCntAfterLocQry >= connCntAfterDistQry + runCnt);
    }

    /**
     * Check that memory metric reports non-zero memory is reserved on both nodes when distributed query is executed and
     * that memory is released when the query finishes.
     *
     * @throws Exception if failed.
     */
    @Test
    public void testMemoryIsTrackedWhenDistributedQueryIsRunningAndReleasedOnFinish() throws Exception {
        startGrids(2);

        int connNodeIdx = 1;
        int otherNodeIdx = 0;

        IgniteCache cache = createCacheFrom(grid(connNodeIdx));

        final String scanQry = "SELECT * FROM TAB WHERE ID <> suspendHook(5)";

        IgniteInternalFuture distQryIsDone =
            runAsyncX(() -> cache.query(new SqlFieldsQuery(scanQry).setLazy(false)).getAll());

        SqlStatisticsAbstractTest.SuspendQuerySqlFunctions.awaitQueryStopsInTheMiddle();

        validateMemoryUsageOn(connNodeIdx, MEMORY_IS_USED);
        validateMemoryUsageOn(otherNodeIdx, MEMORY_IS_USED);

        SqlStatisticsAbstractTest.SuspendQuerySqlFunctions.resumeQueryExecution();

        distQryIsDone.get(WAIT_OP_TIMEOUT_SEC, TimeUnit.SECONDS);

        validateMemoryUsageOn(connNodeIdx, MEMORY_IS_FREE);
        validateMemoryUsageOn(otherNodeIdx, MEMORY_IS_FREE);
    }

    /**
     * Check that memory metric reports non-zero memory is reserved only on one node when local query is executed and
     * that memory is released when the query finishes. The other node should not reserve the memory at any moment.
     *
     * @throws Exception if failed.
     */
    @Test
    public void testMemoryIsTrackedWhenLocalQueryIsRunningAndReleasedOnFinish() throws Exception {
        startGrids(2);

        int connNodeIdx = 1;
        int otherNodeIdx = 0;

        IgniteCache cache = createCacheFrom(grid(connNodeIdx));

        final String scanQry = "SELECT * FROM TAB WHERE ID <> suspendHook(5)";

        IgniteInternalFuture locQryIsDone =
            runAsyncX(() -> cache.query(new SqlFieldsQuery(scanQry).setLocal(true).setLazy(false)).getAll());

        SqlStatisticsAbstractTest.SuspendQuerySqlFunctions.awaitQueryStopsInTheMiddle();

        validateMemoryUsageOn(connNodeIdx, MEMORY_IS_USED);
        validateMemoryUsageOn(otherNodeIdx, MEMORY_IS_FREE);

        SqlStatisticsAbstractTest.SuspendQuerySqlFunctions.resumeQueryExecution();

        locQryIsDone.get(WAIT_OP_TIMEOUT_SEC, TimeUnit.SECONDS);

        validateMemoryUsageOn(connNodeIdx, MEMORY_IS_FREE);
        validateMemoryUsageOn(otherNodeIdx, MEMORY_IS_FREE);
    }

    /**
     * Check that if we set different sql mem pool sizes for 2 different nodes, appropriate metric values reflect this
     * fact.
     *
     * @throws Exception if failed.
     */
    @Test
    public void testMaxMemMetricShowCustomMaxMemoryValuesForDifferentNodes() throws Exception {
        final int oneMaxMem = 512 * 1024;
        final int otherMaxMem = 1024 * 1024;
        final int unlimMaxMem = 0;

        final int oneNodeIdx = 0;
        final int otherNodeIdx = 1;
        final int unlimNodeIdx = 2;

        startGridWithMaxMem(oneNodeIdx, oneMaxMem);
        startGridWithMaxMem(otherNodeIdx, otherMaxMem);
        startGridWithMaxMem(unlimNodeIdx, unlimMaxMem);

        assertEquals(oneMaxMem, longMetricValue(oneNodeIdx, "maxMem"));
        assertEquals(otherMaxMem, longMetricValue(otherNodeIdx, "maxMem"));
        assertEquals(unlimMaxMem, longMetricValue(unlimNodeIdx, "maxMem"));
    }

    /**
     * Check in complex scenario that metrics are not changed if global (maxMem) quota is unlimited.
     *
     * @throws Exception if failed.
     */
    @Test
    public void testAllMetricsIfMemoryQuotaIsUnlimited() throws Exception {
        final MemValidator quotaUnlim = (free, max) -> {
            assertEquals(0, max);
            assertTrue(0 >= free);
        };

        int connNodeIdx = 1;
        int otherNodeIdx = 0;

        startGridWithMaxMem(connNodeIdx, 0);
        startGridWithMaxMem(otherNodeIdx, 0);

        IgniteCache cache = createCacheFrom(grid(connNodeIdx));

        final SqlFieldsQuery scanQry = new SqlFieldsQuery("SELECT * FROM TAB WHERE ID <> suspendHook(5)");

        IgniteInternalFuture distQryIsDone =
            runAsyncX(() -> cache.query(scanQry).getAll());

        SqlStatisticsAbstractTest.SuspendQuerySqlFunctions.awaitQueryStopsInTheMiddle();

        validateMemoryUsageOn(connNodeIdx, quotaUnlim);
        validateMemoryUsageOn(otherNodeIdx, quotaUnlim);

        // we don't track memory for lazy queries for now
        assertEquals(scanQry.isLazy() ? 0 : 1, longMetricValue(connNodeIdx, "requests"));
        assertEquals(scanQry.isLazy() ? 0 : 1, longMetricValue(otherNodeIdx, "requests"));

        SqlStatisticsAbstractTest.SuspendQuerySqlFunctions.resumeQueryExecution();

        distQryIsDone.get(WAIT_OP_TIMEOUT_SEC, TimeUnit.SECONDS);

        validateMemoryUsageOn(connNodeIdx, quotaUnlim);
        validateMemoryUsageOn(otherNodeIdx, quotaUnlim);

        assertEquals(scanQry.isLazy() ? 0 : 3, longMetricValue(connNodeIdx, "requests"));
        assertEquals(scanQry.isLazy() ? 0 : 3, longMetricValue(otherNodeIdx, "requests"));
    }

    /**
     * Validate memory metrics freeMem and maxMem on the specified node.
     *
     * @param nodeIdx index of the node which metrics to validate.
     * @param validator function(freeMem, maxMem) that validates these values.
     */
    private void validateMemoryUsageOn(int nodeIdx, MemValidator validator) {
        long free = longMetricValue(nodeIdx, "freeMem");
        long maxMem = longMetricValue(nodeIdx, "maxMem");

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
        MetricRegistry sqlMemReg = grid(gridIdx).context().metric().registry(SqlMemoryStatisticsHolder.SQL_QUOTAS_REG_NAME);

        Metric metric = sqlMemReg.findMetric(metricName);

        Assert.assertNotNull("Didn't find metric " + metricName, metric);

        Assert.assertTrue("Expected long metric, but got "+ metric.getClass(), metric instanceof LongMetric);

        return ((LongMetric)metric).value();
    }

    /**
     * @param l1 First number.
     * @param l2 Second number.
     * @param error Max difference between numbers.
     *
     * @return {@code true} if the numbers differ from each other no more than {@code error}.
     */
    private boolean almostEquals(long l1, long l2, long error) {
        return Math.max(l1, l2) - Math.min(l1, l2) <= Math.abs(error);
    }

    /**
     * Functional interface to validate memory metrics values.
     */
    private static interface MemValidator {
        /**
         *
         * @param free freeMem metric value.
         * @param max maxMem metric value.
         */
        void validate(long free, long max);
    }
}
