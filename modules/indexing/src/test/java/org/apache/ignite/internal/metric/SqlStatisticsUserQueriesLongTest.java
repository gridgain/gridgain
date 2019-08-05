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

import javax.cache.CacheException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.processors.query.RunningQueryManager;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for statistics of user initiated queries execution, that require grid restart.
 *
 * @see RunningQueryManager
 */
public class SqlStatisticsUserQueriesLongTest extends UserQueriesTestBase {
    /**
     * Teardown.
     */
    @After
    public void stopAll() {
        stopAllGrids();
    }

    /**
     * Check that after grid starts, counters are 0.
     *
     * @throws Exception on fail.
     */
    @Test
    public void testInitialValuesAreZero() throws Exception {
        startGrids(2);

        createCacheFrom(grid(REDUCER_IDX));

        Assert.assertEquals(0, longMetricValue(REDUCER_IDX, "success"));
        Assert.assertEquals(0, longMetricValue(REDUCER_IDX, "failed"));
        Assert.assertEquals(0, longMetricValue(REDUCER_IDX, "canceled"));
        Assert.assertEquals(0, longMetricValue(REDUCER_IDX, "failedByOOM"));

        Assert.assertEquals(0, longMetricValue(MAPPER_IDX, "success"));
        Assert.assertEquals(0, longMetricValue(MAPPER_IDX, "failed"));
        Assert.assertEquals(0, longMetricValue(MAPPER_IDX, "canceled"));
        Assert.assertEquals(0, longMetricValue(MAPPER_IDX, "failedByOOM"));
    }


    /**
     * Verify map phase failure affects only general fail metric, not OOM metric.
     *
     * @throws Exception on fail.
     */
    @Test
    public void testIfMapPhaseFailedByOomThenOomMetricIsNotUpdated() throws Exception {
        int strongMemQuota = 256 * 1024;
        int memQuotaUnlimited = -1;

        startGridWithMaxMem(MAPPER_IDX, strongMemQuota);
        startGridWithMaxMem(REDUCER_IDX, memQuotaUnlimited);

        IgniteCache cache = createCacheFrom(grid(REDUCER_IDX));

        assertMetricsIncrementedOnlyOnReducer(() -> GridTestUtils.assertThrows(
            log,
            () -> cache.query(new SqlFieldsQuery("SELECT * FROM TAB")).getAll(),
            CacheException.class,
            null), "failed");
    }

    /**
     * If reduce part of the query failed due to OOM protection, only general failure metric and OOM metric should be
     * incremented only on reduce node.
     */
    @Test
    public void testIfReduceQueryOomThenOnlyReducerMetricsAreIncremented() throws Exception {
        int strongMemQuota = 256 * 1024;
        int memQuotaUnlimited = -1;

        startGridWithMaxMem(MAPPER_IDX, memQuotaUnlimited);

        // Since reduce node is client, it doesn't execute map queries, and reduce part fails.
        startGridWithMaxMem(REDUCER_IDX, strongMemQuota, true);

        IgniteCache cache = createCacheFrom(grid(REDUCER_IDX));

        assertMetricsIncrementedOnlyOnReducer(() -> GridTestUtils.assertThrows(
            log,
            () -> cache.query(new SqlFieldsQuery("SELECT * FROM TAB GROUP BY NAME")).getAll(),
            CacheException.class,
            null), "failed", "failedByOOM");
    }
}
