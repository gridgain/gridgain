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
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.spi.metric.Metric;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link SqlStatisticsHolderMemoryQuotas}.
 */
public class SqlStatisticsMemoryQuotaTest extends GridCommonAbstractTest {
    /** Number of rows in the test table. */
    private static final int TABLE_SIZE = 10_000;

    /**
     * Create cache with a test table.
     */
    private IgniteCache createCacheFrom(Ignite node) {
        CacheConfiguration<Integer, String> ccfg = new CacheConfiguration<Integer, String>("TestCache")
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

        for (int i = 0; i < runCnt; i++)
            cache.query(new SqlFieldsQuery("SELECT * FROM TAB WHERE ID <> 5")).getAll();

        long otherCntAfterDistQry = longMetricValue(otherNodeIdx, "requests");
        long connCntAfterDistQry = longMetricValue(connNodeIdx, "requests");

        assertTrue(otherCntAfterDistQry >= runCnt);
        assertTrue(connCntAfterDistQry >= runCnt);

        // And then run local query and check that only connect node metric has changed.

        for (int i = 0; i < runCnt; i++)
            cache.query(new SqlFieldsQuery("SELECT * FROM TAB WHERE ID <> 5").setLocal(true)).getAll();

        long otherCntAfterLocQry = longMetricValue(otherNodeIdx, "requests");
        long connCntAfterLocQry = longMetricValue(connNodeIdx, "requests");

        assertTrue(otherCntAfterLocQry == otherCntAfterDistQry);
        assertTrue(connCntAfterLocQry >= connCntAfterDistQry + runCnt);
    }


    /**
     * Finds LongMetric from sql memory registry by specified metric name and returns its value.
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
}
