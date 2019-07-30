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
import org.apache.ignite.IgniteDataStreamer;
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

import static org.apache.ignite.internal.processors.query.RunningQueryManager.SQL_USER_QUERIES_REG_NAME;

public class SqlStatisticsUserQueriesTest extends GridCommonAbstractTest {
    /**
     * Number of rows in the test table.
     */
    private static final int TABLE_SIZE = 10_000;

    /**
     * Start the cache with a test table and test data.
     */
    private IgniteCache createCacheFrom(Ignite node) {
        CacheConfiguration<Integer, String> ccfg = new CacheConfiguration<Integer, String>("TestCache")
            .setSqlFunctionClasses(SqlStatisticsMemoryQuotaTest.SuspendQuerySqlFunctions.class)
            .setQueryEntities(Collections.singleton(
                new QueryEntity(Integer.class.getName(), String.class.getName())
                    .setTableName("TAB")
                    .addQueryField("id", Integer.class.getName(), null)
                    .addQueryField("name", String.class.getName(), null)
                    .setKeyFieldName("id")
                    .setValueFieldName("name")
            ));

        IgniteCache<Integer, String> cache = node.createCache(ccfg);

        try (IgniteDataStreamer<Object, Object> ds = node.dataStreamer("TestCache")) {
            for (int i = 0; i < TABLE_SIZE; i++)
                ds.addData(i, UUID.randomUUID().toString());
        }
        
        return cache;
    }

    @After
    public void stopAll () {
        stopAllGrids();
    }

    @Test
    public void testInitialValuesAreZero() throws Exception {
        startGrids(2);

        createCacheFrom(grid(0));

        Assert.assertEquals(0, longMetricValue(0, "success"));
        Assert.assertEquals(0, longMetricValue(0, "failed"));
        Assert.assertEquals(0, longMetricValue(0, "canceled"));
        Assert.assertEquals(0, longMetricValue(0, "failedByOOM"));

        Assert.assertEquals(0, longMetricValue(1, "success"));
        Assert.assertEquals(0, longMetricValue(1, "failed"));
        Assert.assertEquals(0, longMetricValue(1, "canceled"));
        Assert.assertEquals(0, longMetricValue(1, "failedByOOM"));
    }

    @Test
    public void testIfDistributedQuerySucceededOnlyReducerSuccessMetricUpdated() throws Exception {
        startGrids(2);

        int qryReducerIdx = 0;
        int qryMapperIdx = 0;

        IgniteCache cache = createCacheFrom(grid(qryReducerIdx));

        cache.query(new SqlFieldsQuery("SELECT * FROM TAB")).getAll();

        cache.query(new SqlFieldsQuery("CREATE INDEX myidx ON TAB(ID)")).getAll();
        cache.query(new SqlFieldsQuery("DROP INDEX myidx")).getAll();

        cache.query(new SqlFieldsQuery("SET STREAMING ON")).getAll();
        cache.query(new SqlFieldsQuery("SET STREAMING OFF")).getAll();

        cache.query(new SqlFieldsQuery("SET STREAMING OFF")).getAll();



    }

    @Test
    public void testUnparseableQueriesAreNotCounted() {

    }

    @Test
    public void testUnregiserableQueriesAreNotCounted() {

    }



    /**
     * Finds LongMetric from sql user queries registry by specified metric name and returns it's value.
     *
     * @param gridIdx index of a grid which metric value to find.
     * @param metricName short name of the metric from the "sql memory" metric registry.
     */
    private long longMetricValue(int gridIdx, String metricName) {
        MetricRegistry sqlMemReg = grid(gridIdx).context().metric().registry(SQL_USER_QUERIES_REG_NAME);

        Metric metric = sqlMemReg.findMetric(metricName);

        Assert.assertNotNull("Didn't find metric " + metricName, metric);

        Assert.assertTrue("Expected long metric, but got "+ metric.getClass(),  metric instanceof LongMetric);

        return ((LongMetric)metric).value();
    }
}
