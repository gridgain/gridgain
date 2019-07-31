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
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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

    /** Short names of all tested metrics. */
    private static final String[] ALL_METRICS = {"success", "failed", "canceled", "failedByOOM"};

    /** By convention we start queries from node with this grid index. Reduce phase is performed here. */
    private static final int REDUCER_IDX = 0;

    /** The second node index. This node should execute only map parts of the queries. */
    private static final int MAPPER_IDX = 1;


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

    /**
     *
     */
    @After
    public void stopAll () {
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

    /**
     * Check that one distributed query execution causes only success metric increment only on the reducer node.
     * Various (not all) queries tested : native/h2 parsed; select, ddl, dml, fast delete, update with subselect.
     */
    @Test
    public void testIfDistributedQuerySucceededOnlySuccessReducerMetricUpdated() throws Exception {
        startGrids(2);

        IgniteCache cache = createCacheFrom(grid(REDUCER_IDX));

        assertOnlyOneMetricIncrementedOnReducer("success",
            () -> cache.query(new SqlFieldsQuery("SELECT * FROM TAB")).getAll());

        assertOnlyOneMetricIncrementedOnReducer("success",
            () -> cache.query(new SqlFieldsQuery("CREATE INDEX myidx ON TAB(ID)")).getAll());

        assertOnlyOneMetricIncrementedOnReducer("success",
            () -> cache.query(new SqlFieldsQuery("CREATE TABLE ANOTHER_TAB (ID INT PRIMARY KEY, VAL VARCHAR)")
                .setSchema("PUBLIC")).getAll());

        assertOnlyOneMetricIncrementedOnReducer("success",
            () -> cache.query(new SqlFieldsQuery("DROP INDEX myidx")).getAll());

        assertOnlyOneMetricIncrementedOnReducer("success",
            () -> cache.query(new SqlFieldsQuery("DELETE FROM TAB WHERE ID = 5")).getAll());

        assertOnlyOneMetricIncrementedOnReducer("success",
            () -> cache.query(new SqlFieldsQuery("DELETE FROM TAB WHERE ID > (SELECT AVG(ID) FROM TAB WHERE ID < 20)")).getAll());

        assertOnlyOneMetricIncrementedOnReducer("success",
            () -> cache.query(new SqlFieldsQuery("INSERT INTO TAB VALUES(5, 'Name')")).getAll());

        assertOnlyOneMetricIncrementedOnReducer("success",
            () -> cache.query(new SqlFieldsQuery("MERGE INTO TAB(ID, NAME) VALUES(5, 'NewerName')")).getAll());
    }

    @Test
    public void testLocalQuery()  throws Exception {

    }

    @Test
    public void testGeneralFailedQuery() throws Exception {

    }

    @Test
    public void testOomFailedQuery() throws Exception {

    }

    @Test
    public void testUnparseableQueriesAreNotCounted() throws Exception {

    }

    @Test
    public void testUnregiserableQueriesAreNotCounted() throws Exception {

    }

    private void assertOnlyOneMetricIncrementedOnReducer(String metric, Runnable act) {
        Map<String, Long> expValuesMapper = fetchAllMetrics(MAPPER_IDX);

        Map<String, Long> expValuesReducer = fetchAllMetrics(REDUCER_IDX);

        expValuesReducer.compute(metric, (name, val) -> val + 1);

        assertMetricsAre(expValuesReducer, expValuesMapper,  act);
    }

    /**
     * @param nodeIdx Node which metrics to fetch.
     *
     * @return metrics from specified node (metric name -> metric value)
     */
    private Map <String, Long> fetchAllMetrics(int nodeIdx) {
        return Stream.of(ALL_METRICS).collect(
            Collectors.toMap(
                mName -> mName,
                mName -> longMetricValue(nodeIdx, mName)
            )
        );
    }

    /**
     * Verify that after specified action is performed, metrics on mapper and reducer have specified values.
     *
     * @param expMetricsReducer Expected metrics on reducer.
     * @param expMetricsMapper Expected metrics on mapper.
     * @param act callback to perform. Usually sql query execution.
     */
    private void assertMetricsAre(
        Map<String, Long> expMetricsReducer,
        Map<String, Long> expMetricsMapper,
        Runnable act) {
        act.run();

        expMetricsReducer.forEach((mName, expVal) -> {
            long actVal = longMetricValue(REDUCER_IDX, mName);

            Assert.assertEquals("Unexpected value for metric " + mName, (long)expVal, actVal);
        });

        expMetricsMapper.forEach((mName, expVal) -> {
            long actVal = longMetricValue(MAPPER_IDX, mName);

            Assert.assertEquals("Unexpected value for metric " + mName, (long)expVal, actVal);
        });
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
