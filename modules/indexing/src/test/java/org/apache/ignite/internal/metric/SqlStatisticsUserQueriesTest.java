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

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.query.RunningQueryManager;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.spi.metric.Metric;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.ignite.internal.processors.query.RunningQueryManager.SQL_USER_QUERIES_REG_NAME;

/**
 * Tests for statistics of user initiated queries execution.
 *
 * @see RunningQueryManager
 */
public class SqlStatisticsUserQueriesTest extends SqlStatisticsAbstractTest {
    /** Short names of all tested metrics. */
    private static final String[] ALL_METRICS = {"success", "failed", "canceled", "failedByOOM"};

    /** By convention we start queries from node with this grid index. Reduce phase is performed here. */
    private static final int REDUCER_IDX = 0;

    /** The second node index. This node should execute only map parts of the queries. */
    private static final int MAPPER_IDX = 1;


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
            () -> cache.query(new SqlQuery(String.class, "ID < 5")).getAll());

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

    /**
     * Local queries should also be counted.
     *
     * @throws Exception if failed.
     */
    @Test
    public void testIfLocalQuerySucceedsMetricIsUpdated()  throws Exception {
        startGrids(2);

        IgniteCache cache = createCacheFrom(grid(REDUCER_IDX));

        assertOnlyOneMetricIncrementedOnReducer("success",
            () -> cache.query(new SqlFieldsQuery("SELECT * FROM TAB WHERE ID < 100").setLocal(true)).getAll());

        assertOnlyOneMetricIncrementedOnReducer("success",
            () -> cache.query(new SqlQuery(String.class, "ID < 5").setLocal(true)).getAll());
    }

    /**
     * Verify that if query fails at runtime only appropriate reducer metric is updated.
     *
     * @throws Exception on fail.
     */
    @Test
    public void testIfParsableQueryFailedOnlyReducerMetricIsUpdated() throws Exception {
        startGrids(2);

        IgniteCache cache = createCacheFrom(grid(REDUCER_IDX));

        assertOnlyOneMetricIncrementedOnReducer("failed",
            () -> GridTestUtils.assertThrows(
                log,
                () -> cache.query(new SqlFieldsQuery("SELECT * FROM TAB WHERE ID = failFunction()")).getAll(),
                CacheException.class,
                null)
        );
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

        assertOnlyOneMetricIncrementedOnReducer("failed",
            () -> GridTestUtils.assertThrows(
                log,
                () -> cache.query(new SqlFieldsQuery("SELECT * FROM TAB")).getAll(),
                CacheException.class,
                null)
        );
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

        assertNegativeMetricIncrementedOnReducer("failedByOOM",
            () -> GridTestUtils.assertThrows(
                log,
                () -> cache.query(new SqlFieldsQuery("SELECT * FROM TAB GROUP BY NAME")).getAll(),
                CacheException.class,
                null)
        );
    }

    /**
     * Check that unparseable query doesn't affect any metric value.
     *
     * @throws Exception if failed.
     */
    @Test
    public void testUnparseableQueriesAreNotCounted() throws Exception {
        startGrids(2);

        IgniteCache cache = createCacheFrom(grid(REDUCER_IDX));

        assertMetricsRemainTheSame(() -> {
            GridTestUtils.assertThrows(
                log,
                () -> cache.query(new SqlFieldsQuery("THIS IS NOT A SQL STATEMENT")).getAll(),
                CacheException.class,
                "Failed to parse query");

        });
    }

    @Test
    public void testUnregiserableQueriesAreNotCounted() throws Exception {

    }

    /**
     * Verify that after specified action is performed, all metrics are left unchanged.
     *
     * @param act Action.
     */
    private void assertMetricsRemainTheSame(Runnable act) {
        assertMetricsAre(fetchAllMetrics(REDUCER_IDX), fetchAllMetrics(MAPPER_IDX),  act);
    }


    /**
     * Verify that after specified action is performed, specified 'negative' metric and general failure metric are
     * incremented.
     *
     * @param metric Metric.
     * @param act Action.
     */
    private void assertNegativeMetricIncrementedOnReducer(String metric, Runnable act) {
        Map<String, Long> expValuesMapper = fetchAllMetrics(MAPPER_IDX);

        Map<String, Long> expValuesReducer = fetchAllMetrics(REDUCER_IDX);

        expValuesReducer.compute("failed", (name, val) -> val + 1);

        expValuesReducer.compute(metric, (name, val) -> val + 1);

        assertMetricsAre(expValuesReducer, expValuesMapper,  act);
    }



    /**
     * Verify that after specified action is performed, specified metric is incremented.
     *
     * @param metric Metric.
     * @param act Action.
     */
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
