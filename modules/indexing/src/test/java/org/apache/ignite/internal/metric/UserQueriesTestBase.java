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
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.spi.metric.Metric;
import org.junit.Assert;

import static org.apache.ignite.internal.processors.query.RunningQueryManager.SQL_USER_QUERIES_REG_NAME;

/**
 * Test base for the tests for user metrics. Contains methods that are common for the scenarios that require and don't
 * require grid restart.
 */
public class UserQueriesTestBase extends SqlStatisticsAbstractTest {
    /** Short names of all tested metrics. */
    private static final String[] ALL_METRICS = {"success", "failed", "canceled", "failedByOOM"};

    /** By convention we start queries from node with this grid index. Reduce phase is performed here. */
    protected static final int REDUCER_IDX = 0;

    /** The second node index. This node should execute only map parts of the queries. */
    protected static final int MAPPER_IDX = 1;

    /**
     * Verify that after specified action is performed, all metrics are left unchanged.
     *
     * @param act Action.
     */
    protected void assertMetricsRemainTheSame(Runnable act) {
        assertMetricsAre(fetchAllMetrics(REDUCER_IDX), fetchAllMetrics(MAPPER_IDX), act);
    }

    /**
     * Verify that after action is performed, specified metrics gets incremented only on reducer node.
     *
     * @param act action (callback) to perform.
     * @param incrementedMetrics array of metrics to check.
     */
    protected void assertMetricsIncrementedOnlyOnReducer(Runnable act, String... incrementedMetrics) {
        Map<String, Long> expValuesMapper = fetchAllMetrics(MAPPER_IDX);

        Map<String, Long> expValuesReducer = fetchAllMetrics(REDUCER_IDX);

        for (String incMet : incrementedMetrics)
            expValuesReducer.compute(incMet, (name, val) -> val + 1);

        assertMetricsAre(expValuesReducer, expValuesMapper, act);
    }

    /**
     * @param nodeIdx Node which metrics to fetch.
     * @return metrics from specified node (metric name -> metric value)
     */
    private Map<String, Long> fetchAllMetrics(int nodeIdx) {
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
    protected long longMetricValue(int gridIdx, String metricName) {
        MetricRegistry sqlMemReg = grid(gridIdx).context().metric().registry(SQL_USER_QUERIES_REG_NAME);

        Metric metric = sqlMemReg.findMetric(metricName);

        Assert.assertNotNull("Didn't find metric " + metricName, metric);

        Assert.assertTrue("Expected long metric, but got " + metric.getClass(), metric instanceof LongMetric);

        return ((LongMetric)metric).value();
    }
}
