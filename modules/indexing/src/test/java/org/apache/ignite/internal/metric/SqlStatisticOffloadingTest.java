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

import java.util.Objects;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.query.oom.DiskSpillingAbstractTest;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.spi.metric.Metric;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.ignite.configuration.IgniteConfiguration.DFLT_QUERY_THREAD_POOL_SIZE;

/**
 * Test cases for offloading statistics.
 */
public class SqlStatisticOffloadingTest extends DiskSpillingAbstractTest {
    /** {@inheritDoc} */
    @Override protected int nodeCount() {
        return 2;
    }

    /** {@inheritDoc} */
    @Override protected boolean startClient() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected boolean fromClient() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setSqlConfiguration(new SqlConfiguration()
                .setSqlGlobalMemoryQuota("16k")
                .setSqlOffloadingEnabled(true)
            );
    }

    /**
     * Start grid with 16k global query memory and disk offloading enabled
     * Remember current offload metrics
     * Execute query that fits 16k result
     * Ensure that query succeed
     * Ensure that metrics does not changed
     * Execute query that overflows 16k result
     * Ensure that query succeed
     * Ensure that disk spilling happened
     * Ensure that metrics increased relative to p.2
     */
    @Test
    public void testOffloadStats() {
        Metrics m0 = withdrawMetrics();

        // Execute query that fits 16k result.
        checkQuery(Result.SUCCESS_NO_OFFLOADING, "SELECT * FROM person WHERE id < 10");

        // Ensure that metrics does not changed.
        assertEquals(m0, withdrawMetrics());

        // Execute query that overflows 16k result.
        checkQuery(Result.SUCCESS_WITH_OFFLOADING, "SELECT * FROM person WHERE id < 100 GROUP BY name ORDER BY id");

        // Ensure that metrics increased.
        Metrics m1 = withdrawMetrics();

        Metrics expected = m0.add(Metrics.of(6249, 6249, 1, 16878, 16878, 2)); // Expected numbers of written bytes.

        assertEquals(expected, m1);

        if (log.isInfoEnabled())
            log.info("Offloading stats: \n" + m1);
    }

    /**
     * Start grid with 16k global query memory and disk offloading enabled
     * Remember current offload metrics
     * Execute query that overflows 16k result
     * Ensure that disk spilling happened
     * Ensure that metrics increased relative to p.2
     * Calculate and remember difference between old and new metrics
     * Execute same query 100 times in parallel for DFLT_QUERY_THREAD_POOL_SIZE threads
     * Ensure that all queries succeed
     * Ensure that disk spilling happened
     * Ensure that metrics increased 100 times relative to p.6
     */
    @Test
    public void testOffloadStatsParallel() throws Exception {
        // Remember current offload metrics.
        Metrics m0 = withdrawMetrics();

        final String qry = "SELECT * FROM person WHERE id < 100 GROUP BY name ORDER BY id";

        // Execute query that overflows 16k result.
        checkQuery(Result.SUCCESS_WITH_OFFLOADING, qry);

        Metrics m1 = withdrawMetrics();

        // Calculate and remember difference between old and new metrics.
        Metrics diff = m1.subtract(m0);

        assertTrue("Metrics not changed after offloading: " + diff, diff.notZero());

        final int iterations = 100;

        // Execute same query 100 times in parallel for DFLT_QUERY_THREAD_POOL_SIZE threads.
        checkQuery(Result.SUCCESS_WITH_OFFLOADING, qry, DFLT_QUERY_THREAD_POOL_SIZE, iterations);

        // Ensure that metrics increased 100 times relative to p.6.
        Metrics m2 = withdrawMetrics();

        Metrics expected = m1.add(diff.multiply(DFLT_QUERY_THREAD_POOL_SIZE * iterations));

        assertEquals(expected, m2);

        if (log.isInfoEnabled())
            log.info("Offloading stats: \n" + m2);
    }

    /**
     * Finds LongMetric from sql memory registry by specified metric name and returns it's value.
     * @param client flag whether to get metrics from
     * @param metricName short name of the metric from the "sql memory" metric registry.
     */
    private long longMetricValue(boolean client, String metricName) {
        IgniteEx node = client ? grid("client") : grid(0);
        MetricRegistry sqlMemReg = node.context().metric().registry(SqlMemoryStatisticsHolder.SQL_QUOTAS_REG_NAME);

        Metric metric = sqlMemReg.findMetric(metricName);

        Assert.assertNotNull("Didn't find metric " + metricName, metric);

        Assert.assertTrue("Expected long metric, but got "+ metric.getClass(), metric instanceof LongMetric);

        return ((LongMetric)metric).value();
    }

    /**
     * @return Current metrics for client and one of the server nodes.
     */
    private Metrics withdrawMetrics() {
        return Metrics.of(
            longMetricValue(false, "OffloadingWritten"),
            longMetricValue(false, "OffloadingRead"),
            longMetricValue(false, "OffloadedQueriesNumber"),
            longMetricValue(true, "OffloadingWritten"),
            longMetricValue(true, "OffloadingRead"),
            longMetricValue(true, "OffloadedQueriesNumber")
        );
    }

    /**
     * Metrics data.
     */
    private static class Metrics {
        /** */
        private final long writtenServer;

        /** */
        private final long readServer;

        /** */
        private final long filesNumServer;

        /** */
        private final long writtenClient;

        /** */
        private final long readClient;

        /** */
        private final long fileNumClient;

        /** */
        private Metrics(
            long writtenServer,
            long readServer,
            long filesNumServer,
            long writtenClient,
            long readClient,
            long fileNumClient) {
            this.writtenServer = writtenServer;
            this.readServer = readServer;
            this.filesNumServer = filesNumServer;
            this.writtenClient = writtenClient;
            this.readClient = readClient;
            this.fileNumClient = fileNumClient;
        }

        /**
         * @param add Metrics to add.
         * @return Sum of metrics.
         */
        Metrics add(Metrics add) {
            return of(writtenServer + add.writtenServer,
                readServer + add.readServer,
                filesNumServer + add.filesNumServer,
                writtenClient + add.writtenClient,
                readClient + add.readClient,
                fileNumClient + add.fileNumClient);
        }

        /**
         * @param sub Metrics to subtract.
         * @return Metrics difference.
         */
        Metrics subtract(Metrics sub) {
            return of(writtenServer - sub.writtenServer,
                readServer - sub.readServer,
                filesNumServer - sub.filesNumServer,
                writtenClient - sub.writtenClient,
                readClient - sub.readClient,
                fileNumClient - sub.fileNumClient);
        }

        /**
         * @param multiplier Multiplier
         * @return The result multiplication.
         */
        Metrics multiply(long multiplier) {
            return of(writtenServer * multiplier,
                readServer * multiplier,
                filesNumServer * multiplier,
                writtenClient * multiplier,
                readClient * multiplier,
                fileNumClient * multiplier);
        }

        /**
         * @return {@code True} if metrics is zero
         */
        boolean notZero() {
            return this.writtenServer > 0 &&
            this.readServer > 0 &&
            this.filesNumServer > 0 &&
            this.writtenClient > 0 &&
            this.readClient > 0 &&
            this.fileNumClient > 0;
        }

        /**
         * Fabric method.
         */
        static Metrics of(
            long writtenServer,
            long readServer,
            long filesNumServer,
            long writtenClient,
            long readClient,
            long fileNumClient) {
            return new Metrics(writtenServer,
            readServer,
            filesNumServer,
            writtenClient,
            readClient,
            fileNumClient);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            Metrics metrics = (Metrics)o;
            return writtenServer == metrics.writtenServer &&
                readServer == metrics.readServer &&
                filesNumServer == metrics.filesNumServer &&
                writtenClient == metrics.writtenClient &&
                readClient == metrics.readClient &&
                fileNumClient == metrics.fileNumClient;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(writtenServer, readServer, filesNumServer, writtenClient, readClient, fileNumClient);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Metrics{" +
                "writtenServer=" + writtenServer +
                ", readServer=" + readServer +
                ", filesNumServer=" + filesNumServer +
                ", writtenClient=" + writtenClient +
                ", readClient=" + readClient +
                ", fileNumClient=" + fileNumClient +
                '}';
        }
    }
}