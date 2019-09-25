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

package org.apache.ignite.internal.processors.cache.msgtimelogging;

import java.util.Map;
import java.util.UUID;
import java.util.stream.LongStream;
import javax.management.MalformedObjectNameException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxQueryEnlistResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxQueryFirstEnlistRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicFullUpdateRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicSingleUpdateRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicUpdateResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearLockRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearLockResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxEnlistRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxEnlistResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareResponse;
import org.apache.ignite.internal.processors.metric.impl.HistogramMetric;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpiMBean;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_COMM_SPI_TIME_HIST_BOUNDS;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_ENABLE_MESSAGES_TIME_LOGGING;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationMetricsListener.DEFAULT_HIST_BOUNDS;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;

/**
 * Tests for CommunicationSpi time metrics.
 */
@WithSystemProperty(key = IGNITE_ENABLE_MESSAGES_TIME_LOGGING, value = "true")
public class CacheMessagesTimeLoggingTest extends GridCacheMessagesTimeLoggingAbstractTest {
    /** */
    @Test
    public void testGridDhtTxPrepareRequestTimeLogging() throws MalformedObjectNameException {
        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        populateCache(cache);

        checkOutgoingEventsNum(GridDhtTxPrepareRequest.class, GridDhtTxPrepareResponse.class);
    }

    /**
     * Near node sends requests to primary node but gets responses from backup node
     * for atomic caches with full sync mode.
     * Time logging must be disabled for this case.
     */
    @Test
    public void testAtomicFullSyncCache() throws MalformedObjectNameException {
        IgniteCache<Integer, Integer> cache0 = grid(0).createCache(new CacheConfiguration<Integer, Integer>()
                                                                            .setName("some_cache_0")
                                                                            .setAtomicityMode(ATOMIC)
                                                                            .setWriteSynchronizationMode(FULL_SYNC));

        populateCache(cache0);

        TcpCommunicationSpiMBean mbean = mbean(0);

        Map<UUID, Map<String, HistogramMetric>> nodeMap = mbean.getOutMetricsByNodeByMsgClass();

        assertNotNull(nodeMap);

        assertTrue("Some timestamps are unexpectedly registered", nodeMap.isEmpty());
    }

    /** */
    @Test
    public void testGridNearAtomicUpdateLogging() throws MalformedObjectNameException {
        IgniteCache<Integer, Integer> cache0 = grid(0).createCache(new CacheConfiguration<Integer, Integer>()
                                                                            .setName("some_cache_0")
                                                                            .setAtomicityMode(ATOMIC));

        populateCache(cache0);

        RecordingSpi spi = (RecordingSpi)grid(0).configuration().getCommunicationSpi();

        HistogramMetric metric = getMetric(0, 1, GridNearAtomicUpdateResponse.class);
        assertNotNull("HistogramMetric not found", metric);

        long metricSum = LongStream.of(metric.value()).sum();

        String metricName1 = metricName(grid(1).localNode().id(), GridNearAtomicSingleUpdateRequest.class);
        String metricName2 = metricName(grid(1).localNode().id(), GridNearAtomicFullUpdateRequest.class);

        Integer eventsNum1 = spi.getClassesMap().get(metricName1);
        Integer eventsNum2 = spi.getClassesMap().get(metricName2);

        String msg = "metricSum: " + metricSum + ", " +
                     GridNearAtomicSingleUpdateRequest.class.getSimpleName() + ": " + eventsNum1 +
                     GridNearAtomicFullUpdateRequest.class.getSimpleName() + ": " + eventsNum2;

        assertEquals(msg, metricSum, (long)eventsNum1 + (long)eventsNum2);
    }

    /** */
    @Test
    public void testTransactions() throws MalformedObjectNameException {
        IgniteCache<Integer, Integer> cache0 = grid(0).createCache(new CacheConfiguration<Integer, Integer>()
                                                                            .setName("some_cache_0")
                                                                            .setBackups(1)
                                                                            .setAtomicityMode(TRANSACTIONAL));

            try (Transaction tx = grid(0).transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            populateCache(cache0);

            tx.commit();
        }

        checkOutgoingEventsNum(GridNearLockRequest.class, GridNearLockResponse.class);
        checkOutgoingEventsNum(GridNearTxPrepareRequest.class, GridNearTxPrepareResponse.class);
    }

    /** */
    @Test
    public void testGridNearTxEnlistRequest() throws MalformedObjectNameException {
        IgniteCache<Integer, Integer> cache0 = grid(0).createCache(new CacheConfiguration<Integer, Integer>()
                                                                            .setName("some_cache_0")
                                                                            .setBackups(1)
                                                                            .setAtomicityMode(TRANSACTIONAL_SNAPSHOT));

        try (Transaction tx = grid(0).transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            populateCache(cache0);

            tx.commit();
        }

        checkOutgoingEventsNum(GridNearTxEnlistRequest.class, GridNearTxEnlistResponse.class);
        checkOutgoingEventsNum(GridNearTxPrepareRequest.class, GridNearTxPrepareResponse.class);
        checkOutgoingEventsNum(GridDhtTxQueryFirstEnlistRequest.class, GridDhtTxQueryEnlistResponse.class);
    }

    /**
     * @throws Exception if failed to start grid.
     */
    @Test
    public void testMetricBounds() throws Exception {
        try {
            IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

            populateCache(cache);

            HistogramMetric metric = getMetric(0, 1, GridDhtTxPrepareResponse.class);

            assertNotNull(metric);

            assertEquals(DEFAULT_HIST_BOUNDS.length + 1, metric.value().length);

            // Checking custom metrics bound.
            System.setProperty(IGNITE_COMM_SPI_TIME_HIST_BOUNDS, "1,10,100");

            IgniteEx grid3 = startGrid(GRID_CNT);

            IgniteCache<Integer, Integer> cache3 = grid3.createCache(new CacheConfiguration<Integer, Integer>()
                                                                            .setName("cache3")
                                                                            .setBackups(GRID_CNT));

            cache3.put(1, 1);

            HistogramMetric metric3 = getMetric(GRID_CNT, 1, GridNearAtomicUpdateResponse.class);
            assertNotNull(metric3);

            assertEquals(4, metric3.value().length);

            // Checking invalid custom metrics bound.
            System.setProperty(IGNITE_COMM_SPI_TIME_HIST_BOUNDS, "wrong_val");

            IgniteEx grid4 = startGrid(GRID_CNT + 1);

            IgniteCache<Integer, Integer> cache4 = grid4.createCache(new CacheConfiguration<Integer, Integer>()
                                                                            .setName("cache4")
                                                                            .setBackups(GRID_CNT + 1));

            cache4.put(1, 1);

            HistogramMetric metric4 = getMetric(GRID_CNT + 1, 1, GridNearAtomicUpdateResponse.class);
            assertNotNull(metric4);

            assertEquals(DEFAULT_HIST_BOUNDS.length + 1, metric4.value().length);
        } finally {
            System.clearProperty(IGNITE_COMM_SPI_TIME_HIST_BOUNDS);
        }
    }

    /**
     * @throws Exception if test failed.
     */
    @Test
    public void testMetricClearOnNodeLeaving() throws Exception {
        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        populateCache(cache);

        UUID leavingNodeId = grid(1).localNode().id();

        HistogramMetric metric = getMetric(0, leavingNodeId, GridDhtTxPrepareResponse.class);

        assertNotNull(metric);

        stopGrid(1);

        awaitPartitionMapExchange();

        HistogramMetric metricAfterNodeStop = getMetric(0, leavingNodeId, GridDhtTxPrepareResponse.class);

        assertNull(metricAfterNodeStop);
    }

    /**
     * Tests metrics disabling
     */
    @Test
    @WithSystemProperty(key = IGNITE_ENABLE_MESSAGES_TIME_LOGGING, value = "not boolean value")
    public void testDisabledMetric() throws MalformedObjectNameException {
        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        populateCache(cache);

        HistogramMetric metric = getMetric(0, 1, GridDhtTxPrepareResponse.class);

        assertNull("Metrics unexpectedly enabled", metric);
    }
}
