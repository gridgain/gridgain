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

import java.util.HashMap;
import java.util.Map;
import javax.management.MalformedObjectNameException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxQueryFirstEnlistRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicFullUpdateRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicSingleUpdateRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearLockRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxEnlistRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
import org.apache.ignite.internal.processors.metric.HistogramMetric;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationMetricsListener;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_COMM_SPI_TIME_HIST_BOUNDS;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_ENABLE_MESSAGES_TIME_LOGGING;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_MESSAGES_INFO_STORE_TIME;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;
import static org.apache.ignite.internal.managers.communication.GridIoManager.DEFAULT_HIST_BOUNDS;

/**
 * Tests for CommunicationSpi time metrics.
 */
public class CacheMessagesTimeLoggingTest extends GridCacheMessagesTimeLoggingAbstractTest {
    /**
     *
     */
    @Test
    public void testGridDhtTxPrepareRequestTimeLogging() throws MalformedObjectNameException {
        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        populateCache(cache);

        checkOutcomingEventsNum(GridDhtTxPrepareRequest.class);
    }

    /**
     *
     */
    @Test
    public void testGridNearAtomicUpdateLogging() throws MalformedObjectNameException {
        IgniteCache<Integer, Integer> cache0 = grid(0).createCache(new CacheConfiguration<Integer, Integer>()
                                                                            .setName("some_cache_0")
                                                                            .setAtomicityMode(ATOMIC));

        populateCache(cache0);

        checkOutcomingEventsNum(GridNearAtomicSingleUpdateRequest.class);
        checkOutcomingEventsNum(GridNearAtomicFullUpdateRequest.class);

        IgniteCache<Integer, Integer> cache1 = grid(1).createCache(new CacheConfiguration<Integer, Integer>()
                                                                            .setName("some_cache_1")
                                                                            .setAtomicityMode(ATOMIC));

        populateCache(cache1);

        checkIncomingEventsNum(GridNearAtomicSingleUpdateRequest.class);
        checkIncomingEventsNum(GridNearAtomicFullUpdateRequest.class);
    }

    /**
     *
     */
    @Test
    public void testTransactions() throws MalformedObjectNameException {
        IgniteCache<Integer, Integer> cache0 = grid(0).createCache(new CacheConfiguration<Integer, Integer>()
                                                                            .setName("some_cache_0")
                                                                            .setBackups(1)
                                                                            .setAtomicityMode(TRANSACTIONAL));


        try (Transaction tx = grid(0).transactions().txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.READ_COMMITTED)) {
            populateCache(cache0);

            tx.commit();
        }

        checkOutcomingEventsNum(GridNearLockRequest.class);
        checkOutcomingEventsNum(GridNearTxPrepareRequest.class);

        IgniteCache<Integer, Integer> cache1 = grid(1).createCache(new CacheConfiguration<Integer, Integer>()
                                                                            .setName("some_cache_1")
                                                                            .setBackups(1)
                                                                            .setAtomicityMode(TRANSACTIONAL));


        try (Transaction tx = grid(1).transactions().txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.READ_COMMITTED)) {
            populateCache(cache1);

            tx.commit();
        }

        checkIncomingEventsNum(GridNearLockRequest.class);
        checkIncomingEventsNum(GridNearTxPrepareRequest.class);
    }

    /**
     *
     */
    @Test
    public void testGridNearTxEnlistRequest() throws MalformedObjectNameException {
        IgniteCache<Integer, Integer> cache0 = grid(0).createCache(new CacheConfiguration<Integer, Integer>()
                                                                            .setName("some_cache_0")
                                                                            .setBackups(1)
                                                                            .setAtomicityMode(TRANSACTIONAL_SNAPSHOT));

        try (Transaction tx = grid(0).transactions().txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.READ_COMMITTED)) {
            populateCache(cache0);

            tx.commit();
        }

        checkOutcomingEventsNum(GridNearTxEnlistRequest.class);
        checkOutcomingEventsNum(GridNearTxPrepareRequest.class);
        checkOutcomingEventsNum(GridDhtTxQueryFirstEnlistRequest.class);

        IgniteCache<Integer, Integer> cache1 = grid(1).createCache(new CacheConfiguration<Integer, Integer>()
                                                                            .setName("some_cache_1")
                                                                            .setBackups(1)
                                                                            .setAtomicityMode(TRANSACTIONAL_SNAPSHOT));

        try (Transaction tx = grid(1).transactions().txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.READ_COMMITTED)) {
            populateCache(cache1);

            tx.commit();
        }

        checkIncomingEventsNum(GridNearTxEnlistRequest.class);
        checkIncomingEventsNum(GridNearTxPrepareRequest.class);
    }

    /**
     *
     */
    private void populateCache(IgniteCache<Integer, Integer> cache) {
        Map<Integer, Integer> map = new HashMap<>();

        for (int i = 0; i < 20; ++i) {
            cache.put(i, i);
            map.put(i + 20, i * 2);
        }

        cache.putAll(map);
    }



    /**
     * @throws InterruptedException if {@code Thread#sleep} failed.
     */
    @Test
    public void testEviction() throws InterruptedException {
        System.setProperty(IGNITE_MESSAGES_INFO_STORE_TIME, "1");

        Map<Long, Long> map = new TcpCommunicationMetricsListener.TimestampMap();

        map.put(10L, System.nanoTime());
        map.putIfAbsent(20L, System.nanoTime());

        Thread.sleep(2000);

        assertTrue("Unexpected map size before eviction: " + map.size(), map.size() == 2);

        map.putIfAbsent(30L, System.nanoTime());

        assertTrue("Unexpected map size after eviction: " + map.size(), map.size() == 1);
    }

    /**
     * @throws Exception if failed to start grid.
     */
    @Test
    public void testMetricBounds() throws Exception {
        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        cache.put(1, 1);

        HistogramMetric metric = getMetric(0, grid(1), GridDhtTxPrepareRequest.class, true);

        assertNotNull(metric);

        assertEquals(DEFAULT_HIST_BOUNDS.length + 1, metric.value().length);

        // Checking custom metrics bound.
        System.setProperty(IGNITE_COMM_SPI_TIME_HIST_BOUNDS, "1,10,100");

        IgniteEx grid3 = startGrid(GRID_CNT);

        IgniteCache<Integer, Integer> cache3 = grid3.createCache(new CacheConfiguration<Integer, Integer>()
                                                                            .setName("cache3")
                                                                            .setBackups(GRID_CNT));

        cache3.put(1, 1);

        HistogramMetric metric3 = getMetric(GRID_CNT, grid(1), GridNearAtomicSingleUpdateRequest.class, true);
        assertNotNull(metric3);

        assertEquals(4, metric3.value().length);

        // Checking invalid custom metrics bound.
        System.setProperty(IGNITE_COMM_SPI_TIME_HIST_BOUNDS, "wrong_val");

        IgniteEx grid4 = startGrid(GRID_CNT + 1);

        IgniteCache<Integer, Integer> cache4 = grid4.createCache(new CacheConfiguration<Integer, Integer>()
                                                                        .setName("cache4")
                                                                        .setBackups(GRID_CNT + 1));

        cache4.put(1, 1);

        HistogramMetric metric4 = getMetric(GRID_CNT + 1, grid(1), GridNearAtomicSingleUpdateRequest.class, true);
        assertNotNull(metric4);

        assertEquals(DEFAULT_HIST_BOUNDS.length + 1, metric4.value().length);
    }

    /** {@inheritDoc} */
    @Override void setEnabledParam() {
        System.setProperty(IGNITE_ENABLE_MESSAGES_TIME_LOGGING, "true");
    }
}
