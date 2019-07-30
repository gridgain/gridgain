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

package org.apache.ignite.internal.processors.cache;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.LongStream;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.communication.GridIoManager;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxQueryFirstEnlistRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicFullUpdateRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicSingleUpdateRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearLockRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxEnlistRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.impl.HistogramMetric;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_COMM_SPI_TIME_HIST_BOUNDS;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_ENABLE_MESSAGES_TIME_LOGGING;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_MESSAGES_INFO_STORE_TIME;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;
import static org.apache.ignite.internal.managers.communication.GridIoManager.DEFAULT_HIST_BOUNDS;
import static org.apache.ignite.internal.managers.communication.GridIoManager.METRIC_REGISTRY_NAME;
import static org.apache.ignite.internal.processors.cache.GridCacheMessage.MIN_MSG_ID;

/**
 * Tests for CommunicationSpi time metrics.
 */
public class CacheMessagesTimeLoggingTest  extends GridCommonAbstractTest {
    /** Grid count. */
    private static final int GRID_CNT = 3;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setAtomicityMode(TRANSACTIONAL);

        ccfg.setBackups(2);

        cfg.setCacheConfiguration(ccfg);

        cfg.setCommunicationSpi(new RecordingSpi());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        System.setProperty(IGNITE_ENABLE_MESSAGES_TIME_LOGGING, "true");

        startGrids(GRID_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     *
     */
    @Test
    public void testGridDhtTxPrepareRequestTimeLogging() {
        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        populateCache(cache);

        checkEventsNum(GridDhtTxPrepareRequest.class);
    }

    /**
     *
     */
    @Test
    public void testGridNearAtomicUpdateLogging() {
        IgniteCache<Integer, Integer> cache = grid(0).createCache(new CacheConfiguration<Integer, Integer>()
                                                                            .setName("some_cache")
                                                                            .setAtomicityMode(ATOMIC));

        populateCache(cache);

        checkEventsNum(GridNearAtomicSingleUpdateRequest.class);
        checkEventsNum(GridNearAtomicFullUpdateRequest.class);
    }

    /**
     *
     */
    @Test
    public void testTransactions() {
        IgniteCache<Integer, Integer> cache = grid(0).createCache(new CacheConfiguration<Integer, Integer>()
                                                                            .setName("some_cache")
                                                                            .setBackups(1)
                                                                            .setAtomicityMode(TRANSACTIONAL));


        try (Transaction tx = grid(0).transactions().txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.READ_COMMITTED)) {
            populateCache(cache);

            tx.commit();
        }

        checkEventsNum(GridNearLockRequest.class);
        checkEventsNum(GridNearTxPrepareRequest.class);
    }

    /**
     *
     */
    @Test
    public void testGridNearTxEnlistRequest() {
        IgniteCache<Integer, Integer> cache = grid(0).createCache(new CacheConfiguration<Integer, Integer>()
                                                                            .setName("some_cache")
                                                                            .setBackups(1)
                                                                            .setAtomicityMode(TRANSACTIONAL_SNAPSHOT));

        try (Transaction tx = grid(0).transactions().txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.READ_COMMITTED)) {
            populateCache(cache);

            tx.commit();
        }

        checkEventsNum(GridNearTxEnlistRequest.class);
        checkEventsNum(GridNearTxPrepareRequest.class);
        checkEventsNum(GridDhtTxQueryFirstEnlistRequest.class);
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
     *
     */
    private void checkEventsNum(Class msgClass) {
        checkEventsNum(grid(0), grid(1), msgClass);
    }

    /**
     * Compares sent events number with histogram entries number.
     * Fails if these numbers differ.
     */
    private void checkEventsNum(IgniteEx source, IgniteEx target, Class msgClass) {
        RecordingSpi spi = (RecordingSpi)source.configuration().getCommunicationSpi();

        HistogramMetric metric = getMetric(source, target, msgClass);
        assertNotNull("HistogramMetric not found", metric);

        String metricName = getTimeStorage(source).metricName(target.localNode().id(), msgClass);

        long sum = LongStream.of(metric.value()).sum();

        Integer eventsNum = spi.classesMap.get(metricName);
        assertNotNull("Value " + metricName + " not found in classesMap", eventsNum);

        assertTrue("Unexpected metric data amount for " + msgClass + ": " + sum + ". Events num: " + eventsNum, sum == eventsNum);
    }

    /**
     * @throws InterruptedException if {@code Thread#sleep} failed.
     */
    @Test
    public void testEviction() throws InterruptedException {
        System.setProperty(IGNITE_MESSAGES_INFO_STORE_TIME, "1");

        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        // Needed to create timestamp storage.
        cache.put(1, 1);

        GridIoManager.ReqRespTimeStorage timeStorage = getTimeStorage(grid(0));

        Map<UUID, Map<Class<? extends Message>, Map<Long, Long>>> storage = U.field(timeStorage, "storage");
        assertNotNull(storage);

        Map<Long, Long> timestampMap = storage.get(grid(1).localNode().id()).get(GridDhtTxPrepareRequest.class);

        final int entriesNum = 20;

        for (long i = MIN_MSG_ID; i < MIN_MSG_ID + entriesNum; i++)
            timestampMap.putIfAbsent(i, System.nanoTime());

        Thread.sleep(2000);

        assertTrue("Unexpected map size before eviction: " + timestampMap.size(), timestampMap.size() == entriesNum);

        timestampMap.putIfAbsent(MIN_MSG_ID + entriesNum + 1, System.nanoTime());

        assertTrue("Unexpected map size after eviction: " + timestampMap.size(), timestampMap.size() == 1);
    }

    /**
     * Tests metrics disabling
     */
    @Test
    public void testDisabledMetric() {
        System.setProperty(IGNITE_ENABLE_MESSAGES_TIME_LOGGING, "not boolean value");

        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        cache.put(1, 1);

        HistogramMetric metric = getMetric(grid(0), grid(1), GridDhtTxPrepareRequest.class);

        assertNull("Metrics unexpectedly enabled", metric);
    }

    /**
     * @throws Exception if failed to start grid.
     */
    @Test
    public void testMetricBounds() throws Exception {
        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        cache.put(1, 1);

        HistogramMetric metric = getMetric(grid(0), grid(1), GridDhtTxPrepareRequest.class);

        assertEquals(DEFAULT_HIST_BOUNDS.length + 1, metric.value().length);

        // Checking custom metrics bound.
        System.setProperty(IGNITE_COMM_SPI_TIME_HIST_BOUNDS, "1,10,100");

        IgniteEx grid3 = startGrid(GRID_CNT);

        IgniteCache<Integer, Integer> cache3 = grid3.createCache(new CacheConfiguration<Integer, Integer>()
                                                                            .setName("cache3")
                                                                            .setBackups(GRID_CNT));

        cache3.put(1, 1);

        HistogramMetric metric3 = getMetric(grid3, grid(1), GridNearAtomicSingleUpdateRequest.class);

        assertEquals(4, metric3.value().length);

        // Checking invalid custom metrics bound.
        System.setProperty(IGNITE_COMM_SPI_TIME_HIST_BOUNDS, "wrong_val");

        IgniteEx grid4 = startGrid(GRID_CNT + 1);

        IgniteCache<Integer, Integer> cache4 = grid4.createCache(new CacheConfiguration<Integer, Integer>()
                                                                        .setName("cache4")
                                                                        .setBackups(GRID_CNT + 1));

        cache4.put(1, 1);

        HistogramMetric metric4 = getMetric(grid4, grid(1), GridNearAtomicSingleUpdateRequest.class);

        assertEquals(DEFAULT_HIST_BOUNDS.length + 1, metric4.value().length);
    }


    /**
     * @param sourceNode Node that stores metric.
     * @param targetNode Node where requests are sent.
     * @param msgClass Metric request class.
     * @return {@code HistogramMetric} for {@code msgClass}.
     */
    @Nullable private HistogramMetric getMetric(IgniteEx sourceNode, IgniteEx targetNode, Class msgClass) {
        GridIoManager.ReqRespTimeStorage timeStorage = getTimeStorage(sourceNode);

        String metricName = timeStorage.metricName(targetNode.localNode().id(), msgClass);

        MetricRegistry registry = sourceNode.context().metric().registry(METRIC_REGISTRY_NAME);

        return (HistogramMetric)registry.findMetric(metricName);
    }

    /**
     *
     */
    private static GridIoManager.ReqRespTimeStorage getTimeStorage(IgniteEx igniteEx) {
        GridIoManager.ReqRespTimeStorage timeStorage = U.field(igniteEx.context().io(), "timeStorage");

        assertNotNull(timeStorage);

        return timeStorage;
    }

    /**
     * Counts sent messages num per message class.
     */
    private static class RecordingSpi extends TcpCommunicationSpi {
        /** */
        private Map<String, Integer> classesMap = new HashMap<>();

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg) throws IgniteSpiException {
            recordMessage(node, msg);

            super.sendMessage(node, msg);
        }

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg,
            IgniteInClosure<IgniteException> ackC) throws IgniteSpiException {
            recordMessage(node, msg);

            super.sendMessage(node, msg, ackC);
        }

        /**
         *
         */
        private void recordMessage(ClusterNode node, Message msg) {
            if (!node.isLocal()) {
                Message msg0 = msg;

                if (msg instanceof GridIoMessage)
                    msg0 = ((GridIoMessage)msg).message();

                GridIoManager.ReqRespTimeStorage timeStorage = getTimeStorage((IgniteEx)ignite);

                classesMap.merge(timeStorage.metricName(node.id(), msg0.getClass()), 1, Integer::sum);
            }
        }
    }
}
