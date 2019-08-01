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

import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.LongStream;
import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxQueryFirstEnlistRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicFullUpdateRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicSingleUpdateRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearLockRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxEnlistRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
import org.apache.ignite.internal.processors.metric.HistogramMetric;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationMetricsListener;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpiMBean;
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
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationMetricsListener.metricName;

/**
 * Tests for CommunicationSpi time metrics.
 */
public class CacheMessagesTimeLoggingTest extends GridCommonAbstractTest {
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
    public void testGridDhtTxPrepareRequestTimeLogging() throws MalformedObjectNameException {
        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        populateCache(cache);

        checkOutcommingEventsNum(GridDhtTxPrepareRequest.class);
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

        checkOutcommingEventsNum(GridNearAtomicSingleUpdateRequest.class);
        checkOutcommingEventsNum(GridNearAtomicFullUpdateRequest.class);

        IgniteCache<Integer, Integer> cache1 = grid(1).createCache(new CacheConfiguration<Integer, Integer>()
                                                                            .setName("some_cache_1")
                                                                            .setAtomicityMode(ATOMIC));

        populateCache(cache1);

        checkIncommingEventsNum(GridNearAtomicSingleUpdateRequest.class);
        checkIncommingEventsNum(GridNearAtomicFullUpdateRequest.class);
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

        checkOutcommingEventsNum(GridNearLockRequest.class);
        checkOutcommingEventsNum(GridNearTxPrepareRequest.class);

        IgniteCache<Integer, Integer> cache1 = grid(1).createCache(new CacheConfiguration<Integer, Integer>()
                                                                            .setName("some_cache_1")
                                                                            .setBackups(1)
                                                                            .setAtomicityMode(TRANSACTIONAL));


        try (Transaction tx = grid(1).transactions().txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.READ_COMMITTED)) {
            populateCache(cache1);

            tx.commit();
        }

        checkIncommingEventsNum(GridNearLockRequest.class);
        checkIncommingEventsNum(GridNearTxPrepareRequest.class);
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

        checkOutcommingEventsNum(GridNearTxEnlistRequest.class);
        checkOutcommingEventsNum(GridNearTxPrepareRequest.class);
        checkOutcommingEventsNum(GridDhtTxQueryFirstEnlistRequest.class);

        IgniteCache<Integer, Integer> cache1 = grid(1).createCache(new CacheConfiguration<Integer, Integer>()
                                                                            .setName("some_cache_1")
                                                                            .setBackups(1)
                                                                            .setAtomicityMode(TRANSACTIONAL_SNAPSHOT));

        try (Transaction tx = grid(1).transactions().txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.READ_COMMITTED)) {
            populateCache(cache1);

            tx.commit();
        }

        checkIncommingEventsNum(GridNearTxEnlistRequest.class);
        checkIncommingEventsNum(GridNearTxPrepareRequest.class);
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
    private void checkOutcommingEventsNum(Class msgClass) throws MalformedObjectNameException {
        checkEventsNum(0, grid(1), msgClass, true);
    }

    /**
     *
     */
    private void checkIncommingEventsNum(Class msgClass) throws MalformedObjectNameException {
        checkEventsNum(0, grid(1), msgClass, false);
    }

    /**
     * Compares sent events number with histogram entries number.
     * Fails if these numbers differ.
     */
    private void checkEventsNum(int sourceIdx, IgniteEx target, Class msgClass, boolean outcomming) throws MalformedObjectNameException {
        RecordingSpi spi = (RecordingSpi)grid(sourceIdx).configuration().getCommunicationSpi();

        HistogramMetric metric = getMetric(sourceIdx, target, msgClass, outcomming);
        assertNotNull("HistogramMetric not found", metric);

        String metricName = metricName(target.localNode().id(), msgClass);

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

        Map<Long, Long> map = Collections.synchronizedMap(new TcpCommunicationMetricsListener.TimestampMap());

        map.put(10L, System.nanoTime());
        map.putIfAbsent(20L, System.nanoTime());

        Thread.sleep(2000);

        assertTrue("Unexpected map size before eviction: " + map.size(), map.size() == 2);

        map.putIfAbsent(30L, System.nanoTime());

        assertTrue("Unexpected map size after eviction: " + map.size(), map.size() == 1);
    }

    /**
     * Tests metrics disabling
     */
    @Test
    public void testDisabledMetric() throws MalformedObjectNameException {
        System.setProperty(IGNITE_ENABLE_MESSAGES_TIME_LOGGING, "not boolean value");

        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        cache.put(1, 1);

        HistogramMetric metric = getMetric(0, grid(1), GridDhtTxPrepareRequest.class, true);

        assertNull("Metrics unexpectedly enabled", metric);
    }

    /**
     * @throws Exception if failed to start grid.
     */
    @Test
    public void testMetricBounds() throws Exception {
        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        cache.put(1, 1);

        HistogramMetric metric = getMetric(0, grid(1), GridDhtTxPrepareRequest.class, true);

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


    /**
     * @param sourceNodeIdx Index of node that stores metric.
     * @param targetNode Node where requests are sent.
     * @param msgClass Metric request class.
     * @return {@code HistogramMetric} for {@code msgClass}.
     */
    @Nullable private HistogramMetric getMetric(int sourceNodeIdx, IgniteEx targetNode, Class msgClass, boolean outcomming) throws MalformedObjectNameException {
        try {
            TcpCommunicationSpiMBean mbean = mbean(sourceNodeIdx);

            Map<UUID, Map<Class<? extends Message>, HistogramMetric>> nodeMap = outcomming ? mbean.getOutMetricsByNodeByMsgClass() : mbean.getInMetricsByNodeByMsgClass();

            Map<Class<? extends Message>, HistogramMetric> classMap = nodeMap.get(targetNode.localNode().id());

            return classMap.get(msgClass);
        } catch (NullPointerException e) {
            return null;
        }
    }

    /**
     * Gets TcpCommunicationSpiMBean for given node.
     *
     * @param nodeIdx Node index.
     * @return MBean instance.
     */
    private TcpCommunicationSpiMBean mbean(int nodeIdx) throws MalformedObjectNameException {
        ObjectName mbeanName = U.makeMBeanName(getTestIgniteInstanceName(nodeIdx), "SPIs",
            RecordingSpi.class.getSimpleName());

        MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();

        if (mbeanServer.isRegistered(mbeanName))
            return MBeanServerInvocationHandler.newProxyInstance(mbeanServer, mbeanName, TcpCommunicationSpiMBean.class,
                true);
        else
            fail("MBean is not registered: " + mbeanName.getCanonicalName());

        return null;
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

                classesMap.merge(metricName(node.id(), msg0.getClass()), 1, Integer::sum);
            }
        }
    }
}
