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

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.impl.ObjectGauge;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.metric.IntMetric;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_REBALANCE_STATISTICS_TIME_INTERVAL;
import static org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction.DFLT_PARTITION_COUNT;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.cacheGroupMetricsRegistryName;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 *
 */
public class CacheGroupsMetricsRebalanceTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE1 = "cache1";

    /** */
    private static final String CACHE2 = "cache2";

    /** */
    private static final String CACHE3 = "cache3";

    /** */
    private static final String CACHE4 = "cache4";

    /** */
    private static final String CACHE5 = "cache5";

    /** */
    private static final long REBALANCE_DELAY = 5_000;

    /** */
    private static final String GROUP = "group1";

    /** */
    private static final String GROUP2 = "group2";

    /** */
    private static final int KEYS_COUNT = 10_000;

    /** Acceptable time inaccuracy for testRebalanceEstimateFinishTime() */
    public static final long ACCEPTABLE_TIME_INACCURACY = 25_000L;

    /** */
    private long rebalanceDelay = 0;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 10 * 60 * 1000;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration cfg1 = new CacheConfiguration()
            .setName(CACHE1)
            .setGroupName(GROUP)
            .setCacheMode(CacheMode.PARTITIONED)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setRebalanceMode(CacheRebalanceMode.ASYNC)
            .setRebalanceBatchSize(100)
            .setStatisticsEnabled(true);

        CacheConfiguration cfg2 = new CacheConfiguration(cfg1)
            .setName(CACHE2);

        CacheConfiguration cfg3 = new CacheConfiguration()
            .setName(CACHE3)
            .setCacheMode(CacheMode.PARTITIONED)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setRebalanceMode(CacheRebalanceMode.ASYNC)
            .setRebalanceBatchSize(100)
            .setStatisticsEnabled(true)
            .setRebalanceDelay(rebalanceDelay);

        CacheConfiguration cfg4 = new CacheConfiguration()
            .setAffinity(new RendezvousAffinityFunction())
            .setRebalanceMode(CacheRebalanceMode.ASYNC)
            .setName(CACHE4)
            .setCacheMode(CacheMode.REPLICATED)
            .setGroupName(GROUP2);

        CacheConfiguration cfg5 = new CacheConfiguration(cfg4)
            .setName(CACHE5);

        cfg.setCacheConfiguration(cfg1, cfg2, cfg3, cfg4, cfg5);

        cfg.setIncludeEventTypes(EventType.EVTS_ALL);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        rebalanceDelay = 0;
    }

    /**
     * Checks the correctness of {@link CacheMetrics#getRebalancingKeysRate}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRebalance() throws Exception {
        Ignite ignite = startGrid(0);

        IgniteCache<Object, Object> cache1 = ignite.cache(CACHE1);
        IgniteCache<Object, Object> cache2 = ignite.cache(CACHE2);

        for (int i = 0; i < KEYS_COUNT; i++) {
            cache1.put(i, CACHE1 + "-" + i);

            if (i % 2 == 0)
                cache2.put(i, CACHE2 + "-" + i);
        }

        ignite = startGrid(1);

        awaitPartitionMapExchange(true, true, null, true);

        CacheMetrics metrics1 = ignite.cache(CACHE1).localMetrics();
        CacheMetrics metrics2 = ignite.cache(CACHE2).localMetrics();

        long rate1 = metrics1.getRebalancingKeysRate();
        long rate2 = metrics2.getRebalancingKeysRate();

        assertTrue(rate1 > 0);
        assertTrue(rate2 > 0);
        assertTrue(rate1 > rate2);

        assertEquals(metrics1.getRebalancedKeys(), rate1);
        assertEquals(metrics2.getRebalancedKeys(), rate2);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCacheGroupRebalance() throws Exception {
        IgniteEx ignite0 = startGrid(0);

        List<String> cacheNames = Arrays.asList(CACHE4, CACHE5);

        int allKeysCount = 0;

        for (String cacheName : cacheNames) {
            Map<Integer, Long> data = new Random().ints(KEYS_COUNT).distinct().boxed()
                .collect(Collectors.toMap(i -> i, i -> (long)i));

            ignite0.getOrCreateCache(cacheName).putAll(data);

            allKeysCount += data.size();
        }

        TestRecordingCommunicationSpi.spi(ignite0)
            .blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
                @Override public boolean apply(ClusterNode node, Message msg) {
                    return (msg instanceof GridDhtPartitionSupplyMessage) &&
                        CU.cacheId(GROUP2) == ((GridCacheGroupIdMessage)msg).groupId();
                }
            });

        IgniteEx ignite1 = startGrid(1);

        TestRecordingCommunicationSpi.spi(ignite0).waitForBlocked();

        MetricRegistry mreg = ignite1.context().metric()
            .registry(cacheGroupMetricsRegistryName(GROUP2));

        LongMetric startTime = mreg.findMetric("RebalancingStartTime");
        LongMetric lastCancelledTime = mreg.findMetric("RebalancingLastCancelledTime");
        LongMetric endTime = mreg.findMetric("RebalancingEndTime");
        LongMetric partitionsLeft = mreg.findMetric("RebalancingPartitionsLeft");
        IntMetric partitionsTotal = mreg.findMetric("RebalancingPartitionsTotal");
        LongMetric receivedKeys = mreg.findMetric("RebalancingReceivedKeys");
        LongMetric receivedBytes = mreg.findMetric("RebalancingReceivedBytes");

        ObjectGauge<Map<UUID, Long>> fullReceivedKeys = mreg.findMetric("RebalancingFullReceivedKeys");
        ObjectGauge<Map<UUID, Long>> histReceivedKeys = mreg.findMetric("RebalancingHistReceivedKeys");
        ObjectGauge<Map<UUID, Long>> fullReceivedBytes = mreg.findMetric("RebalancingFullReceivedBytes");
        ObjectGauge<Map<UUID, Long>> histReceivedBytes = mreg.findMetric("RebalancingHistReceivedBytes");

        assertEquals("During the start of the rebalancing, the number of partitions in the metric should be " +
                "equal to the number of partitions in the cache group.", DFLT_PARTITION_COUNT, partitionsLeft.value());

        assertEquals("The total number of partitions in the metric should be " +
                "equal to the number of partitions in the cache group.", DFLT_PARTITION_COUNT, partitionsTotal.value());

        long rebalancingStartTime = startTime.value();

        assertNotSame("During rebalancing start, the start time metric must be determined.",
            -1, startTime.value());

        assertEquals("Rebalancing last cancelled time must be undefined.", -1, lastCancelledTime.value());

        assertEquals("Before the rebalancing is completed, the end time metric must be undefined.",
            -1, endTime.value());

        ToLongFunction<Map<UUID, Long>> sumFunc = map -> map.values().stream().mapToLong(Long::longValue).sum();

        String zeroReceivedKeysMsg = "Until a partition supply message has been delivered, keys cannot be received.";
        assertEquals(zeroReceivedKeysMsg, 0, receivedKeys.value());
        assertEquals(zeroReceivedKeysMsg, 0, sumFunc.applyAsLong(fullReceivedKeys.value()));
        assertEquals(zeroReceivedKeysMsg, 0, sumFunc.applyAsLong(histReceivedKeys.value()));

        String zeroReceivedBytesMsg = "Until a partition supply message has been delivered, bytes cannot be received.";
        assertEquals(zeroReceivedBytesMsg, 0, receivedBytes.value());
        assertEquals(zeroReceivedBytesMsg, 0, sumFunc.applyAsLong(fullReceivedBytes.value()));
        assertEquals(zeroReceivedBytesMsg, 0, sumFunc.applyAsLong(histReceivedBytes.value()));

        checkSuppliers(
            Arrays.asList(ignite0.localNode().id()),
            fullReceivedKeys, histReceivedKeys, fullReceivedBytes, histReceivedBytes
        );

        TestRecordingCommunicationSpi.spi(ignite0).stopBlock();

        for (String cacheName : cacheNames)
            ignite1.context().cache().internalCache(cacheName).preloader().rebalanceFuture().get();

        assertEquals("After completion of rebalancing, there are no partitions of the cache group that are" +
            " left to rebalance.", 0, partitionsLeft.value());

        assertEquals("After completion of rebalancing, the total number of partitions in the metric should be" +
            " equal to the number of partitions in the cache group.", DFLT_PARTITION_COUNT, partitionsTotal.value());

        assertEquals("After the rebalancing is ended, the rebalancing start time must be equal to the start time " +
                "measured immediately after the rebalancing start.", rebalancingStartTime, startTime.value());

        assertEquals("Rebalancing last cancelled time must be undefined.", -1, lastCancelledTime.value());

        waitForCondition(() -> endTime.value() != -1, 1000);

        assertTrue("Rebalancing end time must be determined and must be longer than the start time " +
                "[RebalancingStartTime=" + rebalancingStartTime + ", RebalancingEndTime=" + endTime.value() + "].",
            rebalancingStartTime < endTime.value());

        String wrongReceivedKeyCntMsg = "The number of currently rebalanced keys for the whole cache group should " +
            "be equal to the number of entries in the caches.";
        assertEquals(wrongReceivedKeyCntMsg, allKeysCount, receivedKeys.value());
        assertEquals(wrongReceivedKeyCntMsg, allKeysCount, sumFunc.applyAsLong(fullReceivedKeys.value()));
        assertEquals(0, sumFunc.applyAsLong(histReceivedKeys.value()));

        int estimateByteCnt = allKeysCount * (Integer.BYTES + Long.BYTES);

        String wrongReceivedByteCntMsg = "The number of currently rebalanced bytes of this cache group was expected " +
            "more " + estimateByteCnt + " bytes.";
        assertTrue(wrongReceivedByteCntMsg, receivedBytes.value() > estimateByteCnt);
        assertTrue(wrongReceivedByteCntMsg, sumFunc.applyAsLong(fullReceivedBytes.value()) > estimateByteCnt);
        assertEquals(0, sumFunc.applyAsLong(histReceivedBytes.value()));

        checkSuppliers(
            Arrays.asList(ignite0.localNode().id()),
            fullReceivedKeys, histReceivedKeys, fullReceivedBytes, histReceivedBytes
        );
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRebalancingLastCancelledTime() throws Exception {
        rebalanceDelay = REBALANCE_DELAY; // Used for trigger rebalance cancellation.

        IgniteEx ignite0 = startGrid(0);

        List<String> cacheNames = Arrays.asList(CACHE4, CACHE5);

        for (String cacheName : cacheNames) {
            ignite0.getOrCreateCache(cacheName).putAll(new Random().ints(KEYS_COUNT).distinct().boxed()
                .collect(Collectors.toMap(i -> i, i -> (long)i)));
        }

        TestRecordingCommunicationSpi.spi(ignite0)
            .blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
                @Override public boolean apply(ClusterNode node, Message msg) {
                    return (msg instanceof GridDhtPartitionSupplyMessage) &&
                        ((GridCacheGroupIdMessage)msg).groupId() == CU.cacheId(GROUP2);
                }
            });

        IgniteEx ignite1 = startGrid(1);

        TestRecordingCommunicationSpi.spi(ignite0).waitForBlocked();

        MetricRegistry mreg = ignite1.context().metric().registry(cacheGroupMetricsRegistryName(GROUP2));

        LongMetric startTime = mreg.findMetric("RebalancingStartTime");
        LongMetric lastCancelledTime = mreg.findMetric("RebalancingLastCancelledTime");
        LongMetric endTime = mreg.findMetric("RebalancingEndTime");
        LongMetric partitionsLeft = mreg.findMetric("RebalancingPartitionsLeft");
        IntMetric partitionsTotal = mreg.findMetric("RebalancingPartitionsTotal");

        assertEquals("During the start of the rebalancing, the number of partitions in the metric should be " +
            "equal to the number of partitions in the cache group.", DFLT_PARTITION_COUNT, partitionsLeft.value());

        assertEquals("The total number of partitions in the metric should be " +
            "equal to the number of partitions in the cache group.", DFLT_PARTITION_COUNT, partitionsTotal.value());

        long rebalancingStartTime = startTime.value();

        assertNotSame("During rebalancing start, the start time metric must be determined.",
            -1, startTime.value());

        assertEquals("Rebalancing last cancelled time must be undefined.", -1, lastCancelledTime.value());

        assertEquals("Before the rebalancing is completed, the end time metric must be undefined.",
            -1, endTime.value());

        IgniteInternalFuture chain = ignite1.context().cache().internalCache(CACHE5).preloader().rebalanceFuture()
            .chain(f -> {
                assertEquals("After the rebalancing is ended, the rebalancing start time must be equal to " +
                        "the start time measured immediately after the rebalancing start.",
                    rebalancingStartTime, startTime.value());

                assertEquals("If the rebalancing has been cancelled, the end time must not be set.",
                    -1, endTime.value());

                return null;
            });

        TestRecordingCommunicationSpi.spi(ignite0).stopBlock(false);

        chain.get();

        assertNotSame("The rebalancing start time must not be equal to the previously measured start time, since" +
                " the first rebalancing was cancelled and restarted.", rebalancingStartTime, startTime.value());

        waitForCondition(() -> lastCancelledTime.value() != -1, 5000);

        assertTrue("The rebalancing last cancelled time must be greater than or equal to the start time of the " +
            "cancelled rebalancing [RebalancingStartTime=" + rebalancingStartTime + ", rebalancingLastCancelledTime=" +
            lastCancelledTime.value() + "].", rebalancingStartTime <= lastCancelledTime.value());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRebalanceProgressUnderLoad() throws Exception {
        Ignite ignite = startGrids(4);

        IgniteCache<Object, Object> cache1 = ignite.cache(CACHE1);

        Random r = new Random();

        GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
                for (int i = 0; i < 100_000; i++) {
                    int next = r.nextInt();

                    cache1.put(next, CACHE1 + "-" + next);
                }
            }
        });

        IgniteEx ig = startGrid(4);

        GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
                for (int i = 0; i < 100_000; i++) {
                    int next = r.nextInt();

                    cache1.put(next, CACHE1 + "-" + next);
                }
            }
        });

        CountDownLatch latch = new CountDownLatch(1);

        ig.events().localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                latch.countDown();

                return false;
            }
        }, EventType.EVT_CACHE_REBALANCE_STOPPED);

        latch.await();

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                CacheMetrics snapshot = ig.cache(CACHE1).metrics();

                return snapshot.getRebalancedKeys() > snapshot.getEstimatedRebalancingKeys()
                    && snapshot.getRebalancingPartitionsCount() == 0;
            }
        }, 5000);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRebalanceEstimateFinishTime() throws Exception {
        System.setProperty(IGNITE_REBALANCE_STATISTICS_TIME_INTERVAL, String.valueOf(10_000));

        Ignite ig1 = startGrid(1);

        final int KEYS = GridTestUtils.SF.applyLB(1_000_000, 300_000);

        try (IgniteDataStreamer<Integer, String> st = ig1.dataStreamer(CACHE1)) {
            for (int i = 0; i < KEYS; i++)
                st.addData(i, CACHE1 + "-" + i);
        }

        final Ignite ig2 = startGrid(2);

        boolean rebalancingStartTimeGot = waitForCondition(() -> ig2.cache(CACHE1).localMetrics().getRebalancingStartTime() != -1L, 5_000);

        assertTrue("Unable to resolve rebalancing start time.", rebalancingStartTimeGot);

        CacheMetrics metrics = ig2.cache(CACHE1).localMetrics();

        long startTime = metrics.getRebalancingStartTime();
        long currTime = U.currentTimeMillis();

        assertTrue("Invalid start time [startTime=" + startTime + ", currTime=" + currTime + ']',
            startTime > 0L && (currTime - startTime) >= 0L && (currTime - startTime) <= 5000L);

        final CountDownLatch latch = new CountDownLatch(1);

        runAsync(() -> {
            // Waiting 75% keys will be rebalanced.
            int partKeys = KEYS / 2;

            final long keysLine = (long)partKeys / 4L;

            log.info("Wait until keys left will be less than: " + keysLine);

            while (true) {
                CacheMetrics m = ig2.cache(CACHE1).localMetrics();

                long keyLeft = m.getKeysToRebalanceLeft();

                if (keyLeft < keysLine) {
                    latch.countDown();

                    break;
                }

                log.info("Keys left: " + m.getKeysToRebalanceLeft());

                try {
                    Thread.sleep(1_000);
                }
                catch (InterruptedException e) {
                    log.warning("Interrupt thread", e);

                    Thread.currentThread().interrupt();
                }
            }
        });

        assertTrue(latch.await(getTestTimeout(), TimeUnit.MILLISECONDS));

        boolean estimatedRebalancingFinishTimeGot = waitForCondition(new PA() {
            @Override public boolean apply() {
                return ig2.cache(CACHE1).localMetrics().getEstimatedRebalancingFinishTime() != -1L;
            }
        }, 5_000L);

        assertTrue("Unable to resolve estimated rebalancing finish time.", estimatedRebalancingFinishTimeGot);

        long finishTime = ig2.cache(CACHE1).localMetrics().getEstimatedRebalancingFinishTime();

        assertTrue("Not a positive estimation of rebalancing finish time: " + finishTime,
            finishTime > 0L);

        currTime = U.currentTimeMillis();

        long timePassed = currTime - startTime;
        long timeLeft = finishTime - currTime;

        // TODO: finishRebalanceLatch gets countdown much earlier because of ForceRebalanceExchangeTask triggered by cache with delay
//        assertTrue("Got timeout while waiting for rebalancing. Estimated left time: " + timeLeft,
//            finishRebalanceLatch.await(timeLeft + 10_000L, TimeUnit.MILLISECONDS));

        boolean allKeysRebalanced = waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return ig2.cache(CACHE1).localMetrics().getKeysToRebalanceLeft() == 0;
            }
        }, timeLeft + ACCEPTABLE_TIME_INACCURACY);

        assertTrue("Some keys aren't rebalanced.", allKeysRebalanced);

        log.info("[timePassed=" + timePassed + ", timeLeft=" + timeLeft +
                ", Time to rebalance=" + (finishTime - startTime) +
                ", startTime=" + startTime + ", finishTime=" + finishTime + ']'
        );

        System.clearProperty(IGNITE_REBALANCE_STATISTICS_TIME_INTERVAL);

        currTime = U.currentTimeMillis();

        log.info("Rebalance time: " + (currTime - startTime));

        long diff = finishTime - currTime;

        assertTrue("Expected less than " + ACCEPTABLE_TIME_INACCURACY + ", but actual: " + diff,
            Math.abs(diff) < ACCEPTABLE_TIME_INACCURACY);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRebalanceDelay() throws Exception {
        rebalanceDelay = REBALANCE_DELAY;

        Ignite ig1 = startGrid(1);

        CacheConfiguration cfg3 = new CacheConfiguration()
            .setName(CACHE3)
            .setCacheMode(CacheMode.PARTITIONED)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setRebalanceMode(CacheRebalanceMode.ASYNC)
            .setRebalanceBatchSize(100)
            .setStatisticsEnabled(true)
            .setRebalanceDelay(REBALANCE_DELAY);

        final IgniteCache<Object, Object> cache = ig1.getOrCreateCache(cfg3);

        for (int i = 0; i < KEYS_COUNT; i++)
            cache.put(i, CACHE3 + "-" + i);

        long beforeStartTime = U.currentTimeMillis();

        startGrid(2);
        startGrid(3);

        waitForCondition(new PA() {
            @Override public boolean apply() {
                return cache.localMetrics().getRebalancingStartTime() != -1L;
            }
        }, 5_000);

        assert (cache.localMetrics().getRebalancingStartTime() < U.currentTimeMillis() + REBALANCE_DELAY);
        assert (cache.localMetrics().getRebalancingStartTime() > beforeStartTime + REBALANCE_DELAY);
    }

    /**
     * Check suppliers in metrics.
     *
     * @param uuids Suppliers.
     * @param gauges Metrics per supplier.
     */
    private void checkSuppliers(Collection<UUID> uuids, ObjectGauge<Map<UUID, Long>>... gauges) {
        A.notEmpty(gauges, "gauges");
        A.notEmpty(uuids, "uuids");

        for (int i = 0; i < gauges.length; i++) {
            Map<UUID, Long> val = gauges[i].value();

            assertEquals("i=" + i, uuids.size(), val.size());

            int fi = i;
            uuids.forEach(uuid -> assertTrue(String.format("i=%s uuid=%s", fi, uuid), val.containsKey(uuid)));
        }
    }
}
