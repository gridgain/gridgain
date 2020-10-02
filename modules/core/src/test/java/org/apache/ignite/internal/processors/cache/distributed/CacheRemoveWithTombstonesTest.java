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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheGroupIdMessage;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.metric.impl.MetricUtils;
import org.apache.ignite.internal.util.lang.GridIterator;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.ASYNC;

/**
 *
 */
@RunWith(Parameterized.class)
public class CacheRemoveWithTombstonesTest extends GridCommonAbstractTest {
    /** Test parameters. */
    @Parameterized.Parameters(name = "persistenceEnabled={0}, historicalRebalance={1}")
    public static Collection parameters() {
        List<Object[]> res = new ArrayList<>();

        res.add(new Object[]{false, false});

//        for (boolean persistenceEnabled : new boolean[] {false, true}) {
//            for (boolean histRebalance : new boolean[] {false, true}) {
//                if (!persistenceEnabled && histRebalance)
//                    continue;
//
//                res.add(new Object[]{persistenceEnabled, histRebalance});
//            }
//        }

        return res;
    }

    /** */
    @Parameterized.Parameter(0)
    public boolean persistence;

    /** */
    @Parameterized.Parameter(1)
    public boolean histRebalance;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setFailureDetectionTimeout(100000);
        cfg.setClientFailureDetectionTimeout(100000);

        TestRecordingCommunicationSpi commSpi = new TestRecordingCommunicationSpi();

        cfg.setConsistentId(gridName);

        cfg.setCommunicationSpi(commSpi);

        if (persistence) {
            DataStorageConfiguration dsCfg = new DataStorageConfiguration().setWalSegmentSize(4 * 1024 * 1024)
                    .setDefaultDataRegionConfiguration(
                            new DataRegionConfiguration()
                                .setInitialSize(256L * 1024 * 1024)
                                .setMaxSize(256L * 1024 * 1024)
                                .setPersistenceEnabled(true)
                    )
                    .setWalMode(WALMode.LOG_ONLY);

            cfg.setDataStorageConfiguration(dsCfg);
        }

        return cfg;
    }

    /**
     *
     */
    @Before
    public void before() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        if (histRebalance)
            System.setProperty(IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD, "0");
    }

    /**
     *
     */
    @After
    public void after() throws Exception {
        if (histRebalance)
            System.clearProperty(IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD);

        stopAllGrids();

        cleanPersistenceDir();
    }

    @Test
    public void testSimple() throws Exception {
        IgniteEx crd = startGrids(3);

        IgniteCache<Object, Object> cache0 = crd.createCache(cacheConfiguration(TRANSACTIONAL));

        final int part = 0;
        List<Integer> keys = loadDataToPartition(part, crd.name(), DEFAULT_CACHE_NAME, 100, 0);

        assertEquals(100, cache0.size());

        for (Integer key : keys)
            cache0.remove(key);

        final LongMetric tombstoneMetric0 = crd.context().metric().registry(
            MetricUtils.cacheGroupMetricsRegistryName(DEFAULT_CACHE_NAME)).findMetric("Tombstones");

        long value = tombstoneMetric0.value();

        assertEquals(100, value);
    }

    @Test
    public void testIterator() throws Exception {
        IgniteEx crd = startGrids(3);

        IgniteCache<Object, Object> cache0 = crd.createCache(cacheConfiguration(ATOMIC));

        final int part = 0;
        final int cnt = 100;

        List<Integer> keys = loadDataToPartition(part, crd.name(), DEFAULT_CACHE_NAME, cnt, 0);

        assertEquals(cnt, cache0.size());

        List<Integer> tsKeys = new ArrayList<>();

        int i = 0;
        for (Integer key : keys) {
            if (i++ % 2 == 0) {
                tsKeys.add(key);

                cache0.remove(key);
            }
        }

        final LongMetric tombstoneMetric0 = crd.context().metric().registry(
            MetricUtils.cacheGroupMetricsRegistryName(DEFAULT_CACHE_NAME)).findMetric("Tombstones");

        assertEquals(cnt/2, tombstoneMetric0.value());

        CacheGroupContext grp = crd.cachex(DEFAULT_CACHE_NAME).context().group();

        List<CacheDataRow> dataRows = new ArrayList<>();
        grp.offheap().partitionIterator(part, IgniteCacheOffheapManager.DATA).forEach(dataRows::add);

        List<CacheDataRow> tsRows = new ArrayList<>();
        grp.offheap().partitionIterator(part, IgniteCacheOffheapManager.TOMBSTONES).forEach(tsRows::add);

        assertNull(crd.cache(DEFAULT_CACHE_NAME).get(tsKeys.get(0)));

        crd.cache(DEFAULT_CACHE_NAME).put(tsKeys.get(0), 0);

        assertEquals(cnt/2 - 1, tombstoneMetric0.value());

        List<CacheDataRow> dataRows0 = new ArrayList<>();
        grp.offheap().partitionIterator(part, IgniteCacheOffheapManager.DATA).forEach(dataRows0::add);

        List<CacheDataRow> tsRows0 = new ArrayList<>();
        grp.offheap().partitionIterator(part, IgniteCacheOffheapManager.TOMBSTONES).forEach(tsRows0::add);

        GridDhtLocalPartition part0 = grp.topology().localPartition(part);

        part0.clearTombstonesAsync().get();

        List<CacheDataRow> dataRows1 = new ArrayList<>();
        grp.offheap().partitionIterator(part, IgniteCacheOffheapManager.DATA).forEach(dataRows1::add);

        List<CacheDataRow> tsRows1 = new ArrayList<>();
        grp.offheap().partitionIterator(part, IgniteCacheOffheapManager.TOMBSTONES).forEach(tsRows1::add);

        assertEquals(0, tombstoneMetric0.value());
    }

    @Test
    public void testRemoveValueUsingInvoke() {
        // TODO
    }

    @Test
    public void testPartitionHavingTombstonesIsRented() {
        // TODO Data and tombstones must be cleared.
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRemoveAndRebalanceRaceTx() throws Exception {
        testRemoveAndRebalanceRace(TRANSACTIONAL, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRemoveAndRebalanceRaceAtomic() throws Exception {
        testRemoveAndRebalanceRace(ATOMIC, false);
    }

    /**
     * @throws Exception If failed.
     * @param expTombstone {@code True} if tombstones should be created.
     */
    private void testRemoveAndRebalanceRace(CacheAtomicityMode atomicityMode, boolean expTombstone) throws Exception {
        IgniteEx ignite0 = startGrid(0);

        if (histRebalance)
            startGrid(1);

        if (persistence)
            ignite0.cluster().active(true);

        IgniteCache<Object, Object> cache0 = ignite0.createCache(cacheConfiguration(atomicityMode));

        final int KEYS = histRebalance ? 1024 : 1024 * 256;

        if (histRebalance) {
            // Preload initial data to have start point for WAL rebalance.
            try (IgniteDataStreamer<Object, Object> streamer = ignite0.dataStreamer(DEFAULT_CACHE_NAME)) {
                streamer.allowOverwrite(true);

                for (int i = 0; i < KEYS; i++)
                    streamer.addData(-i, 0);
            }

            forceCheckpoint();

            stopGrid(1);
        }

        // This data will be rebalanced.
        try (IgniteDataStreamer<Object, Object> streamer = ignite0.dataStreamer(DEFAULT_CACHE_NAME)) {
            streamer.allowOverwrite(true);

            for (int i = 0; i < KEYS; i++)
                streamer.addData(i, i);
        }

        blockRebalance(ignite0);

        IgniteEx ignite1 = GridTestUtils.runAsync(() -> startGrid(1)).get(10, TimeUnit.SECONDS);

        if (persistence) {
            ignite0.cluster().baselineAutoAdjustEnabled(false);

            ignite0.cluster().setBaselineTopology(2);
        }

        TestRecordingCommunicationSpi.spi(ignite0).waitForBlocked();

        Set<Integer> keysWithTombstone = new HashSet<>();

        // Do removes while rebalance is in progress.
        // All keys are removed during historical rebalance.
        for (int i = 0, step = histRebalance ? 1 : 64; i < KEYS; i += step) {
            keysWithTombstone.add(i);

            cache0.remove(i);
        }

        final LongMetric tombstoneMetric0 = ignite0.context().metric().registry(
            MetricUtils.cacheGroupMetricsRegistryName(DEFAULT_CACHE_NAME)).findMetric("Tombstones");

        final LongMetric tombstoneMetric1 = ignite1.context().metric().registry(
            MetricUtils.cacheGroupMetricsRegistryName(DEFAULT_CACHE_NAME)).findMetric("Tombstones");

        // On first node there should not be tombstones.
        assertEquals(0, tombstoneMetric0.value());

        if (expTombstone)
            assertEquals(keysWithTombstone.size(), tombstoneMetric1.value());
        else
            assertEquals(0, tombstoneMetric1.value());

        // Update some of removed keys, this should remove tombstones.
        for (int i = 0; i < KEYS; i += 128) {
            keysWithTombstone.remove(i);

            cache0.put(i, i);
        }

        assertTrue("Keys with tombstones should exist", !keysWithTombstone.isEmpty());

        assertEquals(0, tombstoneMetric0.value());

        if (expTombstone)
            assertEquals(keysWithTombstone.size(), tombstoneMetric1.value());
        else
            assertEquals(0, tombstoneMetric1.value());

        TestRecordingCommunicationSpi.spi(ignite0).stopBlock();

        awaitPartitionMapExchange();

        IgniteCache<Integer, Integer> cache1 = ignite(1).cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < KEYS; i++) {
            if (keysWithTombstone.contains(i))
                assertNull(cache1.get(i));
            else
                assertEquals((Object)i, cache1.get(i));
        }

        // Tombstones should be removed after once rebalance is completed.
        GridTestUtils.waitForCondition(() -> tombstoneMetric1.value() == 0, 30_000);

        assertEquals(0, tombstoneMetric1.value());
    }

    /**
     *
     */
    private static void blockRebalance(IgniteEx node) {
        final int grpId = groupIdForCache(node, DEFAULT_CACHE_NAME);

        TestRecordingCommunicationSpi.spi(node).blockMessages((node0, msg) ->
            (msg instanceof GridDhtPartitionSupplyMessage)
            && ((GridCacheGroupIdMessage)msg).groupId() == grpId
        );
    }

    /**
     * @param atomicityMode Cache atomicity mode.
     * @return Cache configuration.
     */
    private CacheConfiguration<Object, Object> cacheConfiguration(CacheAtomicityMode atomicityMode) {
        return new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAtomicityMode(atomicityMode)
            .setCacheMode(PARTITIONED)
            .setBackups(2)
            .setRebalanceMode(ASYNC)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setAffinity(new RendezvousAffinityFunction(false, 64));
    }
}
