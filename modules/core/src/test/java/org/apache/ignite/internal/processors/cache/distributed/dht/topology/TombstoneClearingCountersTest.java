/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.cache.distributed.dht.topology;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.BooleanSupplier;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemander;
import org.apache.ignite.internal.processors.cache.tree.PendingEntriesTree;
import org.apache.ignite.internal.processors.resource.DependencyResolver;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;

/**
 * Tests fast full rebalancing optimization correctness.
 */
@RunWith(Parameterized.class)
public class TombstoneClearingCountersTest extends GridCommonAbstractTest {
    /** */
    @Parameterized.Parameter(value = 0)
    public CacheAtomicityMode atomicityMode;

    /**
     * @return List of test parameters.
     */
    @Parameterized.Parameters(name = "mode={0}")
    public static List<Object[]> parameters() {
        ArrayList<Object[]> params = new ArrayList<>();

        params.add(new Object[]{ATOMIC});
        params.add(new Object[]{TRANSACTIONAL});

        return params;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());
        cfg.setSystemThreadPoolSize(1); // Avoid assignState parallelization for easier debugging.

        cfg.setRebalanceThreadPoolSize(1);
        cfg.setConsistentId(igniteInstanceName);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration().
            setWalSegmentSize(1024 * 1024).setMaxWalArchiveSize(4 * 1024 * 1024).setWalSegments(2);
        dsCfg.getDefaultDataRegionConfiguration().setPersistenceEnabled(true).setMaxSize(200 * 1024 * 1024);
        cfg.setDataStorageConfiguration(dsCfg);

        cfg.setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME).
            setAtomicityMode(atomicityMode).
            setBackups(1).
            setAffinity(new RendezvousAffinityFunction(false, 64)));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConsistencyOnCounterTriggeredRebalance() throws Exception {
        IgniteEx crd = startGrids(2);
        crd.cluster().state(ClusterState.ACTIVE);

        int testPart = 0;

        IgniteCache<Object, Object> cache = crd.cache(DEFAULT_CACHE_NAME);

        cache.put(testPart, 0);

        forceCheckpoint();

        stopGrid(1);
        awaitPartitionMapExchange();

        crd.cache(DEFAULT_CACHE_NAME).remove(testPart);

        TrackingResolver rslvr = new TrackingResolver(testPart);
        IgniteEx g2 = startGrid(1, rslvr);

        awaitPartitionMapExchange();

        assertTrue(historical(1).isEmpty());
        assertNull(rslvr.reason); // Expecting fast full rebalancing.

        assertPartitionsSame(idleVerify(crd, DEFAULT_CACHE_NAME));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = "DEFAULT_TOMBSTONE_TTL", value = "1000")
    @WithSystemProperty(key = "CLEANUP_WORKER_SLEEP_INTERVAL", value = "100000000") // Disable background cleanup.
    @WithSystemProperty(key = "IGNITE_UNWIND_THROTTLING_TIMEOUT", value = "0") // Disable unwind throttling.
    public void testConsistencyOnCounterTriggeredRebalanceCleanupNotFullBaseline() throws Exception {
        int id = CU.cacheId("cache_group_173");

        IgniteEx crd = startGrids(2);
        crd.cluster().state(ClusterState.ACTIVE);

        int testPart = 0;

        IgniteCache<Object, Object> cache = crd.cache(DEFAULT_CACHE_NAME);

        cache.put(testPart, 0);

        stopGrid(1);
        awaitPartitionMapExchange();

        crd.cache(DEFAULT_CACHE_NAME).remove(testPart);

        doSleep(1100);

        // Tombstone shouldn't be removed if not full baseline.
        CU.unwindEvicts(crd.cachex(DEFAULT_CACHE_NAME).context());

        TrackingResolver rslvr = new TrackingResolver(testPart);
        IgniteEx g2 = startGrid(1, rslvr);

        awaitPartitionMapExchange();

        assertTrue(historical(1).isEmpty());
        assertNull(rslvr.reason); // Expecting fast full rebalancing.

        assertPartitionsSame(idleVerify(crd, DEFAULT_CACHE_NAME));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConsistencyOnCounterTriggeredRebalanceClearTombstones() throws Exception {
        IgniteEx crd = startGrids(2);
        crd.cluster().state(ClusterState.ACTIVE);

        int testPart = 0;

        IgniteCache<Object, Object> cache = crd.cache(DEFAULT_CACHE_NAME);

        cache.put(testPart, 0);

        forceCheckpoint();

        stopGrid(1);
        awaitPartitionMapExchange();

        crd.cache(DEFAULT_CACHE_NAME).remove(testPart);

        // Expecting full clearing on grid1 so no desync happens.
        clearTombstones(cache);

        TrackingResolver rslvr = new TrackingResolver(testPart);
        IgniteEx g2 = startGrid(1, rslvr);

        awaitPartitionMapExchange();

        assertTrue(historical(1).isEmpty());
        assertTrue(rslvr.reason == PartitionsEvictManager.EvictReason.CLEARING); // Tombstones are cleared, need eviction.

        assertPartitionsSame(idleVerify(crd, DEFAULT_CACHE_NAME));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConsistencyOnCounterTriggeredRebalanceClearTombstonesNotCrd() throws Exception {
        IgniteEx crd = startGrids(3);
        crd.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Object, Object> crdCache = crd.cache(DEFAULT_CACHE_NAME);

        // Find a partition owned by non coordinator nodes.
        int testPart = nearKey(crdCache);

        crdCache.put(testPart, 0);

        forceCheckpoint();

        stopGrid(1);
        awaitPartitionMapExchange();

        crd.cache(DEFAULT_CACHE_NAME).remove(testPart);

        IgniteCache<Object, Object> g2Cache = grid(2).cache(DEFAULT_CACHE_NAME);

        // Expecting full clearing on grid1 (g2 will be a supplier) so no desync happens.
        clearTombstones(g2Cache);

        TrackingResolver rslvr = new TrackingResolver(testPart);
        IgniteEx g2 = startGrid(1, rslvr);

        awaitPartitionMapExchange();

        assertTrue(historical(1).isEmpty());
        assertTrue(rslvr.reason == PartitionsEvictManager.EvictReason.CLEARING); // Tombstones are cleared, need eviction.

        assertPartitionsSame(idleVerify(crd, DEFAULT_CACHE_NAME));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConsistencyOnCounterTriggeredRebalanceClearTombstones2Iter() throws Exception {
        IgniteEx crd = startGrids(2);
        crd.cluster().state(ClusterState.ACTIVE);

        int testPart = 0;

        IgniteCache<Object, Object> cache = crd.cache(DEFAULT_CACHE_NAME);

        cache.put(testPart, 0);

        forceCheckpoint();

        stopGrid(1);
        awaitPartitionMapExchange();

        crd.cache(DEFAULT_CACHE_NAME).remove(testPart);

        // Expecting full clearing on grid1 so no desync happens.
        clearTombstones(cache);

        TrackingResolver rslvr = new TrackingResolver(testPart);
        IgniteEx g2 = startGrid(1, rslvr);

        awaitPartitionMapExchange();

        assertTrue(historical(1).isEmpty());
        assertTrue(rslvr.reason == PartitionsEvictManager.EvictReason.CLEARING); // Tombstones are cleared, need eviction.

        assertPartitionsSame(idleVerify(crd, DEFAULT_CACHE_NAME));

        cache.put(testPart, 0);

        forceCheckpoint();

        stopGrid(1);
        awaitPartitionMapExchange();

        crd.cache(DEFAULT_CACHE_NAME).remove(testPart);

        rslvr = new TrackingResolver(testPart);
        startGrid(1, rslvr);
        awaitPartitionMapExchange();

        assertTrue(historical(1).isEmpty());
        assertNull(rslvr.reason); // Second iteration should do fast rebalancing.
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConsistencyOnCounterTriggeredRebalanceRestartDemander() throws Exception {
        IgniteEx crd = startGrids(2);
        crd.cluster().state(ClusterState.ACTIVE);

        int testPart = 0;

        IgniteCache<Object, Object> cache = crd.cache(DEFAULT_CACHE_NAME);

        cache.put(testPart, 0);

        forceCheckpoint();

        stopGrid(1);
        awaitPartitionMapExchange();

        crd.cache(DEFAULT_CACHE_NAME).remove(testPart);

        // Expecting full clearing on grid1 so no desync happens.
        clearTombstones(cache);

        IgniteConfiguration cfg1 = getConfiguration(getTestIgniteInstanceName(1));
        TestRecordingCommunicationSpi spi1 = (TestRecordingCommunicationSpi) cfg1.getCommunicationSpi();
        spi1.blockMessages(TestRecordingCommunicationSpi.blockDemandMessageForGroup(CU.cacheId(DEFAULT_CACHE_NAME)));

        IgniteEx g2 = startGrid(cfg1);

        spi1.waitForBlocked();
        g2.close();

        TrackingResolver rslvr = new TrackingResolver(testPart);
        startGrid(1, rslvr);

        awaitPartitionMapExchange();

        assertTrue(historical(1).isEmpty());
        assertTrue(rslvr.reason == PartitionsEvictManager.EvictReason.CLEARING);

        assertPartitionsSame(idleVerify(crd, DEFAULT_CACHE_NAME));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = "IGNITE_PREFER_WAL_REBALANCE", value = "true")
    public void testConsistencyOnCounterTriggeredRebalanceHistorical() throws Exception {
        IgniteEx crd = startGrids(2);
        crd.cluster().state(ClusterState.ACTIVE);

        int testPart = 0;

        IgniteCache<Object, Object> cache = crd.cache(DEFAULT_CACHE_NAME);

        cache.put(testPart, 0);

        forceCheckpoint();

        stopGrid(1);
        awaitPartitionMapExchange();

        crd.cache(DEFAULT_CACHE_NAME).remove(testPart);

        // We should not clear tombstones on supplier node, because cleared tombstone is not seen on
        // demander causing partition desync.

        // Start node and delay preloading in the middle of partition clearing.
        TrackingResolver rslvr = new TrackingResolver(testPart);
        IgniteEx g2 = startGrid(1, rslvr);

        awaitPartitionMapExchange();

        assertTrue(historical(1).contains(testPart));
        assertNull(rslvr.reason);

        assertPartitionsSame(idleVerify(crd, DEFAULT_CACHE_NAME));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = "IGNITE_PREFER_WAL_REBALANCE", value = "true")
    public void testConsistencyOnCounterTriggeredRebalanceHistoricalRestartDemander() throws Exception {
        IgniteEx crd = startGrids(2);
        crd.cluster().state(ClusterState.ACTIVE);

        int testPart = 0;

        IgniteCache<Object, Object> cache = crd.cache(DEFAULT_CACHE_NAME);

        cache.put(testPart, 0);

        forceCheckpoint();

        stopGrid(1);
        awaitPartitionMapExchange();

        crd.cache(DEFAULT_CACHE_NAME).remove(testPart);

        IgniteConfiguration cfg1 = getConfiguration(getTestIgniteInstanceName(1));
        TestRecordingCommunicationSpi spi1 = (TestRecordingCommunicationSpi) cfg1.getCommunicationSpi();
        spi1.blockMessages(TestRecordingCommunicationSpi.blockDemandMessageForGroup(CU.cacheId(DEFAULT_CACHE_NAME)));

        IgniteEx g2 = startGrid(cfg1);
        spi1.waitForBlocked();
        forceCheckpoint(g2); // Write adjusted counters to enforce full rebalancing after restart.
        g2.close(); // Stop before rebalancing for default is starting.

        TrackingResolver rslvr = new TrackingResolver(testPart);
        startGrid(1, rslvr);

        awaitPartitionMapExchange();

        // Expecting full rebalancing, because LWM on joining node is adjusted to supplier LWM during previous PME.
        assertFalse(historical(1).contains(testPart));
        assertNull(rslvr.reason);

        assertPartitionsSame(idleVerify(crd, DEFAULT_CACHE_NAME));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = "IGNITE_PREFER_WAL_REBALANCE", value = "true")
    public void testConsistencyOnCounterTriggeredRebalanceHistoricalRestartDemanderClearTombstones() throws Exception {
        IgniteEx crd = startGrids(2);
        crd.cluster().state(ClusterState.ACTIVE);

        int testPart = 0;

        IgniteCache<Object, Object> cache = crd.cache(DEFAULT_CACHE_NAME);

        cache.put(testPart, 0);

        forceCheckpoint();

        stopGrid(1);
        awaitPartitionMapExchange();

        crd.cache(DEFAULT_CACHE_NAME).remove(testPart);

        IgniteConfiguration cfg1 = getConfiguration(getTestIgniteInstanceName(1));
        TestRecordingCommunicationSpi spi1 = (TestRecordingCommunicationSpi) cfg1.getCommunicationSpi();
        spi1.blockMessages(TestRecordingCommunicationSpi.blockDemandMessageForGroup(CU.cacheId(DEFAULT_CACHE_NAME)));

        IgniteEx g2 = startGrid(cfg1);
        spi1.waitForBlocked();
        forceCheckpoint(g2); // Write adjusted counters to enforce full rebalancing after restart.
        g2.close(); // Stop before rebalancing for default is starting.

        clearTombstones(cache);

        TrackingResolver rslvr = new TrackingResolver(testPart);
        startGrid(1, rslvr);

        awaitPartitionMapExchange();

        // Expecting full rebalancing, because LWM on joining node is adjusted to supplier LWM during previous PME.
        assertFalse(historical(1).contains(testPart));
        assertTrue(rslvr.reason == PartitionsEvictManager.EvictReason.CLEARING);

        assertPartitionsSame(idleVerify(crd, DEFAULT_CACHE_NAME));
    }

    /**
     *
     */
    @Test
    public void testConsistencyOnCounterTriggeredRebalanceEmptyHistoryHaveTombstones() throws Exception {
        // Expecting partition not cleared.
        IgniteEx crd = startGrids(2);
        crd.cluster().state(ClusterState.ACTIVE);

        int testPart = 0;

        IgniteCache<Object, Object> cache = crd.cache(DEFAULT_CACHE_NAME);

        cache.put(testPart, 0);

        forceCheckpoint();

        stopGrid(1);
        awaitPartitionMapExchange();

        crd.cache(DEFAULT_CACHE_NAME).remove(testPart);

        crd.close();

        crd = startGrid(0);

        // Clear all history.
        String dflt = U.defaultWorkDirectory();
        assertTrue(U.delete(U.resolveWorkDirectory(dflt, DFLT_STORE_DIR + "/wal/" + crd.name(), true)));
        assertTrue(U.delete(U.resolveWorkDirectory(dflt, DFLT_STORE_DIR + "/wal/archive/" + crd.name(), true)));

        TrackingResolver rslvr = new TrackingResolver(testPart);
        IgniteEx g2 = startGrid(1, rslvr);

        awaitPartitionMapExchange();

        assertTrue(historical(1).isEmpty());
        assertNull(rslvr.reason);

        assertPartitionsSame(idleVerify(crd, DEFAULT_CACHE_NAME));
    }

    /**
     *
     */
    @Test
    @WithSystemProperty(key = "IGNITE_PREFER_WAL_REBALANCE", value = "true") // Disable heuristic.
    public void testConsistencyOnCounterTriggeredRebalanceOutdatedHistoryHaveTombstones() throws Exception {
        // Expecting partition not cleared.
        IgniteEx crd = startGrids(2);
        crd.cluster().state(ClusterState.ACTIVE);

        int testPart = 0;

        IgniteCache<Object, Object> cache = crd.cache(DEFAULT_CACHE_NAME);

        cache.put(testPart, 0);

        forceCheckpoint();

        stopGrid(1);
        awaitPartitionMapExchange();

        // Exhaust history.
        for (int i = 0; i < 10_000; i++) {
            crd.cache(DEFAULT_CACHE_NAME).remove(testPart);
            cache.put(testPart, 0);
        }

        TrackingResolver rslvr = new TrackingResolver(testPart);
        IgniteEx g2 = startGrid(1, rslvr);

        awaitPartitionMapExchange();

        assertTrue(historical(1).isEmpty());
        assertNull(rslvr.reason);

        assertPartitionsSame(idleVerify(crd, DEFAULT_CACHE_NAME));
    }

    /**
     *
     */
    @Test
    @WithSystemProperty(key = "IGNITE_PREFER_WAL_REBALANCE", value = "true") // Disable heuristic.
    public void testConsistencyOnCounterTriggeredRebalanceOutdatedHistoryHaveTombstonesRestartDemander() throws Exception {
        // Expecting partition not cleared.
        IgniteEx crd = startGrids(2);
        crd.cluster().state(ClusterState.ACTIVE);

        int testPart = 0;

        IgniteCache<Object, Object> cache = crd.cache(DEFAULT_CACHE_NAME);

        cache.put(testPart, 0);

        forceCheckpoint();

        stopGrid(1);
        awaitPartitionMapExchange();

        // Exhaust history.
        for (int i = 0; i < 10_000; i++) {
            crd.cache(DEFAULT_CACHE_NAME).remove(testPart);
            cache.put(testPart, 0);
        }

        IgniteConfiguration cfg1 = getConfiguration(getTestIgniteInstanceName(1));
        TestRecordingCommunicationSpi spi1 = (TestRecordingCommunicationSpi) cfg1.getCommunicationSpi();
        spi1.blockMessages(TestRecordingCommunicationSpi.blockDemandMessageForGroup(CU.cacheId(DEFAULT_CACHE_NAME)));

        IgniteEx g2 = startGrid(cfg1);

        spi1.waitForBlocked();
        g2.close();

        TrackingResolver rslvr = new TrackingResolver(testPart);
        startGrid(1, rslvr);

        awaitPartitionMapExchange();

        assertTrue(historical(1).isEmpty());
        assertNull(rslvr.reason);

        assertPartitionsSame(idleVerify(crd, DEFAULT_CACHE_NAME));
    }

    /**
     *
     */
    @Test
    public void testConsistencyOnCounterTriggeredRebalanceEmptyHistoryHaveClearedTombstones() throws Exception {
        IgniteEx crd = startGrids(2);
        crd.cluster().state(ClusterState.ACTIVE);

        int testPart = 0;

        IgniteCache<Object, Object> cache = crd.cache(DEFAULT_CACHE_NAME);

        cache.put(testPart, 0);

        forceCheckpoint();

        stopGrid(1);
        awaitPartitionMapExchange();

        crd.cache(DEFAULT_CACHE_NAME).remove(testPart);

        clearTombstones(cache);

        stopGrid(0, false);

        crd = startGrid(0);

        // Clear history.
        String dflt = U.defaultWorkDirectory();
        assertTrue(U.delete(U.resolveWorkDirectory(dflt, DFLT_STORE_DIR + "/wal/" + crd.name(), true)));
        assertTrue(U.delete(U.resolveWorkDirectory(dflt, DFLT_STORE_DIR + "/wal/archive/" + crd.name(), true)));

        TrackingResolver rslvr = new TrackingResolver(testPart);

        startGrid(1, rslvr);

        awaitPartitionMapExchange();

        assertTrue(historical(1).isEmpty());
        assertSame(PartitionsEvictManager.EvictReason.CLEARING, rslvr.reason);

        assertPartitionsSame(idleVerify(crd, DEFAULT_CACHE_NAME));
    }

    /**
     * Tests if the tombstones are not removed during topology change.
     */
    @Test
    @WithSystemProperty(key = "DEFAULT_TOMBSTONE_TTL", value = "3000")
    public void testTombstoneSafetyOnUnstableTopology() throws Exception {
        IgniteEx crd = startGrids(2);
        crd.cluster().state(ClusterState.ACTIVE);

        int testPart = 0;

        IgniteCache<Object, Object> cache = crd.cache(DEFAULT_CACHE_NAME);

        cache.put(testPart, 0);

        stopGrid(1);

        cache.remove(testPart);

        GridCacheContext<Object, Object> ctx0 = crd.cachex(DEFAULT_CACHE_NAME).context();
        PendingEntriesTree t0 = ctx0.group().topology().localPartition(testPart).dataStore().pendingTree();
        assertFalse(t0.isEmpty());

        IgniteConfiguration cfg1 = getConfiguration(getTestIgniteInstanceName(1));
        TestRecordingCommunicationSpi spi1 = (TestRecordingCommunicationSpi) cfg1.getCommunicationSpi();
        spi1.blockMessages(TestRecordingCommunicationSpi.blockSingleExchangeMessage());

        TrackingResolver rslvr = new TrackingResolver(testPart);

        IgniteInternalFuture<Void> startFut = GridTestUtils.runAsync(() -> {
            startGrid(cfg1, rslvr);

            return null;
        });

        spi1.waitForBlocked();

        assertFalse(t0.isEmpty());

        doSleep(4000);

        boolean wasEmpty = t0.isEmpty();

        CU.unwindEvicts(ctx0);

        spi1.stopBlock();

        startFut.get();

        awaitPartitionMapExchange();

        assertPartitionsSame(idleVerify(crd, DEFAULT_CACHE_NAME));

        assertTrue(historical(1).isEmpty());
        assertNull(rslvr.reason);

        assertFalse("Expecting tombstones are not removed during PME", wasEmpty);

        CU.unwindEvicts(ctx0);
    }

    /**
     * Tests that eviction of a partition should not be triggered when the clear tombstone counter value is applicable.
     */
    @Test
    @WithSystemProperty(key = "DEFAULT_TOMBSTONE_TTL", value = "1000")
    public void testEvictionOnRebalance() throws Exception {
        IgniteEx crd = startGrids(2);

        crd.cluster().state(ClusterState.ACTIVE);


        IgniteCache<Integer, Integer> cache = crd.cache(DEFAULT_CACHE_NAME);

        int testPart = 0;
        int cnt = 100;
        List<Integer> keys = new ArrayList<>(cnt);
        Affinity<Integer> aff = affinity(cache);
        int tmp = 0;

        while (keys.size() < cnt) {
            if (aff.partition(tmp) == testPart) {
                keys.add(tmp);
                cache.put(tmp, tmp);
            }

            tmp++;
        }

        cache.remove(keys.get(0));

        // Wait for tombstone expiration. The partition clear counter should be "moved" to cnt + 1 value.
        doSleep(2_000);

        stopGrid(1);

        for (int i = 0; i < cnt / 2; ++i)
            cache.remove(keys.get(i));

        TrackingResolver rslvr = new TrackingResolver(testPart);
        IgniteConfiguration cfg1 = getConfiguration(getTestIgniteInstanceName(1));
        TestRecordingCommunicationSpi spi1 = (TestRecordingCommunicationSpi) cfg1.getCommunicationSpi();
        spi1.blockMessages(TestRecordingCommunicationSpi.blockDemandMessageForGroup(CU.cacheGroupId(DEFAULT_CACHE_NAME, null)));

        startGrid(cfg1, rslvr);

        spi1.waitForBlocked();

        assertNull("Unexpected clearing a partition on rebalance [reason=" + rslvr.reason + ']', rslvr.reason);

        spi1.stopBlock();

        awaitPartitionMapExchange();

        assertPartitionsSame(idleVerify(crd, DEFAULT_CACHE_NAME));
    }

    /**
     * @param idx Grid index.
     * @return Historical set.
     */
    private Set<Integer> historical(int idx) {
        GridDhtPartitionDemander.RebalanceFuture rebFut =
            (GridDhtPartitionDemander.RebalanceFuture) grid(idx).cachex(DEFAULT_CACHE_NAME).context().group().preloader().rebalanceFuture();

        Set<Integer> histParts = U.field(rebFut, "historical");

        return histParts;
    }

    /** */
    private static class TrackingResolver implements DependencyResolver {
        /** Reason. */
        public volatile PartitionsEvictManager.EvictReason reason;

        /** */
        private final int testId;

        /**
         * @param testId Test id.
         */
        public TrackingResolver(int testId) {
            this.testId = testId;
        }

        /** {@inheritDoc} */
        @Override public <T> T resolve(T instance) {
            if (instance instanceof GridDhtPartitionTopologyImpl) {
                GridDhtPartitionTopologyImpl top = (GridDhtPartitionTopologyImpl) instance;

                top.partitionFactory(new GridDhtPartitionTopologyImpl.PartitionFactory() {
                    @Override public GridDhtLocalPartition create(
                        GridCacheSharedContext ctx,
                        CacheGroupContext grp,
                        int id,
                        boolean recovery
                    ) {
                        return new GridDhtLocalPartition(ctx, grp, id, recovery) {
                            @Override protected long clearAll(BooleanSupplier stopClo, PartitionsEvictManager.PartitionEvictionTask task) throws NodeStoppingException {
                                if (testId == id)
                                    reason = task.reason();

                                return super.clearAll(stopClo, task);
                            }
                        };
                    }
                });
            }

            return instance;
        }
    }
}
