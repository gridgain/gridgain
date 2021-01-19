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

import java.util.Set;
import java.util.function.BooleanSupplier;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemander;
import org.apache.ignite.internal.processors.resource.DependencyResolver;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;

/**
 * Tests rebalancing scenarios after tombstones are forcefully cleared on supplier.
 */
public class TombstoneClearingCountersTest extends GridCommonAbstractTest {
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

        crd.close();

        crd = startGrid(0);

        // Clear history.
        String dflt = U.defaultWorkDirectory();
        assertTrue(U.delete(U.resolveWorkDirectory(dflt, DFLT_STORE_DIR + "/wal/" + crd.name(), true)));
        assertTrue(U.delete(U.resolveWorkDirectory(dflt, DFLT_STORE_DIR + "/wal/archive/" + crd.name(), true)));

        TrackingResolver rslvr = new TrackingResolver(testPart);
        IgniteEx g2 = startGrid(1, rslvr);

        awaitPartitionMapExchange();

        assertTrue(historical(1).isEmpty());
        assertTrue(rslvr.reason == PartitionsEvictManager.EvictReason.CLEARING);

        assertPartitionsSame(idleVerify(crd, DEFAULT_CACHE_NAME));
    }

    /**
     * @param size Size.
     */
    private void validadate(int size) {
        assertPartitionsSame(idleVerify(grid(0), DEFAULT_CACHE_NAME));

        for (Ignite grid : G.allGrids())
            assertEquals(size, grid.cache(DEFAULT_CACHE_NAME).size());
    }

    private void checkWAL(IgniteEx ig) throws IgniteCheckedException {
        WALIterator iter = walIterator(ig);

        long cntr = 0;

        while (iter.hasNext()) {
            IgniteBiTuple<WALPointer, WALRecord> tup = iter.next();

            if (tup.get2() instanceof DataRecord) {
                DataRecord rec = (DataRecord)tup.get2();

                log.info("DBG: " + rec.toString());
            }
        }
    }

    /**
     * @param ignite Ignite.
     */
    private WALIterator walIterator(IgniteEx ignite) throws IgniteCheckedException {
        IgniteWriteAheadLogManager walMgr = ignite.context().cache().context().wal();

        return walMgr.replay(null);
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
