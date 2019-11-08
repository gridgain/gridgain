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

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.CacheEntryInfoCollection;
import org.apache.ignite.internal.processors.cache.GridCacheEntryInfo;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.PartitionsEvictManager;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.util.deque.FastSizeDeque;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import org.mockito.Mockito;

import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.configuration.WALMode.LOG_ONLY;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition.MAX_DELETE_QUEUE_SIZE;

/**
 * Test scenario:
 * <p>
 * 1. Put data to single partition.
 * 2. Trigger historical rebalance to make reordering possible.
 * 3. Tamper with supply message reordering entries in various ways or update cache in the middle of rebalance.
 * <p>
 * Success: partitions copies are consistent, all sizes and counters are valid.
 *
 * Note: the test doesn't use parameterization to mitigate possible backport efforts.
 */
// Enable historical rebalancing for all tests.
@WithSystemProperty(key = IGNITE_PDS_WAL_REBALANCE_THRESHOLD, value = "0")
// Prevent automatic deferred entries cleaning.
@WithSystemProperty(key = IgniteSystemProperties.IGNITE_CACHE_REMOVED_ENTRIES_TTL, value = "1000000")
public class CacheRebalancePutRemoveReorderTest extends GridCommonAbstractTest {
    /** Partitions count. */
    public static final int PARTS_CNT = 32;

    /** */
    private CacheAtomicityMode atomicityMode;

    /** */
    private boolean onheapCacheEnabled = false;

    /** */
    private boolean delayDemandMessage = false;

    /** */
    private static final int MB = 1024 * 1024;

    /** */
    private static final int PRELOADED_KEYS = 1;

    /** */
    private static final int PART = 0;

    /** */
    private static final int KEYS_CNT = 1024;

    /** */
    private static final int MAX_QUEUE_SIZE = CU.perPartitionRmvMaxQueueSize(MAX_DELETE_QUEUE_SIZE, PARTS_CNT);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TestRecordingCommunicationSpi commSpi = new TestRecordingCommunicationSpi();
        cfg.setCommunicationSpi(commSpi);

        if (delayDemandMessage) {
            commSpi.blockMessages((node, msg) -> msg instanceof GridDhtPartitionDemandMessage);

            delayDemandMessage = false;
        }

        cfg.setConsistentId("node" + igniteInstanceName);

        cfg.setFailureDetectionTimeout(100000000L);
        cfg.setClientFailureDetectionTimeout(100000000L);
        cfg.setRebalanceThreadPoolSize(4); // Necessary to reproduce some issues.

        cfg.setDataStorageConfiguration(new DataStorageConfiguration().
            setWalHistorySize(1000).
            setWalSegmentSize(8 * MB).setWalMode(LOG_ONLY).setPageSize(1024).
            setCheckpointFrequency(MILLISECONDS.convert(365, DAYS)).
            setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true).
                setInitialSize(100 * MB).setMaxSize(100 * MB)));

        cfg.setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME).
            setBackups(1).
            setOnheapCacheEnabled(onheapCacheEnabled).
            setAtomicityMode(atomicityMode).
            setWriteSynchronizationMode(FULL_SYNC).
            setAffinity(new RendezvousAffinityFunction(false, PARTS_CNT)));

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

    /** Uses tombstones to handle put-remove conflicts for tx cache. */
    @Test
    @WithSystemProperty(key = "TEST_DISABLE_RMW_QUEUE", value = "true")
    public void testPutRemoveReorderWithTombstones() throws Exception {
        atomicityMode = CacheAtomicityMode.TRANSACTIONAL;

        testPutRemoveReorder(this::putRemove2, this::reorder2, this::noop1, 1, PRELOADED_KEYS, 0, 0);
    }

    /** Uses deferred deletion queue to handle put-remove conflicts for tx cache. */
    @Test
    public void testPutRemoveReorderWithDeferredDelete() throws Exception {
        atomicityMode = CacheAtomicityMode.TRANSACTIONAL;

        testPutRemoveReorder(this::putRemove2, this::reorder2, this::noop1, 0, PRELOADED_KEYS, 1, 1);
    }

    @Test
    public void testPutRemoveReorder2() throws Exception {
        atomicityMode = CacheAtomicityMode.ATOMIC;

        testPutRemoveReorder(this::put1, this::noop2, this::remove1, 0, PRELOADED_KEYS, 1, 1);
    }


    /** */
    @Test
    public void testPutRemoveTombstonesMixedTx() throws Exception {
        atomicityMode = CacheAtomicityMode.TRANSACTIONAL;

        testPutRemoveReorder(new BiConsumer<IgniteCache, List>() {
            @Override public void accept(IgniteCache cache, List list) {
                for (Object key : list)
                    cache.put(key, key);
            }
        }, this::noop2, new BiConsumer<IgniteCache, List>() {
            @Override public void accept(IgniteCache cache, List list) {
                for (Object key : list)
                    cache.remove(key);
            }
        }, KEYS_CNT - MAX_QUEUE_SIZE, 0, MAX_QUEUE_SIZE, MAX_QUEUE_SIZE);
    }

    /** */
    @Test
    public void testRemoveNoTombstonesTx() throws Exception {
        doTestTombstonesAndQueueSizeOnRegularDeletions(CacheAtomicityMode.TRANSACTIONAL, MAX_QUEUE_SIZE, MAX_QUEUE_SIZE, 0);
    }

    /** */
    @Test
    public void testRemoveNoTombstonesAtomic() throws Exception {
        doTestTombstonesAndQueueSizeOnRegularDeletions(CacheAtomicityMode.ATOMIC, MAX_QUEUE_SIZE, MAX_QUEUE_SIZE, 0);
    }

    /** */
    @Test
    @WithSystemProperty(key = "TEST_DISABLE_RMW_QUEUE", value = "true")
    public void testRemoveNoTombstonesNoQueueTx() throws Exception {
        doTestTombstonesAndQueueSizeOnRegularDeletions(CacheAtomicityMode.TRANSACTIONAL, 0, 0, 0);
    }

    /** */
    @Test
    @WithSystemProperty(key = "TEST_DISABLE_RMW_QUEUE", value = "true")
    public void testRemoveNoTombstonesNoQueueAtomic() throws Exception {
        // Deferred queue cannot be disabled for atomic caches.
        doTestTombstonesAndQueueSizeOnRegularDeletions(CacheAtomicityMode.ATOMIC, MAX_QUEUE_SIZE, MAX_QUEUE_SIZE, 0);
    }

    /** */
    private void doTestTombstonesAndQueueSizeOnRegularDeletions(CacheAtomicityMode atomicityMode,
        int expQueueSize,
        int expInternalSize,
        int expTombstonesCnt
    ) throws Exception {
        this.atomicityMode = atomicityMode;

        IgniteEx crd = startGrids(2);
        crd.cluster().active(true);

        IgniteCache<Object, Object> cache = crd.cache(DEFAULT_CACHE_NAME);

        List<Integer> keys = partitionKeys(crd.cache(DEFAULT_CACHE_NAME), PART, KEYS_CNT, 0);

        try(IgniteDataStreamer<Object, Object> ds = crd.dataStreamer(DEFAULT_CACHE_NAME)) {
            for (Integer key : keys)
                ds.addData(key, key);
        }

        for (IgniteEx ignite : Arrays.asList(grid(0), grid(1))) {
            @Nullable GridDhtLocalPartition part =
                ignite.cachex(DEFAULT_CACHE_NAME).context().topology().localPartition(PART);

            FastSizeDeque q = U.field(part, "rmvQueue");

            for (Integer key : keys)
                cache.remove(key);

            assertEquals(expQueueSize, q.sizex());
            assertEquals(expInternalSize, part.internalSize());
            assertEquals(expTombstonesCnt, part.dataStore().tombstonesCount());
            assertEquals(0, part.dataStore().fullSize());
        }
    }

    /** */
    private void testPutRemoveReorder(BiConsumer<IgniteCache, List> putClo,
        Consumer<List> reorderClo,
        BiConsumer<IgniteCache, List> beforeReorderClo,
        int expTombstoneCnt,
        int expSize,
        int expHeapSize,
        int expDeferredQeueuSize
    ) throws Exception {
        Ignite supplier = startGrids(2);
        supplier.cluster().active(true);

        IgniteCache<Object, Object> cache = supplier.cache(DEFAULT_CACHE_NAME);

        final int part = 0;

        List<Integer> keys = partitionKeys(supplier.cache(DEFAULT_CACHE_NAME), part, KEYS_CNT, 0);

        // Partition should not be empty for historical rebalancing.
        for (int p = 0; p < PARTS_CNT * PRELOADED_KEYS; p++)
            cache.put(p, 0);

        forceCheckpoint();

        String name = grid(1).name();

        stopGrid(1);

        awaitPartitionMapExchange();

        putClo.accept(cache, keys);

        TestRecordingCommunicationSpi.spi(supplier).blockMessages((node, msg) -> {
            if (msg instanceof GridDhtPartitionSupplyMessage) {
                GridDhtPartitionSupplyMessage sup = (GridDhtPartitionSupplyMessage)msg;

                if (sup.groupId() != CU.cacheId(DEFAULT_CACHE_NAME))
                    return false;

                Map<Integer, CacheEntryInfoCollection> infos = U.field(sup, "infos");

                CacheEntryInfoCollection col = infos.get(part);

                List<GridCacheEntryInfo> infos0 = col.infos();

                reorderClo.accept(infos0);

                return true;
            }

            return false;
        });

        delayDemandMessage = true;

        IgniteEx demander = startGrid(1);

        // Replace evict manager with stubbed clearTombstonesAsync method to avoid tombstones removal on partition owning.
        stubTombstonesRemoval(demander);

        TestRecordingCommunicationSpi.spi(supplier).waitForBlocked();

        if (beforeReorderClo != null)
            beforeReorderClo.accept(cache, keys);

        TestRecordingCommunicationSpi.spi(supplier).stopBlock();

        awaitPartitionMapExchange();

        assertPartitionsSame(idleVerify(supplier, DEFAULT_CACHE_NAME));

        GridDhtLocalPartition part0 = demander.cachex(DEFAULT_CACHE_NAME).context().topology().localPartition(part);

        assertNotNull(part0);

        assertEquals(expSize, part0.dataStore().fullSize());
        assertEquals(expTombstoneCnt, part0.dataStore().tombstonesCount());
        assertEquals(expHeapSize, part0.internalSize());
        assertEquals(expDeferredQeueuSize, ((Collection)U.field(part0, "rmvQueue")).size());
    }

    /**
     * Replace partition evict manager with special version which ignores tombstones clearing on own.
     *
     * @param grid Grid.
     */
    private void stubTombstonesRemoval(IgniteEx grid) throws InterruptedException {
        TestRecordingCommunicationSpi.spi(grid).waitForBlocked();

        GridCacheSharedContext<Object, Object> ctx = grid.context().cache().context();

        PartitionsEvictManager mgr = U.field(ctx, "evictMgr");

        PartitionsEvictManager mocked = Mockito.spy(mgr);

        Mockito.doNothing().when(mocked).clearTombstonesAsync(Mockito.any(), Mockito.any());

        ctx.setEvictManager(mocked);

        TestRecordingCommunicationSpi.spi(grid).stopBlock();
    }

    /**
     * @param cache Cache.
     * @param keys Keys.
     */
    private void putRemove2(IgniteCache cache, List keys) {
        cache.put(keys.get(PRELOADED_KEYS), 1);
        cache.remove(keys.get(PRELOADED_KEYS));
    }

    /**
     * @param cache Cache.
     * @param keys Keys.
     */
    private void putRemove4(IgniteCache cache, List keys) {
        cache.put(keys.get(PRELOADED_KEYS), 1);
        cache.remove(keys.get(PRELOADED_KEYS));
        cache.put(keys.get(PRELOADED_KEYS), 1);
        cache.remove(keys.get(PRELOADED_KEYS));
    }

    /**
     * @param entries Entries.
     */
    private void reorder2(List entries) {
        Object e1 = entries.get(0);
        entries.set(0, entries.get(1));
        entries.set(1, e1);
    }

    /**
     * @param node Node.
     * @param keys Keys.
     */
    private void noop1(IgniteCache cache, List keys) {
        System.out.println();
    }

    /**
     */
    private void noop2(List entries) {
        System.out.println();
    }

    /**
     */
    private void put1(IgniteCache cache, List keys) {
        cache.put(keys.get(PRELOADED_KEYS), 1);
    }

    /**
     */
    private void remove1(IgniteCache cache, List keys) {
        cache.remove(keys.get(PRELOADED_KEYS));
    }
}
