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

import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
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
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.configuration.WALMode.LOG_ONLY;

/**
 * Test scenario:
 * <p>
 * 1. Trigger historical rebalance over single partition to make reordering possible.
 * 2. Tamper with supply message changing entries order in various ways.
 * <p>
 * Success: partitions copies are consistent, all sizes and counters are valid.
 *
 * Note: the test doesn't use parameterization to mitigate possible backport efforts.
 */
@WithSystemProperty(key = IGNITE_PDS_WAL_REBALANCE_THRESHOLD, value = "0")
// Prevent deferred entries cleaning.
@WithSystemProperty(key = IgniteSystemProperties.IGNITE_CACHE_REMOVED_ENTRIES_TTL, value = "100000")
public class CacheRebalancePutRemoveReorderTest extends GridCommonAbstractTest {
    /** Partitions count. */
    public static final int PARTS_CNT = 32;

    /** */
    private CacheAtomicityMode atomicityMode;

    private boolean onheapCacheEnabled = false;

    /** */
    private static final int MB = 1024 * 1024;

    /** */
    private static final int PRELOADED_KEYS = 1;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);
        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

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
    @WithSystemProperty(key = IgniteSystemProperties.IGNITE_ATOMIC_CACHE_DELETE_HISTORY_SIZE, value = "0")
    public void testPutRemoveReorderWithTombstones() throws Exception {
        atomicityMode = CacheAtomicityMode.TRANSACTIONAL;

        testPutRemoveReorder(this::putRemove2, this::reorder2, 1, PRELOADED_KEYS, 0, 0);
    }

    /** Uses deferred deletion queue to handle put-remove conflicts for tx cache. */
    @Test
    public void testPutRemoveReorderWithDeferredDelete() throws Exception {
        atomicityMode = CacheAtomicityMode.TRANSACTIONAL;

        testPutRemoveReorder(this::putRemove2, this::reorder2, 0, PRELOADED_KEYS, 1, 1);
    }

//    /** Uses tombstones to handle put-remove conflicts. */
//    @Test
//    public void testPutRemoveReorderWithTombstonesAndOnheapCache() throws Exception {
//        atomicityMode = CacheAtomicityMode.TRANSACTIONAL;
//
//        testPutRemoveReorder(this::putRemove2, this::reorder2, 1, 0, 0);
//    }
//
//    /** Uses deferred deletion queue to handle put-remove conflicts. */
//    @Test
//    public void testPutRemoveReorderWithDeferredDeleteAtomic() throws Exception {
//        atomicityMode = CacheAtomicityMode.ATOMIC;
//
//        testPutRemoveReorder(this::putRemove2, this::reorder2, 1, 0, 0);
//    }

    /** */
    private void testPutRemoveReorder(BiConsumer<IgniteCache, List> putClo,
        Consumer<List> reorderClo,
        int expTombstoneCnt,
        int expSize,
        int expHeapSize,
        int expDeferredQeueuSize
    ) throws Exception {
        Ignite crd = startGrids(2);
        crd.cluster().active(true);

        IgniteCache<Object, Object> cache = crd.cache(DEFAULT_CACHE_NAME);

        final int part = 0;

        List<Integer> keys = partitionKeys(crd.cache(DEFAULT_CACHE_NAME), part, 2, 0);

        // Partition should not be empty for historical rebalancing.
        for (int p = 0; p < PARTS_CNT * PRELOADED_KEYS; p++)
            cache.put(p, 0);

        forceCheckpoint();

        String name = grid(1).name();

        stopGrid(1);

        awaitPartitionMapExchange();

        putClo.accept(cache, keys);

        TestRecordingCommunicationSpi.spi(crd).blockMessages((node, msg) -> {
            if (msg instanceof GridDhtPartitionSupplyMessage) {
                GridDhtPartitionSupplyMessage sup = (GridDhtPartitionSupplyMessage)msg;

                if (sup.groupId() != CU.cacheId(DEFAULT_CACHE_NAME))
                    return false;

                Map<Integer, CacheEntryInfoCollection> infos = U.field(sup, "infos");

                CacheEntryInfoCollection col = infos.get(part);

                List<GridCacheEntryInfo> infos0 = col.infos();

                reorderClo.accept(infos0);
            }

            return false;
        });

        IgniteEx g1 = startGrid(1);

        awaitPartitionMapExchange();

        assertPartitionsSame(idleVerify(crd, DEFAULT_CACHE_NAME));

        GridDhtLocalPartition part0 = g1.cachex(DEFAULT_CACHE_NAME).context().topology().localPartition(part);

        assertNotNull(part0);

        assertEquals(expSize, part0.dataStore().fullSize());
        assertEquals(expTombstoneCnt, part0.dataStore().tombstonesCount());
        assertEquals(expHeapSize, part0.internalSize());
    }

    private void putRemove2(IgniteCache cache, List keys) {
        cache.put(keys.get(PRELOADED_KEYS), 1);
        cache.remove(keys.get(PRELOADED_KEYS));
    }

    private void reorder2(List entries) {
        Object e1 = entries.get(0);
        entries.set(0, entries.get(1));
        entries.set(1, e1);
    }
}
