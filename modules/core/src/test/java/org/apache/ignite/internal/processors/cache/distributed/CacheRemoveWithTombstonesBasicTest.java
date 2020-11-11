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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.cache.Cache;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ModifiedExpiryPolicy;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheConcurrentMap;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheMapEntry;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.IgniteRebalanceIterator;
import org.apache.ignite.internal.processors.cache.MapCacheStoreStrategy;
import org.apache.ignite.internal.processors.cache.PartitionUpdateCounter;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicSingleUpdateRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.IgniteDhtDemandedPartitionsMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearAtomicCache;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheEntry;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.metric.impl.MetricUtils;
import org.apache.ignite.internal.util.GridCircularBuffer;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.lang.GridIterator;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.util.deque.FastSizeDeque;
import org.junit.Ignore;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CachePeekMode.ONHEAP;
import static org.apache.ignite.cache.CacheRebalanceMode.ASYNC;

/** */
public class CacheRemoveWithTombstonesBasicTest extends GridCommonAbstractTest {
    /** */
    public static final int PARTS = 64;

    /** */
    private boolean persistence;

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

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        persistence = false;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
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

    // TODO org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicSingleUpdateRequest.forceTransformBackups
    @Test
    public void testRemoveValueUsingInvoke() {
        // TODO
    }

    @Test
    public void testPartitionHavingTombstonesIsRented() {
        // TODO Data and tombstones must be cleared.
    }

    /**
     * Tests put-remove on primary reordered to remove-put on backup.
     * @throws Exception If failed.
     */
    @Test
    public void testAtomicReorderPutRemove() throws Exception {
        IgniteEx crd = startGrids(2);
        awaitPartitionMapExchange();

        IgniteCache<Object, Object> cache = crd.createCache(cacheConfiguration(ATOMIC));

        Integer pk = primaryKey(crd.cache(DEFAULT_CACHE_NAME));

        TestRecordingCommunicationSpi.spi(crd).blockMessages((n, msg) -> msg instanceof GridDhtAtomicSingleUpdateRequest);

        IgniteInternalFuture<?> op1 = multithreadedAsync(new Runnable() {
            @Override public void run() {
                cache.put(pk, 0);
            }
        }, 1, "op1-thread");

        TestRecordingCommunicationSpi.spi(crd).waitForBlocked();

        IgniteInternalFuture<?> op2 = multithreadedAsync(new Runnable() {
            @Override public void run() {
                cache.remove(pk);
            }
        }, 1, "op2-thread");

        TestRecordingCommunicationSpi.spi(crd).waitForBlocked(2);

        // Apply on reverse order on backup.
        TestRecordingCommunicationSpi.spi(crd).stopBlock(true, blockedMsg -> {
            GridIoMessage io = blockedMsg.ioMessage();
            GridDhtAtomicSingleUpdateRequest msg0 = (GridDhtAtomicSingleUpdateRequest) io.message();

            return msg0.value(0) == null;
        });

        op2.get();

        validateCache(grid(0).cachex(DEFAULT_CACHE_NAME).context().group(), pk, 1, 0);
        validateCache(grid(1).cachex(DEFAULT_CACHE_NAME).context().group(), pk, 1, 0);

        TestRecordingCommunicationSpi.spi(crd).stopBlock();

        op1.get();

        validateCache(grid(0).cachex(DEFAULT_CACHE_NAME).context().group(), pk, 1, 0);
        validateCache(grid(1).cachex(DEFAULT_CACHE_NAME).context().group(), pk, 1, 0);

        assertPartitionsSame(idleVerify(crd, DEFAULT_CACHE_NAME));
    }

    /**
     * Tests put-remove on primary reordered to remove-put on backup.
     * @throws Exception If failed.
     */
    @Test
    public void testAtomicReorderPutRemoveInvoke() throws Exception {
        IgniteEx crd = startGrids(2);
        awaitPartitionMapExchange();

        IgniteCache<Object, Object> cache = crd.createCache(cacheConfiguration(ATOMIC));

        Integer pk = primaryKey(crd.cache(DEFAULT_CACHE_NAME));

        TestRecordingCommunicationSpi.spi(crd).blockMessages((n, msg) -> msg instanceof GridDhtAtomicSingleUpdateRequest);

        IgniteInternalFuture<?> op1 = multithreadedAsync(new Runnable() {
            @Override public void run() {
                cache.invoke(pk, new InsertClosure(0));
            }
        }, 1, "op1-thread");

        TestRecordingCommunicationSpi.spi(crd).waitForBlocked();

        IgniteInternalFuture<?> op2 = multithreadedAsync(new Runnable() {
            @Override public void run() {
                cache.invoke(pk, new RemoveClosure());
            }
        }, 1, "op2-thread");

        TestRecordingCommunicationSpi.spi(crd).waitForBlocked(2);

        // Apply on reverse order on backup.
        TestRecordingCommunicationSpi.spi(crd).stopBlock(true, blockedMsg -> {
            GridIoMessage io = blockedMsg.ioMessage();
            GridDhtAtomicSingleUpdateRequest msg0 = (GridDhtAtomicSingleUpdateRequest) io.message();

            return msg0.value(0) == null;
        });

        op2.get();

        validateCache(grid(0).cachex(DEFAULT_CACHE_NAME).context().group(), pk, 1, 0);
        validateCache(grid(1).cachex(DEFAULT_CACHE_NAME).context().group(), pk, 1, 0);

        TestRecordingCommunicationSpi.spi(crd).stopBlock();

        op1.get();

        validateCache(grid(0).cachex(DEFAULT_CACHE_NAME).context().group(), pk, 1, 0);
        validateCache(grid(1).cachex(DEFAULT_CACHE_NAME).context().group(), pk, 1, 0);

        assertPartitionsSame(idleVerify(crd, DEFAULT_CACHE_NAME));
    }

    // TODO not needed ?
    @Test
    public void testTombstonesArePreloaded() throws Exception {
        IgniteEx crd = startGrid(0);

        IgniteCache<Object, Object> cache = crd.createCache(cacheConfiguration(ATOMIC));

        int pk = 0;
        cache.put(pk, 0);
        cache.remove(pk);

        List<CacheDataRow> rows = new ArrayList<>();
        IgniteRebalanceIterator iter = grid(0).cachex(DEFAULT_CACHE_NAME).context().group().offheap().rebalanceIterator(
            new IgniteDhtDemandedPartitionsMap(null, Collections.singleton(pk)), new AffinityTopologyVersion(2, 1));
        iter.forEach(rows::add);

        assertEquals("Expecting ts row " + rows.toString(), 1, rows.size());

        IgniteEx g1 = startGrid(1);
        awaitPartitionMapExchange();

        List<CacheDataRow> rows2 = new ArrayList<>();
        GridIterator<CacheDataRow> iter2 = g1.cachex(DEFAULT_CACHE_NAME).context().group().offheap().partitionIterator(pk, IgniteCacheOffheapManager.DATA_AND_TOMBSONES);
        iter2.forEach(rows2::add);

        assertPartitionsSame(idleVerify(grid(0), DEFAULT_CACHE_NAME));

        cache.put(pk, 1);

        validateCache(grid(0).cachex(DEFAULT_CACHE_NAME).context().group(), pk, 0, 1);
        validateCache(grid(1).cachex(DEFAULT_CACHE_NAME).context().group(), pk, 0, 1);
    }

    // Test an entry deleted by lazy TTL eviction is not producing tombstone.
    @Test
    public void testWithTTLNoNear() throws Exception {
        IgniteEx crd = startGrid(0);

        CacheConfiguration<Object, Object> cacheCfg = cacheConfiguration(ATOMIC);
        cacheCfg.setEagerTtl(false);
        cacheCfg.setExpiryPolicyFactory(FactoryBuilder.factoryOf(new ModifiedExpiryPolicy(new Duration(MILLISECONDS, 500))));

        IgniteCache<Object, Object> cache = crd.createCache(cacheCfg);

        int pk = 0;
        cache.put(pk, 0);

        validateCache(grid(0).cachex(DEFAULT_CACHE_NAME).context().group(), pk, 0, 1);

        doSleep(1500);

        assertNull(cache.get(pk));

        validateCache(grid(0).cachex(DEFAULT_CACHE_NAME).context().group(), pk, 0, 0);
    }

    // Test an entry deleted by eager TTL worker is not producing tombstone.
    @Test
    public void testWithTTLNoNear_EagerTTL() throws Exception {
        IgniteEx crd = startGrid(0);

        CacheConfiguration<Object, Object> cacheCfg = cacheConfiguration(ATOMIC);
        cacheCfg.setEagerTtl(true);
        cacheCfg.setExpiryPolicyFactory(FactoryBuilder.factoryOf(new ModifiedExpiryPolicy(new Duration(MILLISECONDS, 500))));

        IgniteCache<Object, Object> cache = crd.createCache(cacheCfg);

        int pk = 0;
        cache.put(pk, 0);

        validateCache(grid(0).cachex(DEFAULT_CACHE_NAME).context().group(), pk, 0, 1);

        // Default eager cleanup delay 500ms.
        doSleep(1500);

        validateCache(grid(0).cachex(DEFAULT_CACHE_NAME).context().group(), pk, 0, 0);
    }

    @Test
    public void testWithCacheStore() throws Exception {
        IgniteEx crd = startGrid(0);

        CacheConfiguration<Object, Object> cacheCfg = cacheConfiguration(ATOMIC);
        cacheCfg.setCacheStoreFactory(new MapCacheStoreStrategy.MapStoreFactory());

        // TODO with invokes, first put optional

        IgniteCache<Object, Object> cache = crd.createCache(cacheCfg);
        int pk = 0;
        cache.put(pk, 0);

        validateCache(grid(0).cachex(DEFAULT_CACHE_NAME).context().group(), pk, 0, 1);

        cache.remove(pk);

        validateCache(grid(0).cachex(DEFAULT_CACHE_NAME).context().group(), pk, 1, 0);
    }

    // Test reordering on atomic near cache.
    @Test
    public void testAtomicReorderPutRemoveNearCache() throws Exception {
        IgniteEx crd = startGrids(4);
        awaitPartitionMapExchange();

        CacheConfiguration<Object, Object> cacheCfg = cacheConfiguration(ATOMIC);
        cacheCfg.setNearConfiguration(new NearCacheConfiguration<>());
        //cacheCfg.setEagerTtl(true);
        //cacheCfg.setExpiryPolicyFactory(FactoryBuilder.factoryOf(new ModifiedExpiryPolicy(new Duration(MILLISECONDS, 500))));

        IgniteCache<Object, Object> cache = crd.createCache(cacheCfg);
        awaitPartitionMapExchange();

        int part = 0;
        Collection<ClusterNode> nodes = crd.affinity(DEFAULT_CACHE_NAME).mapPartitionToPrimaryAndBackups(part);

        ClusterNode nearNode = null;

        for (Ignite grid : G.allGrids()) {
            if (!nodes.contains(grid.cluster().localNode())) {
                nearNode = grid.cluster().localNode();

                break;
            }
        }

        IgniteEx near = (IgniteEx) grid(nearNode);

        GridDhtPartitionTopology topology = near.cachex(DEFAULT_CACHE_NAME).context().near().dht().topology();
        assertNull(topology.localPartition(part));

        // Create reader
        near.cache(DEFAULT_CACHE_NAME).put(part, 0);

        Ignite prim = grid(nodes.iterator().next());

        TestRecordingCommunicationSpi.spi(prim).blockMessages(GridDhtAtomicSingleUpdateRequest.class, near.name());

        IgniteInternalFuture<?> putFut = multithreadedAsync(new Runnable() {
            @Override public void run() {
                prim.cache(DEFAULT_CACHE_NAME).put(part, 0);
            }
        }, 1, "put-thread");

        TestRecordingCommunicationSpi.spi(prim).waitForBlocked();

        IgniteInternalFuture<?> rmvFut = multithreadedAsync(new Runnable() {
            @Override public void run() {
                prim.cache(DEFAULT_CACHE_NAME).remove(part);
            }
        }, 1, "remove-thread");

        TestRecordingCommunicationSpi.spi(prim).waitForBlocked(2);

        // Apply on reverse order on backup.
        TestRecordingCommunicationSpi.spi(prim).stopBlock(true, blockedMsg -> {
            GridIoMessage io = blockedMsg.ioMessage();
            GridDhtAtomicSingleUpdateRequest msg0 = (GridDhtAtomicSingleUpdateRequest) io.message();

            return msg0.value(0) == null;
        });

        rmvFut.get();

        TestRecordingCommunicationSpi.spi(prim).stopBlock();

        putFut.get();

        // Check local map.
        GridNearAtomicCache<Object, Object> nearCache =
            (GridNearAtomicCache<Object, Object>) near.cachex(DEFAULT_CACHE_NAME).context().near();

        GridCacheConcurrentMap map = nearCache.map();

        assertEquals(1, map.internalSize());

        assertNull(near.cache(DEFAULT_CACHE_NAME).get(part));

        doSleep(1000);

        assertEquals(1, map.internalSize());

        Iterable<GridCacheMapEntry> entries = map.entries(CU.cacheId(DEFAULT_CACHE_NAME));

        GridNearCacheEntry rmvd = (GridNearCacheEntry) entries.iterator().next();

        Short val = U.field(rmvd, "evictReservations");

        assertEquals(0, val.intValue());
    }

    /**
     * Tests if removed values are cleared if rmv queue is overflowed for atomic cache.
     * TODO add test with near eviction ?
     * @throws Exception
     */
    @Test
    @WithSystemProperty(key = "ATOMIC_NEAR_CACHE_RMV_HISTORY_SIZE", value = "100")
    public void testTombstonesExpirationOnRmvQueueOverflow() throws Exception {
        IgniteEx crd = startGrids(4);
        awaitPartitionMapExchange();

        CacheConfiguration<Object, Object> cacheCfg = cacheConfiguration(ATOMIC);
        cacheCfg.setNearConfiguration(new NearCacheConfiguration<>());
        //cacheCfg.setEagerTtl(true);
        //cacheCfg.setExpiryPolicyFactory(FactoryBuilder.factoryOf(new ModifiedExpiryPolicy(new Duration(MILLISECONDS, 500))));

        IgniteCache<Object, Object> cache = crd.createCache(cacheCfg);
        awaitPartitionMapExchange();

        int part = 0;
        Collection<ClusterNode> nodes = crd.affinity(DEFAULT_CACHE_NAME).mapPartitionToPrimaryAndBackups(part);

        ClusterNode nearNode = null;

        for (Ignite grid : G.allGrids()) {
            if (!nodes.contains(grid.cluster().localNode())) {
                nearNode = grid.cluster().localNode();

                break;
            }
        }

        IgniteEx near = (IgniteEx) grid(nearNode);

        int keysCnt = 200;

        List<Integer> nearKeys = partitionKeys(cache, part, keysCnt, 0);

        for (Integer nearKey : nearKeys)
            near.cache(DEFAULT_CACHE_NAME).put(nearKey, nearKey);

        GridNearAtomicCache<Object, Object> nearCache =
            (GridNearAtomicCache<Object, Object>) near.cachex(DEFAULT_CACHE_NAME).context().near();

        GridCacheConcurrentMap map = nearCache.map();

        assertEquals(keysCnt, nearKeys.size());
        assertEquals(keysCnt, map.internalSize());
        assertEquals(keysCnt, map.publicSize(CU.cacheId(DEFAULT_CACHE_NAME)));

        int rmvCnt = 100;

        for (int i = 0; i < rmvCnt; i++)
            cache.remove(nearKeys.get(i));

        // Expecting entries are not removed from map.
        assertEquals(rmvCnt, near.cache(DEFAULT_CACHE_NAME).localSize(CachePeekMode.ALL));
        assertEquals(keysCnt, map.internalSize());
        assertEquals(rmvCnt, map.publicSize(CU.cacheId(DEFAULT_CACHE_NAME)));

        for (int i = 0; i < rmvCnt; i++)
            nearCache.put(nearKeys.get(i), nearKeys.get(i));

        assertEquals(keysCnt, nearKeys.size());
        assertEquals(keysCnt, map.internalSize());
        assertEquals(keysCnt, map.publicSize(CU.cacheId(DEFAULT_CACHE_NAME)));

        for (int i = 0; i < rmvCnt; i++)
            cache.remove(nearKeys.get(i));

        assertEquals(rmvCnt, near.cache(DEFAULT_CACHE_NAME).localSize(CachePeekMode.ALL));
        assertEquals(keysCnt, map.internalSize());
        assertEquals(rmvCnt, map.publicSize(CU.cacheId(DEFAULT_CACHE_NAME)));

        // Expecting values are deleted.
        Iterable<GridCacheMapEntry> entries = map.entries(CU.cacheId(DEFAULT_CACHE_NAME));

        int cnt = 0;

        for (GridCacheMapEntry ignored : entries)
            cnt++;

        assertEquals(keysCnt, cnt);

        // Expecting rmv queue to be full.
        FastSizeDeque q = U.field(nearCache, "rmvQueue");

        assertEquals(rmvCnt, q.size());
    }

    /**
     * Tests if removed values are cleared if rmv queue is overflowed for atomic cache.
     * TODO add test with near eviction ?
     * @throws Exception
     */
    @Test
    @WithSystemProperty(key = "ATOMIC_NEAR_CACHE_RMV_HISTORY_SIZE", value = "100")
    public void testTombstonesExpirationOnRmvQueueOverflowOnNear() throws Exception {
        IgniteEx crd = startGrids(4);
        awaitPartitionMapExchange();

        CacheConfiguration<Object, Object> cacheCfg = cacheConfiguration(ATOMIC);
        cacheCfg.setNearConfiguration(new NearCacheConfiguration<>());
        //cacheCfg.setEagerTtl(true);
        //cacheCfg.setExpiryPolicyFactory(FactoryBuilder.factoryOf(new ModifiedExpiryPolicy(new Duration(MILLISECONDS, 500))));

        IgniteCache<Object, Object> cache = crd.createCache(cacheCfg);
        awaitPartitionMapExchange();

        int part = 0;
        Collection<ClusterNode> nodes = crd.affinity(DEFAULT_CACHE_NAME).mapPartitionToPrimaryAndBackups(part);

        ClusterNode nearNode = null;

        for (Ignite grid : G.allGrids()) {
            if (!nodes.contains(grid.cluster().localNode())) {
                nearNode = grid.cluster().localNode();

                break;
            }
        }

        IgniteEx near = (IgniteEx) grid(nearNode);

        int keysCnt = 200;

        List<Integer> nearKeys = partitionKeys(cache, part, keysCnt, 0);

        for (Integer nearKey : nearKeys)
            near.cache(DEFAULT_CACHE_NAME).put(nearKey, nearKey);

        GridNearAtomicCache<Object, Object> nearCache =
            (GridNearAtomicCache<Object, Object>) near.cachex(DEFAULT_CACHE_NAME).context().near();

        GridCacheConcurrentMap map = nearCache.map();

        assertEquals(keysCnt, nearKeys.size());
        assertEquals(keysCnt, map.internalSize());
        assertEquals(keysCnt, map.publicSize(CU.cacheId(DEFAULT_CACHE_NAME)));

        cache = near.cache(DEFAULT_CACHE_NAME);

        int rmvCnt = 100;

        for (int i = 0; i < rmvCnt; i++)
            cache.remove(nearKeys.get(i));

        // Expecting entries are not removed from map.
        assertEquals(rmvCnt, near.cache(DEFAULT_CACHE_NAME).localSize(CachePeekMode.ALL));
        assertEquals(keysCnt, map.internalSize());
        assertEquals(rmvCnt, map.publicSize(CU.cacheId(DEFAULT_CACHE_NAME)));

        for (int i = 0; i < rmvCnt; i++)
            nearCache.put(nearKeys.get(i), nearKeys.get(i));

        assertEquals(keysCnt, nearKeys.size());
        assertEquals(keysCnt, map.internalSize());
        assertEquals(keysCnt, map.publicSize(CU.cacheId(DEFAULT_CACHE_NAME)));

        for (int i = 0; i < rmvCnt; i++)
            cache.remove(nearKeys.get(i));

        assertEquals(rmvCnt, near.cache(DEFAULT_CACHE_NAME).localSize(CachePeekMode.ALL));
        assertEquals(keysCnt, map.internalSize());
        assertEquals(rmvCnt, map.publicSize(CU.cacheId(DEFAULT_CACHE_NAME)));

        // Expecting values are deleted.
        Iterable<GridCacheMapEntry> entries = map.entries(CU.cacheId(DEFAULT_CACHE_NAME));

        int cnt = 0;

        for (GridCacheMapEntry ignored : entries)
            cnt++;

        assertEquals(keysCnt, cnt);

        // Expecting rmv queue to be full.
        FastSizeDeque q = U.field(nearCache, "rmvQueue");

        assertEquals(rmvCnt, q.size());
    }

//    @Test
//    public void testAddReaderOnExpiredDhtEntry() {
//
//    }

    @Test
    public void testRemoveOnSupplierAppliedOnDemander() {
        // TODO
    }

    @Test
    public void testPreloadingCancelledUnderLoadPutRemove() {
        // Test partition preloading retry in the middle after cancellation.
    }

    // Test the removal with expiration produces tombstone.
    @Test
    public void testRemoveWithExpiration() throws Exception {
        IgniteEx g0 = startGrid(0);

        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(ATOMIC);
        ccfg.setEagerTtl(true);

        int pk = 0;

        IgniteCache<Object, Object> cache = g0.createCache(ccfg);
        cache.put(pk, 0);
        cache.withExpiryPolicy(new ModifiedExpiryPolicy(new Duration(MILLISECONDS, 500))).remove(pk);

        doSleep(1500);

        CacheGroupContext grpCtx = grid(0).cachex(DEFAULT_CACHE_NAME).context().group();
        validateCache(grpCtx, pk, 1, 0);
    }

    // Test removing of non-existent row creates tombstone.
    @Test
    public void testRemoveNonExistentRow() throws Exception {
        IgniteEx crd = startGrid(0);

        IgniteCache<Object, Object> cache = crd.createCache(cacheConfiguration(ATOMIC));

        // Should create TS.
        int pk = 0;
        cache.remove(pk);

        CacheGroupContext grpCtx = grid(0).cachex(DEFAULT_CACHE_NAME).context().group();
        validateCache(grpCtx, pk, 1, 0);

        grpCtx.topology().localPartition(pk).clearTombstonesAsync().get();
        validateCache(grpCtx, pk, 0, 0);
    }

    // Test removing of already existing tombstone is no-op.
    @Test
    public void testRemoveExpicitTombstoneRow() throws Exception {
        IgniteEx crd = startGrid(0);

        IgniteCache<Object, Object> cache = crd.createCache(cacheConfiguration(TRANSACTIONAL));

        // Should create TS.
        int pk = 0;
        cache.remove(pk);

        CacheGroupContext grpCtx = grid(0).cachex(DEFAULT_CACHE_NAME).context().group();
        validateCache(grpCtx, pk, 1, 0);

        // Should be no-op.
        cache.remove(pk);

        validateCache(grpCtx, pk, 1, 0);
    }

    @Test
    public void testRemoveExpicitTombstoneRowAndReplace() throws Exception {
        IgniteEx crd = startGrid(0);

        IgniteCache<Object, Object> cache = crd.createCache(cacheConfiguration(TRANSACTIONAL));

        // Should create TS.
        int pk = 0;
        cache.remove(pk);

        CacheGroupContext grpCtx = grid(0).cachex(DEFAULT_CACHE_NAME).context().group();
        validateCache(grpCtx, pk, 1, 0);

        // Should be no-op.
        Object prev = cache.getAndPut(pk, 0);

        assertNull(prev);
    }

    // TODO test for many keys for each node, various tx types.
    @Test
    public void testTombstoneReplaceWithInvoke() throws Exception {
        IgniteEx crd = startGrid(0);

        IgniteCache<Object, Object> cache = crd.createCache(cacheConfiguration(ATOMIC));

        // Should create TS.
        int pk = 0;
        cache.remove(pk);

        CacheGroupContext grpCtx = grid(0).cachex(DEFAULT_CACHE_NAME).context().group();
        validateCache(grpCtx, pk, 1, 0);

        // Should be no-op.
        cache.invoke(pk, new InsertClosure(0));

        validateCache(grpCtx, pk, 0, 1);
    }

    @Test
    public void testInPlaceTombstoneRow() throws Exception {
        IgniteEx crd = startGrid(0);

        IgniteCache<Object, Object> cache = crd.createCache(cacheConfiguration(TRANSACTIONAL));
        CacheGroupContext grpCtx = grid(0).cachex(DEFAULT_CACHE_NAME).context().group();

        // Should create ts.
        int pk = 0;
        cache.put(pk, new byte[0]); // Same size as ts for in-place update.

        cache.remove(pk);

        validateCache(grpCtx, pk, 1, 0);

        cache.put(pk, new byte[0]);

        validateCache(grpCtx, pk, 0, 1);
    }

    // in-place not possible with indexes.
    // TODO move to indexing
    @Test
    @Ignore
    public void testInPlaceUpdateWithIndexes() throws Exception {
        IgniteEx crd = startGrid(0);

        QueryEntity qe = new QueryEntity();
        qe.setKeyType(Integer.class.getName()).
            setKeyFieldName("id").
            setValueType(Integer.class.getName()).
            setValueFieldName("val").
            setFields(Stream.of(
                new AbstractMap.SimpleEntry<>("id", Integer.class.getName()),
                new AbstractMap.SimpleEntry<>("val", Integer.class.getName())
            ).collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue, (a, b) -> a, LinkedHashMap::new)))
            .setIndexes(Collections.singletonList(new QueryIndex("val")));

        CacheConfiguration<Object, Object> cfg = cacheConfiguration(ATOMIC);
        cfg.setQueryEntities(Collections.singleton(qe));
        IgniteCache<Object, Object> cache = crd.createCache(cfg);

        cache.put(0, 1);
        cache.put(0, 2);

        List<Cache.Entry<Integer, Integer>> rows = cache.query(new SqlQuery<Integer, Integer>(Integer.class, "val = 2")).getAll();

        System.out.println();

        assertEquals(2, cache.get(0));
    }

    @Test
    public void testAtomicReorderRemovePut() throws Exception {
        IgniteEx crd = startGrids(2);
        awaitPartitionMapExchange();

        IgniteCache<Object, Object> cache = crd.createCache(cacheConfiguration(ATOMIC));

        Integer pk = primaryKey(crd.cache(DEFAULT_CACHE_NAME));

        cache.put(pk, 0);

        TestRecordingCommunicationSpi.spi(crd).blockMessages((n, msg) -> msg instanceof GridDhtAtomicSingleUpdateRequest);

        IgniteInternalFuture<?> op1 = multithreadedAsync(new Runnable() {
            @Override public void run() {
                cache.remove(pk);
            }
        }, 1, "op1-thread");

        TestRecordingCommunicationSpi.spi(crd).waitForBlocked();

        IgniteInternalFuture<?> op2 = multithreadedAsync(new Runnable() {
            @Override public void run() {
                cache.put(pk, 1);
            }
        }, 1, "op2-thread");

        TestRecordingCommunicationSpi.spi(crd).waitForBlocked(2);

        // Apply on reverse order on backup.
        TestRecordingCommunicationSpi.spi(crd).stopBlock(true, blockedMsg -> {
            GridIoMessage io = blockedMsg.ioMessage();
            GridDhtAtomicSingleUpdateRequest msg0 = (GridDhtAtomicSingleUpdateRequest) io.message();

            return msg0.value(0) != null;
        });

        op2.get();

        TestRecordingCommunicationSpi.spi(crd).stopBlock();

        op1.get();

        assertPartitionsSame(idleVerify(crd, DEFAULT_CACHE_NAME));

        assertEquals(1, cache.get(pk));
    }

    /**
     * Test the entry explicitely removed after expiration produces no leak on heap map.
     */
    // TODO add tx test.
    @Test
    public void testNoLeakOnExpiredEntryRemoval() throws Exception {
        IgniteEx crd = startGrid(0);

        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(ATOMIC);
        ccfg.setEagerTtl(false);

        IgniteCache<Object, Object> cache = crd.createCache(ccfg);
        CacheGroupContext grpCtx = grid(0).cachex(DEFAULT_CACHE_NAME).context().group();

        cache.put(0, 0);

        long ttl = 500;

        cache.withExpiryPolicy(new ModifiedExpiryPolicy(new Duration(MILLISECONDS, ttl))).put(0, 1);

        doSleep(ttl + 100);

        cache.remove(0);

        validateCache(grpCtx, 0, 1, 0);

        assertEquals("Cache entry is leaked", 0, cache.localSize(ONHEAP));
    }

    @Test
    public void testTombstoneLoggedToWALAsNull() throws Exception {
        persistence = true;

        IgniteEx crd = startGrid(0);
        crd.cluster().state(ClusterState.ACTIVE);

        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(TRANSACTIONAL);
        IgniteCache<Object, Object> cache = crd.createCache(ccfg);

        CacheGroupContext grpCtx = grid(0).cachex(DEFAULT_CACHE_NAME).context().group();

        int key = 0;
        cache.put(key, 0);
        cache.remove(key);

        validateCache(grpCtx, key, 1, 0);

        IgniteWriteAheadLogManager walMgr = crd.context().cache().context().wal();

        WALIterator iter = walMgr.replay(null);

        List<DataRecord> tmp = new ArrayList<>();

        while (iter.hasNext()) {
            IgniteBiTuple<WALPointer, WALRecord> tup = iter.next();

            if (tup.get2() instanceof DataRecord) {
                DataRecord rec = (DataRecord) tup.get2();

                tmp.add(rec);
            }
        }

        assertEquals(2, tmp.size());
        assertEquals(GridCacheOperation.CREATE, tmp.get(0).writeEntries().get(0).op());
        assertEquals(GridCacheOperation.DELETE, tmp.get(1).writeEntries().get(0).op());
    }

    // TODO atomic test.
    @Test
    public void testTombstoneLoggedForEachRemoveTx() throws Exception {
        persistence = true;

        IgniteEx crd = startGrid(0);
        crd.cluster().state(ClusterState.ACTIVE);

        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(TRANSACTIONAL);
        IgniteCache<Object, Object> cache = crd.createCache(ccfg);

        CacheGroupContext grpCtx = grid(0).cachex(DEFAULT_CACHE_NAME).context().group();

        int key = 0;
        assertFalse(cache.remove(key));
        assertFalse(cache.remove(key));
        assertFalse(cache.remove(key));

        IgniteWriteAheadLogManager walMgr = crd.context().cache().context().wal();

        WALIterator iter = walMgr.replay(null);

        List<DataRecord> tmp = new ArrayList<>();

        while (iter.hasNext()) {
            IgniteBiTuple<WALPointer, WALRecord> tup = iter.next();

            if (tup.get2() instanceof DataRecord) {
                DataRecord rec = (DataRecord) tup.get2();

                tmp.add(rec);
            }
        }

        validateCache(grpCtx, key, 1, 0);

        assertEquals(3, tmp.size());

        List<CacheDataRow> dataRows0 = new ArrayList<>();
        grpCtx.offheap().partitionIterator(key, IgniteCacheOffheapManager.TOMBSTONES).forEach(dataRows0::add);

        assertEquals(tmp.get(2).writeEntries().get(0).writeVersion(), dataRows0.get(0).version());
    }

    // TODO atomic test.
    @Test
    public void testTombstoneLoggedForEachRemoveAtomic() throws Exception {
        persistence = true;

        IgniteEx crd = startGrid(0);
        crd.cluster().state(ClusterState.ACTIVE);

        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(ATOMIC);
        IgniteCache<Object, Object> cache = crd.createCache(ccfg);

        CacheGroupContext grpCtx = grid(0).cachex(DEFAULT_CACHE_NAME).context().group();

        int key = 0;
        assertFalse(cache.remove(key));
        assertFalse(cache.remove(key));
        assertFalse(cache.remove(key));

        IgniteWriteAheadLogManager walMgr = crd.context().cache().context().wal();

        WALIterator iter = walMgr.replay(null);

        List<DataRecord> tmp = new ArrayList<>();

        while (iter.hasNext()) {
            IgniteBiTuple<WALPointer, WALRecord> tup = iter.next();

            if (tup.get2() instanceof DataRecord) {
                DataRecord rec = (DataRecord) tup.get2();

                tmp.add(rec);
            }
        }

        validateCache(grpCtx, key, 1, 0);

        assertEquals(3, tmp.size());

        List<CacheDataRow> dataRows0 = new ArrayList<>();
        grpCtx.offheap().partitionIterator(key, IgniteCacheOffheapManager.TOMBSTONES).forEach(dataRows0::add);

        assertEquals(tmp.get(2).writeEntries().get(0).writeVersion(), dataRows0.get(0).version());
    }

    @Test
    public void testUnswapTombstone() throws Exception {
        IgniteEx crd = startGrid(0);
        crd.cluster().state(ClusterState.ACTIVE);

        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(ATOMIC);
        IgniteCache<Object, Object> cache = crd.createCache(ccfg);
        GridCacheContext<Object, Object> ctx = grid(0).cachex(DEFAULT_CACHE_NAME).context();
        CacheGroupContext grpCtx = ctx.group();

        int key = 0;
        cache.remove(key);
        validateCache(grpCtx, key, 1, 0);

        assertNull(cache.get(key));

        GridCacheEntryEx entryEx = ctx.cache().entryEx(key);
        entryEx.unswap();

        assertTrue(entryEx.toString(), entryEx.version().order() != 0);
    }

    @Test
    public void testNoopUpdatesAndHistoricalRebalance() {
        // TODO
    }

    // Test if updating concurrently with clearing tombstones doesn't produce inconsistency.
    @Test
    public void testPutRemoveWithExpirationEagerTTL() throws Exception {
        IgniteEx crd = startGrids(3);
        awaitPartitionMapExchange();

        CacheConfiguration<Object, Object> cacheCfg = cacheConfiguration(ATOMIC);
        cacheCfg.setBackups(1);
        cacheCfg.setEagerTtl(true);
        //cacheCfg.setExpiryPolicyFactory(FactoryBuilder.factoryOf(new ModifiedExpiryPolicy(new Duration(MILLISECONDS, 500))));

        IgniteCache<Object, Object> cache = crd.createCache(cacheCfg);
        awaitPartitionMapExchange();

        // 1. Find a node having least owning partitions count.
        int idx0 = 0;
        int idx1 = 1;

        GridDhtPartitionTopology top0 = grid(idx0).cachex(DEFAULT_CACHE_NAME).context().group().topology();
        GridDhtPartitionTopology top1 = grid(idx1).cachex(DEFAULT_CACHE_NAME).context().group().topology();

        // Compute the node with less partitions.
        int idx = idx0;

        if (top0.localPartitions().size() > top1.localPartitions().size())
            idx = idx1;

        // 2. Put single update to each partition and wait for expiration.

        List<Integer> parts = new ArrayList<>();

        for (int i = 0; i < 64; i++) {
            Collection<ClusterNode> nodes = grid(0).affinity(DEFAULT_CACHE_NAME).mapKeyToPrimaryAndBackups(i);

            if (nodes.contains(grid(idx0).localNode()) && nodes.contains(grid(idx1).localNode()) && nodes.iterator().next().equals(grid(idx).localNode()))
                parts.add(i);
        }

        IgniteCache<Object, Object> cache0 = cache.withExpiryPolicy(new ModifiedExpiryPolicy(new Duration(MILLISECONDS, 1000)));

        // Versions must be synchronized on PME.
        GridCacheVersion last0 = grid(idx0).context().cache().context().versions().last();
        GridCacheVersion last1 = grid(idx1).context().cache().context().versions().last();

        assertEquals(last0, last1);

        parts.forEach(p -> cache0.put(p, p));

        parts.forEach(new Consumer<Integer>() {
            @Override public void accept(Integer p) {
                try {
                    assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
                        @Override public boolean apply() {
                            return grid(idx0).cache(DEFAULT_CACHE_NAME).localPeek(p) == null &&
                                grid(idx1).cache(DEFAULT_CACHE_NAME).localPeek(p) == null;
                        }
                    }, 5_000));
                } catch (IgniteInterruptedCheckedException e) {
                    fail(X.getFullStackTrace(e));
                }
            }
        });

//        assertEquals(0, grid(idx0).cache(DEFAULT_CACHE_NAME).localSize(CachePeekMode.PRIMARY, CachePeekMode.BACKUP));
//        assertEquals(0, grid(idx1).cache(DEFAULT_CACHE_NAME).localSize(CachePeekMode.PRIMARY, CachePeekMode.BACKUP));
//

        int pk = -1;

        for (Integer part : parts) {
            if (grid(0).affinity(DEFAULT_CACHE_NAME).isPrimary(grid(idx).localNode(), part))
                pk = part;
        }


        assertPartitionsSame(idleVerify(grid(0), DEFAULT_CACHE_NAME));

        GridCacheVersion last01 = grid(idx0).context().cache().context().versions().last();
        GridCacheVersion last11 = grid(idx1).context().cache().context().versions().last();

        cache.put(pk, -1);

        assertPartitionsSame(idleVerify(grid(0), DEFAULT_CACHE_NAME));
    }

    /**
     * TODO validate cache size, add test for many caches in group.
     *
     * @param grpCtx ctx.
     * @param part Partition.
     * @param expTsCnt Expected timestamp count.
     * @param expDataCnt Expected data count.
     */
    private void validateCache(CacheGroupContext grpCtx, int part, int expTsCnt, int expDataCnt) throws IgniteCheckedException {
        List<CacheDataRow> dataRows0 = new ArrayList<>();
        List<CacheDataRow> dataRows1 = new ArrayList<>();

        grpCtx.offheap().partitionIterator(part, IgniteCacheOffheapManager.TOMBSTONES).forEach(dataRows0::add);
        grpCtx.offheap().partitionIterator(part, IgniteCacheOffheapManager.DATA).forEach(dataRows1::add);

        final LongMetric tombstoneMetric0 = grpCtx.cacheObjectContext().kernalContext().metric().registry(
            MetricUtils.cacheGroupMetricsRegistryName(DEFAULT_CACHE_NAME)).findMetric("Tombstones");

        assertEquals(grpCtx.cacheOrGroupName() + " " + part, expTsCnt, dataRows0.size());
        assertEquals(grpCtx.cacheOrGroupName() + " " + part, expDataCnt, dataRows1.size());
        assertEquals(grpCtx.cacheOrGroupName() + " " + part, expDataCnt, grpCtx.topology().localPartition(part).dataStore().cacheSize(CU.cacheId(DEFAULT_CACHE_NAME)));
        assertEquals(grpCtx.cacheOrGroupName() + " " + part, expTsCnt, tombstoneMetric0.value());
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

    /** Insert new value into cache. */
    private static class InsertClosure implements CacheEntryProcessor {
        private final Object newVal;

        public InsertClosure(Object newVal) {
            this.newVal = newVal;
        }

        @Override public Object process(MutableEntry entry, Object... arguments) throws EntryProcessorException {
            assert entry.getValue() == null : entry;

            entry.setValue(newVal);

            return null;
        }
    }

    private static class RemoveClosure implements CacheEntryProcessor {
        @Override public Object process(MutableEntry entry, Object... arguments) throws EntryProcessorException {
            entry.remove();

            return null;
        }
    }
}
