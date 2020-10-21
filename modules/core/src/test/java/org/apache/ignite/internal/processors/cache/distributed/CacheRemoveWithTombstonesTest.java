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

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import javax.cache.Cache;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ModifiedExpiryPolicy;
import javax.cache.expiry.TouchedExpiryPolicy;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cluster.ClusterNode;
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
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheGroupIdMessage;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.IgniteRebalanceIterator;
import org.apache.ignite.internal.processors.cache.MapCacheStoreStrategy;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicSingleUpdateRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.IgniteDhtDemandedPartitionsMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.metric.impl.MetricUtils;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.lang.GridIterator;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.ASYNC;

/**
 * TODO add test multicache group, filtered nodes, client nodes.
 * TODO wal recovery should bring tombstone entry.
 * TODO make atomic/tx variants of tests when applicable.
 */
@RunWith(Parameterized.class)
@Ignore
public class CacheRemoveWithTombstonesTest extends GridCommonAbstractTest {
    /** */
    public static final int PARTS = 64;

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
        TestRecordingCommunicationSpi.spi(crd).stopBlock(true, new IgnitePredicate<T2<ClusterNode, GridIoMessage>>() {
            @Override public boolean apply(T2<ClusterNode, GridIoMessage> pair) {
                GridIoMessage io = pair.get2();
                GridDhtAtomicSingleUpdateRequest msg0 = (GridDhtAtomicSingleUpdateRequest) io.message();

                return msg0.value(0) == null;
            }
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
        TestRecordingCommunicationSpi.spi(crd).stopBlock(true, new IgnitePredicate<T2<ClusterNode, GridIoMessage>>() {
            @Override public boolean apply(T2<ClusterNode, GridIoMessage> pair) {
                GridIoMessage io = pair.get2();
                GridDhtAtomicSingleUpdateRequest msg0 = (GridDhtAtomicSingleUpdateRequest) io.message();

                return msg0.value(0) == null;
            }
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
        TestRecordingCommunicationSpi.spi(prim).stopBlock(true, new IgnitePredicate<T2<ClusterNode, GridIoMessage>>() {
            @Override public boolean apply(T2<ClusterNode, GridIoMessage> pair) {
                GridIoMessage io = pair.get2();
                GridDhtAtomicSingleUpdateRequest msg0 = (GridDhtAtomicSingleUpdateRequest) io.message();

                return msg0.value(0) == null;
            }
        });

        rmvFut.get();

        TestRecordingCommunicationSpi.spi(prim).stopBlock();

        putFut.get();

        assertNull(near.cache(DEFAULT_CACHE_NAME).get(part));
    }

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
        TestRecordingCommunicationSpi.spi(crd).stopBlock(true, new IgnitePredicate<T2<ClusterNode, GridIoMessage>>() {
            @Override public boolean apply(T2<ClusterNode, GridIoMessage> pair) {
                GridIoMessage io = pair.get2();
                GridDhtAtomicSingleUpdateRequest msg0 = (GridDhtAtomicSingleUpdateRequest) io.message();

                return msg0.value(0) != null;
            }
        });

        op2.get();

        TestRecordingCommunicationSpi.spi(crd).stopBlock();

        op1.get();

        assertPartitionsSame(idleVerify(crd, DEFAULT_CACHE_NAME));

        assertEquals(1, cache.get(pk));
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
     * @param grid Grid.
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
