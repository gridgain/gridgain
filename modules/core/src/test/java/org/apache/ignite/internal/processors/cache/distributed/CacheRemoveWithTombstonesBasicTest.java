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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
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
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
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
import org.apache.ignite.internal.processors.cache.tree.PendingEntriesTree;
import org.apache.ignite.internal.processors.cache.tree.PendingRow;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.metric.impl.MetricUtils;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.util.deque.FastSizeDeque;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CachePeekMode.ONHEAP;
import static org.apache.ignite.cache.CacheRebalanceMode.ASYNC;
import static org.junit.Assume.assumeTrue;

/** */
@RunWith(Parameterized.class)
public class CacheRemoveWithTombstonesBasicTest extends GridCommonAbstractTest {
    /** */
    public static final int PARTS = 64;

    /** */
    private static final int WAIT_FOR_EAGER_TTL_CLEANUP = 1100;

    /** */
    private static final String TS_METRIC_NAME = "Tombstones";

    /** */
    @Parameterized.Parameter(value = 0)
    public CacheAtomicityMode atomicityMode;

    /** */
    @Parameterized.Parameter(value = 1)
    public boolean persistence;

    /**
     * @return List of test parameters.
     */
    @Parameterized.Parameters(name = "mode={0} persistence={1}")
    public static List<Object[]> parameters() {
        ArrayList<Object[]> params = new ArrayList<>();

        params.add(new Object[]{ATOMIC, false});
        params.add(new Object[]{ATOMIC, true});
        params.add(new Object[]{TRANSACTIONAL, false});
        params.add(new Object[]{TRANSACTIONAL, true});

        return params;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setClusterStateOnStart(ClusterState.INACTIVE);

        TestRecordingCommunicationSpi commSpi = new TestRecordingCommunicationSpi();

        cfg.setConsistentId(gridName);

        cfg.setCommunicationSpi(commSpi);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration().setWalSegmentSize(4 * 1024 * 1024)
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setInitialSize(256L * 1024 * 1024)
                    .setMaxSize(256L * 1024 * 1024)
                    .setPersistenceEnabled(persistence)
            );

        cfg.setDataStorageConfiguration(dsCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** */
    @Test
    public void testSimpleRemove() throws Exception {
        IgniteEx crd = startGrids(1);
        crd.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Object, Object> cache0 = crd.createCache(cacheConfiguration(atomicityMode));

        final int part = 0;
        List<Integer> keys = loadDataToPartition(part, crd.name(), DEFAULT_CACHE_NAME, 100, 0);

        assertEquals(100, cache0.size());

        for (Integer key : keys)
            cache0.remove(key);

        final LongMetric tsMetric = crd.context().metric().registry(
            MetricUtils.cacheGroupMetricsRegistryName(DEFAULT_CACHE_NAME)).findMetric(TS_METRIC_NAME);

        assertEquals(100, tsMetric.value());

        validateCache(grid(0).cachex(DEFAULT_CACHE_NAME).context().group(), part, 100, 0);
    }

    /** */
    @Test
    public void testSimpleRemove2() throws Exception {
        IgniteEx crd = startGrids(3);
        crd.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Object, Object> cache0 = crd.createCache(cacheConfiguration(atomicityMode));

        final int part = 0;
        List<Integer> keys = loadDataToPartition(part, crd.name(), DEFAULT_CACHE_NAME, 100, 0);

        assertEquals(100, cache0.size());

        for (Integer key : keys)
            cache0.remove(key);

        final LongMetric tsMetric = crd.context().metric().registry(
            MetricUtils.cacheGroupMetricsRegistryName(DEFAULT_CACHE_NAME)).findMetric(TS_METRIC_NAME);

        assertEquals(100, tsMetric.value());

        validateCache(grid(0).cachex(DEFAULT_CACHE_NAME).context().group(), part, 100, 0);
        validateCache(grid(1).cachex(DEFAULT_CACHE_NAME).context().group(), part, 100, 0);
        validateCache(grid(2).cachex(DEFAULT_CACHE_NAME).context().group(), part, 100, 0);
    }

    /** */
    @Test
    public void testIterator() throws Exception {
        IgniteEx crd = startGrids(3);
        crd.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Object, Object> cache0 = crd.createCache(cacheConfiguration(atomicityMode));

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

        final LongMetric tsMetric = crd.context().metric().registry(
            MetricUtils.cacheGroupMetricsRegistryName(DEFAULT_CACHE_NAME)).findMetric("Tombstones");

        assertEquals(cnt / 2, tsMetric.value());

        CacheGroupContext grp = crd.cachex(DEFAULT_CACHE_NAME).context().group();

        List<CacheDataRow> dataRows = new ArrayList<>();
        grp.offheap().partitionIterator(part, IgniteCacheOffheapManager.DATA).forEach(dataRows::add);

        List<CacheDataRow> tsRows = new ArrayList<>();
        grp.offheap().partitionIterator(part, IgniteCacheOffheapManager.TOMBSTONES).forEach(tsRows::add);

        assertNull(crd.cache(DEFAULT_CACHE_NAME).get(tsKeys.get(0)));

        crd.cache(DEFAULT_CACHE_NAME).put(tsKeys.get(0), 0);

        assertEquals(cnt / 2 - 1, tsMetric.value());

        List<CacheDataRow> dataRows0 = new ArrayList<>();
        grp.offheap().partitionIterator(part, IgniteCacheOffheapManager.DATA).forEach(dataRows0::add);

        List<CacheDataRow> tsRows0 = new ArrayList<>();
        grp.offheap().partitionIterator(part, IgniteCacheOffheapManager.TOMBSTONES).forEach(tsRows0::add);

        grp.topology().localPartition(part).clearTombstonesAsync().get();

        List<CacheDataRow> dataRows1 = new ArrayList<>();
        grp.offheap().partitionIterator(part, IgniteCacheOffheapManager.DATA).forEach(dataRows1::add);

        List<CacheDataRow> tsRows1 = new ArrayList<>();
        grp.offheap().partitionIterator(part, IgniteCacheOffheapManager.TOMBSTONES).forEach(tsRows1::add);

        assertEquals(0, tsMetric.value());
    }

    /** */
    @Test
    public void testRemoveValueUsingInvoke() throws Exception {
        IgniteEx crd = startGrids(3);
        crd.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Object, Object> cache0 = crd.createCache(cacheConfiguration(atomicityMode));

        final int part = 0;
        List<Integer> keys = loadDataToPartition(part, crd.name(), DEFAULT_CACHE_NAME, 100, 0);

        assertEquals(100, cache0.size());

        for (Integer key : keys)
            cache0.invoke(key, new RemoveClosure());

        final LongMetric tsMetric = crd.context().metric().registry(
            MetricUtils.cacheGroupMetricsRegistryName(DEFAULT_CACHE_NAME)).findMetric(TS_METRIC_NAME);

        assertEquals(100, tsMetric.value());
    }

    /**
     * Tests put-remove on primary reordered to remove-put on backup for atomic cache.
     * @throws Exception If failed.
     */
    @Test
    public void testAtomicReorderPutRemove() throws Exception {
        assumeTrue(atomicityMode == ATOMIC);

        IgniteEx crd = startGrids(2);
        crd.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Object, Object> cache = crd.createCache(cacheConfiguration(atomicityMode));

        Integer pk = primaryKey(crd.cache(DEFAULT_CACHE_NAME));

        TestRecordingCommunicationSpi.spi(crd).blockMessages((n, msg) -> msg instanceof GridDhtAtomicSingleUpdateRequest);

        IgniteInternalFuture<?> op1 = multithreadedAsync(() -> cache.put(pk, 0), 1, "op1-thread");

        TestRecordingCommunicationSpi.spi(crd).waitForBlocked();

        IgniteInternalFuture<?> op2 = multithreadedAsync(() -> cache.remove(pk), 1, "op2-thread");

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
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = "DEFAULT_TOMBSTONE_TTL", value = "50") // Disable cleanup by unwindEvicts.
    @WithSystemProperty(key = "IGNITE_TTL_EXPIRE_BATCH_SIZE", value = "0") // Disable implicit clearing on cache op.
    @WithSystemProperty(key = "CLEANUP_WORKER_SLEEP_INTERVAL", value = "100000000") // Disable background cleanup.
    @WithSystemProperty(key = "IGNITE_UNWIND_THROTTLING_TIMEOUT", value = "0") // Disable unwind throttling.
    public void testAtomicReorderPutRemovePutRemove() throws Exception {
        assumeTrue(atomicityMode == ATOMIC);

        IgniteEx crd = startGrids(2);
        crd.cluster().state(ClusterState.ACTIVE);

        doSleep(500);

        IgniteCache<Object, Object> cache = crd.createCache(cacheConfiguration(atomicityMode));

        Integer pk = primaryKey(crd.cache(DEFAULT_CACHE_NAME));

        TestRecordingCommunicationSpi.spi(crd).record(GridDhtAtomicSingleUpdateRequest.class);

        for (int i = 0; i < PERMUTATIONS.length; i++) {
            int[] permutation = PERMUTATIONS[i];

            log.info("Testing permutation " + Arrays.toString(permutation));

            TestRecordingCommunicationSpi.spi(crd).blockMessages((n, msg) -> msg instanceof GridDhtAtomicSingleUpdateRequest);

            IgniteInternalFuture[] futs = new IgniteInternalFuture[4];

            futs[0] = multithreadedAsync(() -> cache.put(pk, 0), 1, "op1-thread");

            TestRecordingCommunicationSpi.spi(crd).waitForBlocked();

            futs[1] = multithreadedAsync(() -> cache.remove(pk), 1, "op2-thread");

            TestRecordingCommunicationSpi.spi(crd).waitForBlocked(2);

            futs[2] = multithreadedAsync(() -> cache.put(pk, 1), 1, "op3-thread");

            TestRecordingCommunicationSpi.spi(crd).waitForBlocked(3);

            futs[3] = multithreadedAsync(() -> cache.remove(pk), 1, "op4-thread");

            TestRecordingCommunicationSpi.spi(crd).waitForBlocked(4);

            List<Object> msgs = TestRecordingCommunicationSpi.spi(crd).recordedMessages(false);

            for (int j = 0; j < permutation.length; j++) {
                int finalJ = j;

                TestRecordingCommunicationSpi.spi(crd).stopBlock(true,
                    desc -> desc.ioMessage().message() == msgs.get(permutation[finalJ]));

                futs[permutation[finalJ]].get();
            }

            assertPartitionsSame(idleVerify(crd, DEFAULT_CACHE_NAME));

            GridCacheContext<Object, Object> ctx0 = grid(0).cachex(DEFAULT_CACHE_NAME).context();
            validateCache(ctx0.group(), pk, 1, 0);

            GridCacheContext<Object, Object> ctx1 = grid(1).cachex(DEFAULT_CACHE_NAME).context();
            validateCache(ctx1.group(), pk, 1, 0);

            doSleep(100);

            ctx0.ttl().expire(1);
            ctx1.ttl().expire(1);

            validateCache(ctx0.group(), pk, 0, 0);
            validateCache(ctx1.group(), pk, 0, 0);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = "DEFAULT_TOMBSTONE_TTL", value = "50") // Disable cleanup by unwindEvicts.
    @WithSystemProperty(key = "IGNITE_TTL_EXPIRE_BATCH_SIZE", value = "0") // Disable implicit clearing on cache op.
    @WithSystemProperty(key = "CLEANUP_WORKER_SLEEP_INTERVAL", value = "100000000") // Disable background cleanup.
    @WithSystemProperty(key = "IGNITE_UNWIND_THROTTLING_TIMEOUT", value = "0") // Disable unwind throttling.
    public void testAtomicReorderPutPutRemoveRemove() throws Exception {
        assumeTrue(atomicityMode == ATOMIC);

        IgniteEx crd = startGrids(2);
        crd.cluster().state(ClusterState.ACTIVE);

        doSleep(500);

        IgniteCache<Object, Object> cache = crd.createCache(cacheConfiguration(atomicityMode));

        Integer pk = primaryKey(crd.cache(DEFAULT_CACHE_NAME));

        TestRecordingCommunicationSpi.spi(crd).record(GridDhtAtomicSingleUpdateRequest.class);

        for (int i = 0; i < PERMUTATIONS.length; i++) {
            int[] permutation = PERMUTATIONS[i];

            log.info("Testing permutation " + Arrays.toString(permutation));

            TestRecordingCommunicationSpi.spi(crd).blockMessages((n, msg) -> msg instanceof GridDhtAtomicSingleUpdateRequest);

            IgniteInternalFuture[] futs = new IgniteInternalFuture[4];

            futs[0] = multithreadedAsync(() -> cache.put(pk, 0), 1, "op1-thread");

            TestRecordingCommunicationSpi.spi(crd).waitForBlocked();

            futs[1] = multithreadedAsync(() -> cache.put(pk, 1), 1, "op2-thread");

            TestRecordingCommunicationSpi.spi(crd).waitForBlocked(2);

            futs[2] = multithreadedAsync(() -> cache.remove(pk), 1, "op3-thread");

            TestRecordingCommunicationSpi.spi(crd).waitForBlocked(3);

            futs[3] = multithreadedAsync(() -> cache.remove(pk), 1, "op4-thread");

            TestRecordingCommunicationSpi.spi(crd).waitForBlocked(4);

            List<Object> msgs = TestRecordingCommunicationSpi.spi(crd).recordedMessages(false);

            for (int j = 0; j < permutation.length; j++) {
                int finalJ = j;

                TestRecordingCommunicationSpi.spi(crd).stopBlock(true,
                    desc -> desc.ioMessage().message() == msgs.get(permutation[finalJ]));

                futs[permutation[finalJ]].get();
            }

            assertPartitionsSame(idleVerify(crd, DEFAULT_CACHE_NAME));

            GridCacheContext<Object, Object> ctx0 = grid(0).cachex(DEFAULT_CACHE_NAME).context();
            validateCache(ctx0.group(), pk, 1, 0);

            GridCacheContext<Object, Object> ctx1 = grid(1).cachex(DEFAULT_CACHE_NAME).context();
            validateCache(ctx1.group(), pk, 1, 0);

            doSleep(100);

            ctx0.ttl().expire(1);
            ctx1.ttl().expire(1);

            validateCache(ctx0.group(), pk, 0, 0);
            validateCache(ctx1.group(), pk, 0, 0);
        }
    }

    /**
     * Tests put-remove on primary reordered to remove-put on backup.
     * @throws Exception If failed.
     */
    @Test
    public void testAtomicReorderPutRemoveInvoke() throws Exception {
        assumeTrue(atomicityMode == ATOMIC);

        IgniteEx crd = startGrids(2);
        crd.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Object, Object> cache = crd.createCache(cacheConfiguration(ATOMIC));

        Integer pk = primaryKey(crd.cache(DEFAULT_CACHE_NAME));

        TestRecordingCommunicationSpi.spi(crd).blockMessages((n, msg) -> msg instanceof GridDhtAtomicSingleUpdateRequest);

        IgniteInternalFuture<?> op1 = multithreadedAsync(() -> cache.invoke(pk, new InsertClosure(0)), 1, "op1-thread");

        TestRecordingCommunicationSpi.spi(crd).waitForBlocked();

        IgniteInternalFuture<?> op2 = multithreadedAsync(() -> cache.invoke(pk, new RemoveClosure()), 1, "op2-thread");

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
     * Tests if tombstones are transferred during rebalancing.
     */
    @Test
    public void testTombstonesArePreloaded() throws Exception {
        IgniteEx crd = startGrid(0);
        crd.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Object, Object> cache = crd.createCache(cacheConfiguration(atomicityMode));

        int part = 0;
        cache.put(part, 0);
        cache.remove(part);

        List<CacheDataRow> rows = new ArrayList<>();
        CacheGroupContext grpCtx0 = grid(0).cachex(DEFAULT_CACHE_NAME).context().group();
        IgniteRebalanceIterator iter = grpCtx0.offheap().rebalanceIterator(
            new IgniteDhtDemandedPartitionsMap(null, Collections.singleton(part)), new AffinityTopologyVersion(2, 1));
        iter.forEach(rows::add);

        assertEquals("Expecting ts row " + rows.toString(), 1, rows.size());

        startGrid(1);

        if (persistence)
            resetBaselineTopology();

        awaitPartitionMapExchange();

        assertPartitionsSame(idleVerify(grid(0), DEFAULT_CACHE_NAME));

        CacheGroupContext grpCtx1 = grid(1).cachex(DEFAULT_CACHE_NAME).context().group();

        validateCache(grpCtx0, part, 1, 0);
        validateCache(grpCtx1, part, 1, 0);

        PendingEntriesTree t0 = grpCtx0.topology().localPartition(part).dataStore().pendingTree();
        PendingEntriesTree t1 = grpCtx1.topology().localPartition(part).dataStore().pendingTree();

        PendingRow r0 = t0.findFirst();
        PendingRow r1 = t1.findFirst();

        assertTrue(r1.expireTime > r0.expireTime); // Tombstone TTL is refreshed after preloading.

        cache.put(part, 1);

        validateCache(grpCtx0, part, 0, 1);
        validateCache(grpCtx1, part, 0, 1);
    }

    /**
     * Tests if tombstones are transferred during rebalancing if a TTL has expired while a node was offline.
     */
    @Test
    @WithSystemProperty(key = "DEFAULT_TOMBSTONE_TTL", value = "500")
    @WithSystemProperty(key = "CLEANUP_WORKER_SLEEP_INTERVAL", value = "100000000")
    public void testTombstonesArePreloadedAfterExpiration() throws Exception {
        assumeTrue(persistence); // In volatile mode tombstones cleanup is never blocked.

        IgniteEx crd = startGrids(2); // Create baseline.
        crd.cluster().baselineAutoAdjustEnabled(false);
        crd.cluster().state(ClusterState.ACTIVE);

        stopGrid(1);

        final int part = 0;

        IgniteCache<Object, Object> cache = crd.createCache(cacheConfiguration(atomicityMode));

        CacheGroupContext grpCtx0 = grid(0).cachex(DEFAULT_CACHE_NAME).context().group();

        cache.put(part, 0);
        cache.remove(part);

        PendingEntriesTree t0 = grpCtx0.topology().localPartition(part).dataStore().pendingTree();
        PendingRow r0 = t0.findFirst();
        assertNotNull(r0);

        doSleep(700); // Wait a bit until tombstone is expired.

        CU.unwindEvicts(grpCtx0.singleCacheContext()); // Should be no-op.

        assertEquals(1, t0.size()); // Tombstones are not cleared if a baseline is not complete.

        startGrid(1);
        awaitPartitionMapExchange();

        CacheGroupContext grpCtx1 = grid(1).cachex(DEFAULT_CACHE_NAME).context().group();
        PendingEntriesTree t1 = grpCtx1.topology().localPartition(part).dataStore().pendingTree();
        PendingRow r1 = t1.findFirst();
        assertNotNull(r1);

        assertPartitionsSame(idleVerify(grid(0), DEFAULT_CACHE_NAME));

        doSleep(700);

        CU.unwindEvicts(grpCtx0.singleCacheContext());
        CU.unwindEvicts(grpCtx1.singleCacheContext());

        validateCache(grpCtx0, part, 0, 0);
        validateCache(grpCtx1, part, 0, 0);

        assertTrue(r1.expireTime > r0.expireTime);
    }

    /**
     * Tests if an entry deleted by lazy TTL eviction is not keeping tombstone.
     */
    @Test
    public void testWithTTLNoNear() throws Exception {
        IgniteEx crd = startGrid(0);
        crd.cluster().state(ClusterState.ACTIVE);

        CacheConfiguration<Object, Object> cacheCfg = cacheConfiguration(atomicityMode);
        cacheCfg.setEagerTtl(false);
        cacheCfg.setExpiryPolicyFactory(FactoryBuilder.factoryOf(new ModifiedExpiryPolicy(new Duration(MILLISECONDS, 500))));

        IgniteCache<Object, Object> cache = crd.createCache(cacheCfg);

        int part = 0;
        cache.put(part, 0);

        validateCache(grid(0).cachex(DEFAULT_CACHE_NAME).context().group(), part, 0, 1);

        doSleep(600);

        assertNull(cache.get(part));

        validateCache(grid(0).cachex(DEFAULT_CACHE_NAME).context().group(), part, 0, 0);
    }

    /**
     * Test an entry deleted by cleanup worker is not keeping tombstone.
     */
    @Test
    public void testWithTTLNoNear_EagerTTL() throws Exception {
        IgniteEx crd = startGrid(0);
        crd.cluster().state(ClusterState.ACTIVE);

        CacheConfiguration<Object, Object> cacheCfg = cacheConfiguration(atomicityMode);
        cacheCfg.setEagerTtl(true);
        cacheCfg.setExpiryPolicyFactory(FactoryBuilder.factoryOf(new ModifiedExpiryPolicy(new Duration(MILLISECONDS, 500))));

        IgniteCache<Object, Object> cache = crd.createCache(cacheCfg);

        int part = 0;
        cache.put(part, 0);

        validateCache(grid(0).cachex(DEFAULT_CACHE_NAME).context().group(), part, 0, 1);

        doSleep(WAIT_FOR_EAGER_TTL_CLEANUP);

        validateCache(grid(0).cachex(DEFAULT_CACHE_NAME).context().group(), part, 0, 0);
    }

    /** */
    @Test
    public void testRemoveWithCacheStore() throws Exception {
        assumeTrue(!persistence);

        IgniteEx crd = startGrid(0);
        crd.cluster().state(ClusterState.ACTIVE);

        CacheConfiguration<Object, Object> cacheCfg = cacheConfiguration(atomicityMode);
        cacheCfg.setCacheStoreFactory(new MapCacheStoreStrategy.MapStoreFactory());

        IgniteCache<Object, Object> cache = crd.createCache(cacheCfg);
        int pk = 0;
        cache.put(pk, 0);

        validateCache(grid(0).cachex(DEFAULT_CACHE_NAME).context().group(), pk, 0, 1);

        cache.remove(pk);

        validateCache(grid(0).cachex(DEFAULT_CACHE_NAME).context().group(), pk, 1, 0);
    }

    /** */
    @Test
    public void testRemoveWithCacheStore_2() throws Exception {
        assumeTrue(!persistence);

        IgniteEx crd = startGrid(0);
        crd.cluster().state(ClusterState.ACTIVE);

        CacheConfiguration<Object, Object> cacheCfg = cacheConfiguration(atomicityMode);
        cacheCfg.setCacheStoreFactory(new MapCacheStoreStrategy.MapStoreFactory());

        IgniteCache<Object, Object> cache = crd.createCache(cacheCfg);

        int pk = 0;
        cache.remove(pk);

        validateCache(grid(0).cachex(DEFAULT_CACHE_NAME).context().group(), pk, 1, 0);
    }

    /**
     * Tests reordering on atomic near cache.
     */
    @Test
    public void testAtomicReorderPutRemoveNearCache() throws Exception {
        assumeTrue(atomicityMode == ATOMIC);

        IgniteEx crd = startGrids(4);
        crd.cluster().state(ClusterState.ACTIVE);

        CacheConfiguration<Object, Object> cacheCfg = cacheConfiguration(ATOMIC);
        cacheCfg.setNearConfiguration(new NearCacheConfiguration<>());

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

        GridDhtPartitionTopology top = near.cachex(DEFAULT_CACHE_NAME).context().near().dht().topology();
        assertNull(top.localPartition(part));

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
     */
    @Test
    @WithSystemProperty(key = "ATOMIC_NEAR_CACHE_RMV_HISTORY_SIZE", value = "100")
    public void testTombstonesExpirationOnRmvQueueOverflow() throws Exception {
        assumeTrue(atomicityMode == ATOMIC);

        IgniteEx crd = startGrids(4);
        crd.cluster().state(ClusterState.ACTIVE);

        CacheConfiguration<Object, Object> cacheCfg = cacheConfiguration(ATOMIC);
        cacheCfg.setNearConfiguration(new NearCacheConfiguration<>());

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

        FastSizeDeque q = U.field(nearCache, "rmvQueue");

        assertTrue(q.isEmpty());

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

        // These updates make entries in deferred rmv queue not applicable (version mismatch)
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
        assertEquals(rmvCnt, q.size());
    }

    /**
     * Test if the removal with expiration produces tombstone.
     */
    @Test
    public void testRemoveWithExpiration() throws Exception {
        IgniteEx crd = startGrid(0);
        crd.cluster().state(ClusterState.ACTIVE);

        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(atomicityMode);
        ccfg.setEagerTtl(true);

        int part = 0;

        IgniteCache<Object, Object> cache = crd.createCache(ccfg);
        cache.put(part, 0);
        cache.withExpiryPolicy(new ModifiedExpiryPolicy(new Duration(MILLISECONDS, 500))).remove(part);

        doSleep(WAIT_FOR_EAGER_TTL_CLEANUP);

        CacheGroupContext grpCtx = grid(0).cachex(DEFAULT_CACHE_NAME).context().group();
        validateCache(grpCtx, part, 1, 0);
    }

    /**
     * Test removing of non-existent row creates tombstone.
     */
    @Test
    public void testRemoveNonExistentRow() throws Exception {
        IgniteEx crd = startGrid(0);
        crd.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Object, Object> cache = crd.createCache(cacheConfiguration(atomicityMode));

        // Should create tombstone.
        int part = 0;
        cache.remove(part);

        CacheGroupContext grpCtx = grid(0).cachex(DEFAULT_CACHE_NAME).context().group();
        validateCache(grpCtx, part, 1, 0);

        grpCtx.topology().localPartition(part).clearTombstonesAsync().get();
        validateCache(grpCtx, part, 0, 0);
    }

    /**
     * Test removing of non-existent row locally doesn't creates tombstone.
     */
    @Test
    public void testRemoveNonExistentRowLocally() throws Exception {
        IgniteEx crd = startGrid(0);
        crd.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Object, Object> cache = crd.createCache(cacheConfiguration(atomicityMode));

        // Should create TS.
        int part = 0;
        cache.localClear(part);

        CacheGroupContext grpCtx = grid(0).cachex(DEFAULT_CACHE_NAME).context().group();
        validateCache(grpCtx, part, 0, 0);
    }

    /**
     * Test removing of already existing tombstone is no-op.
     */
    @Test
    public void testRemoveExpicitTombstoneRow() throws Exception {
        IgniteEx crd = startGrid(0);
        crd.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Object, Object> cache = crd.createCache(cacheConfiguration(atomicityMode));

        // Should create TS.
        int part = 0;
        cache.remove(part);

        CacheGroupContext grpCtx = grid(0).cachex(DEFAULT_CACHE_NAME).context().group();
        validateCache(grpCtx, part, 1, 0);

        // Should be no-op.
        cache.remove(part);

        validateCache(grpCtx, part, 1, 0);
    }

    /**
     * Test locally removing of already existing tombstone is no-op.
     */
    @Test
    public void testRemoveExpicitTombstoneRowLocally() throws Exception {
        IgniteEx crd = startGrid(0);
        crd.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Object, Object> cache = crd.createCache(cacheConfiguration(atomicityMode));

        // Should create tombstone.
        int pk = 0;
        cache.remove(pk);

        CacheGroupContext grpCtx = grid(0).cachex(DEFAULT_CACHE_NAME).context().group();
        validateCache(grpCtx, pk, 1, 0);

        // Should be no-op.
        cache.localClear(pk);

        validateCache(grpCtx, pk, 1, 0);
    }

    /** */
    @Test
    public void testRemoveExpicitTombstoneRowAndReplace() throws Exception {
        IgniteEx crd = startGrid(0);
        crd.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Object, Object> cache = crd.createCache(cacheConfiguration(atomicityMode));

        // Should create tombstone.
        int pk = 0;
        cache.remove(pk);

        CacheGroupContext grpCtx = grid(0).cachex(DEFAULT_CACHE_NAME).context().group();
        validateCache(grpCtx, pk, 1, 0);

        // Should be no-op.
        Object prev = cache.getAndPut(pk, 0);

        assertNull(prev);
    }

    /** */
    @Test
    public void testTombstoneReplaceWithInvoke() throws Exception {
        IgniteEx crd = startGrid(0);
        crd.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Object, Object> cache = crd.createCache(cacheConfiguration(atomicityMode));

        // Should create tombstone.
        int pk = 0;
        cache.remove(pk);

        CacheGroupContext grpCtx = grid(0).cachex(DEFAULT_CACHE_NAME).context().group();
        validateCache(grpCtx, pk, 1, 0);

        // Should be no-op.
        cache.invoke(pk, new InsertClosure(0));

        validateCache(grpCtx, pk, 0, 1);
    }

    /** */
    @Test
    public void testInPlaceTombstoneRow() throws Exception {
        IgniteEx crd = startGrid(0);
        crd.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Object, Object> cache = crd.createCache(cacheConfiguration(atomicityMode));
        CacheGroupContext grpCtx = grid(0).cachex(DEFAULT_CACHE_NAME).context().group();

        // Should create ts.
        int pk = 0;
        cache.put(pk, new byte[0]); // Same size as ts for in-place update.

        cache.remove(pk);

        validateCache(grpCtx, pk, 1, 0);

        cache.put(pk, new byte[0]);

        validateCache(grpCtx, pk, 0, 1);
    }

    /** */
    @Test
    public void testAtomicReorderRemovePut() throws Exception {
        assumeTrue(atomicityMode == ATOMIC);

        IgniteEx crd = startGrids(2);
        crd.cluster().state(ClusterState.ACTIVE);

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
     * Tests if an entry is explicitely removed after expiration it doesn't produces leaks on heap map.
     */
    @Test
    public void testNoLeakOnExpiredEntryRemoval() throws Exception {
        IgniteEx crd = startGrid(0);
        crd.cluster().state(ClusterState.ACTIVE);

        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(atomicityMode);
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

    /** */
    @Test
    public void testTombstoneLoggedToWALAsNull() throws Exception {
        assumeTrue(persistence);

        IgniteEx crd = startGrid(0);
        crd.cluster().state(ClusterState.ACTIVE);

        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(atomicityMode);
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
        DataEntry dataEntry = tmp.get(1).writeEntries().get(0);
        assertEquals(GridCacheOperation.DELETE, dataEntry.op());
        assertNull(dataEntry.value());
    }

    /** */
    @Test
    public void testTombstoneLoggedForEachRemove() throws Exception {
        assumeTrue(persistence);

        IgniteEx crd = startGrid(0);
        crd.cluster().state(ClusterState.ACTIVE);

        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(atomicityMode);
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

    /**
     * Tests if unswap operation produces valid entry in heap for tombstone.
     */
    @Test
    public void testUnswapTombstone() throws Exception {
        IgniteEx crd = startGrid(0);
        crd.cluster().state(ClusterState.ACTIVE);

        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(atomicityMode);
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

    /**
     * Test if updating concurrently with clearing tombstones doesn't produce inconsistency.
     */
    @Test
    public void testPutRemoveWithExpirationEagerTTL() throws Exception {
        IgniteEx crd = startGrids(3);
        crd.cluster().state(ClusterState.ACTIVE);

        CacheConfiguration<Object, Object> cacheCfg = cacheConfiguration(atomicityMode);
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

        int pk = -1;

        for (Integer part : parts) {
            if (grid(0).affinity(DEFAULT_CACHE_NAME).isPrimary(grid(idx).localNode(), part))
                pk = part;
        }

        assertPartitionsSame(idleVerify(grid(0), DEFAULT_CACHE_NAME));

        cache.put(pk, -1);

        assertPartitionsSame(idleVerify(grid(0), DEFAULT_CACHE_NAME));
    }

    /** */
    @Test
    public void testScanClearingMovesCounters() throws Exception {
        IgniteEx crd = startGrid(0);
        crd.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Object, Object> cache = crd.createCache(cacheConfiguration(atomicityMode));

        // Should create TS.
        int pk = 0;
        cache.remove(pk);

        CacheGroupContext grpCtx = grid(0).cachex(DEFAULT_CACHE_NAME).context().group();
        validateCache(grpCtx, pk, 1, 0);

        GridDhtLocalPartition locPart = grpCtx.topology().localPartition(pk);

        PartitionUpdateCounter cntr = locPart.dataStore().partUpdateCounter();

        assertEquals(0, cntr.tombstoneClearCounter());

        locPart.clearTombstonesAsync().get();
        validateCache(grpCtx, pk, 0, 0);

        assertEquals(1, cntr.tombstoneClearCounter());
    }

    /** */
    @Test
    public void testClearingCountersFullScan() throws Exception {
        IgniteEx crd = startGrid(0);
        crd.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Object, Object> cache = crd.createCache(cacheConfiguration(atomicityMode));

        int pk = 0;
        cache.remove(pk);

        CacheGroupContext grpCtx = grid(0).cachex(DEFAULT_CACHE_NAME).context().group();
        validateCache(grpCtx, pk, 1, 0);

        GridDhtLocalPartition locPart = grpCtx.topology().localPartition(0);

        PartitionUpdateCounter cntr = locPart.dataStore().partUpdateCounter();

        grpCtx.topology().localPartition(pk).clearTombstonesAsync().get();
        validateCache(grpCtx, pk, 0, 0);

        assertEquals(1, cntr.tombstoneClearCounter());
    }

    /** */
    @Test
    public void testClearingCountersCacheClear() throws Exception {
        IgniteEx crd = startGrid(0);
        crd.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Object, Object> cache = crd.createCache(cacheConfiguration(atomicityMode));

        int part = 0;

        cache.put(part, 0);
        cache.remove(part);
        cache.put(part + PARTS, 0);
        cache.remove(part + PARTS * 2);

        cache.clear(); // Clear should skip already existing tombstones.

        CacheGroupContext grpCtx = grid(0).cachex(DEFAULT_CACHE_NAME).context().group();
        validateCache(grpCtx, part, 2, 0);

        GridDhtLocalPartition locPart = grpCtx.topology().localPartition(0);
        assertEquals("ts counter shouldn't move", 0, locPart.dataStore().partUpdateCounter().tombstoneClearCounter());
    }

    /** */
    @Test
    public void testClearingCountersRemoveAll() throws Exception {
        IgniteEx crd = startGrid(0);
        crd.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Object, Object> cache = crd.createCache(cacheConfiguration(atomicityMode));

        int part = 0;

        cache.put(part, 0);
        cache.remove(part);
        cache.put(part + PARTS, 0);
        cache.remove(part + PARTS * 2);

        cache.removeAll(); // Remove all should create tombstones and skip existing.

        CacheGroupContext grpCtx = grid(0).cachex(DEFAULT_CACHE_NAME).context().group();
        validateCache(grpCtx, part, 3, 0);

        GridDhtLocalPartition locPart = grpCtx.topology().localPartition(0);
        assertEquals("ts counter shouldn't move", 0, locPart.dataStore().partUpdateCounter().tombstoneClearCounter());
    }

    /**
     * Tests if replacing the tombstone before it's removal doesn't produce TTL on new entry.
     */
    @Test
    public void testTombstoneUpdateNoTTL() throws Exception {
        IgniteEx crd = startGrid(0);
        crd.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Object, Object> cache = crd.createCache(cacheConfiguration(atomicityMode));

        int part = 0;
        cache.put(part, 0);
        cache.remove(part);
        cache.put(part, 1);

        PendingEntriesTree tree =
            crd.cachex(DEFAULT_CACHE_NAME).context().topology().localPartition(part).dataStore().pendingTree();

        assertTrue(tree.isEmpty());
    }

    /** */
    @Test
    @WithSystemProperty(key = "DEFAULT_TOMBSTONE_TTL", value = "500") // Reduce tombstone TTL
    @WithSystemProperty(key = "CLEANUP_WORKER_SLEEP_INTERVAL", value = "1") // Disable timeout for async clearing.
    @WithSystemProperty(key = "REBALANCE_DELAY", value = "1000") // Wait one second before generating assignments.
    public void testOutdatedTombstoneNotExpired() throws Exception {
        assumeTrue(persistence);

        IgniteEx crd = startGrids(2);
        crd.cluster().state(ClusterState.ACTIVE);

        int part = 0;

        IgniteCache<Object, Object> cache = crd.createCache(cacheConfiguration(atomicityMode));
        GridCacheContext<Object, Object> ctx = grid(0).cachex(DEFAULT_CACHE_NAME).context();
        CacheGroupContext grpCtx = ctx.group();

        cache.put(part, 0);

        stopGrid(1);

        cache.remove(part);

        doSleep(1000);

        validateCache(grpCtx, part, 1, 0);

        doSleep(1000);

        validateCache(grpCtx, part, 1, 0);

        assertNull(cache.get(part));

        doSleep(1000);

        validateCache(grpCtx, part, 1, 0);

        GridDhtLocalPartition locPart = grpCtx.topology().localPartition(part);

        PartitionUpdateCounter cntr = locPart.dataStore().partUpdateCounter();

        assertEquals(0, cntr.tombstoneClearCounter());

        startGrid(1);

        awaitPartitionMapExchange();

        assertPartitionsSame(idleVerify(crd, DEFAULT_CACHE_NAME));
    }

    /**
     * @param grpCtx ctx.
     * @param part Partition.
     * @param expTsCnt Expected timestamp count.
     * @param expDataCnt Expected data count.
     */
    private void validateCache(CacheGroupContext grpCtx, int part, int expTsCnt, int expDataCnt) throws IgniteCheckedException {
        List<CacheDataRow> tsRows = new ArrayList<>();
        grpCtx.offheap().partitionIterator(part, IgniteCacheOffheapManager.TOMBSTONES).forEach(tsRows::add);

        List<CacheDataRow> dataRows = new ArrayList<>();
        grpCtx.offheap().partitionIterator(part, IgniteCacheOffheapManager.DATA).forEach(dataRows::add);

        final LongMetric tsMetric = grpCtx.cacheObjectContext().kernalContext().metric().registry(
            MetricUtils.cacheGroupMetricsRegistryName(DEFAULT_CACHE_NAME)).findMetric(TS_METRIC_NAME);

        assertEquals(grpCtx.cacheOrGroupName() + " " + part, expTsCnt, tsRows.size());
        assertEquals(grpCtx.cacheOrGroupName() + " " + part, expDataCnt, dataRows.size());
        assertEquals(grpCtx.cacheOrGroupName() + " " + part, expDataCnt,
            grpCtx.topology().localPartition(part).dataStore().cacheSize(CU.cacheId(DEFAULT_CACHE_NAME)));
        assertEquals(grpCtx.cacheOrGroupName() + " " + part, expTsCnt, tsMetric.value());
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
            .setAffinity(new RendezvousAffinityFunction(false, PARTS));
    }

    /** Insert new value into cache. */
    private static class InsertClosure implements CacheEntryProcessor<Object, Object, Object> {
        /** */
        private final Object newVal;

        /**
         * @param newVal New value.
         */
        public InsertClosure(Object newVal) {
            this.newVal = newVal;
        }

        /** {@inheritDoc} */
        @Override public Object process(MutableEntry<Object, Object> entry, Object... arguments) throws EntryProcessorException {
            assert entry.getValue() == null : entry;

            entry.setValue(newVal);

            return null;
        }
    }

    /** */
    private static class RemoveClosure implements CacheEntryProcessor<Object, Object, Object> {
        /** {@inheritDoc} */
        @Override public Object process(MutableEntry<Object, Object> entry, Object... arguments) throws EntryProcessorException {
            entry.remove();

            return null;
        }
    }

    /** Permutations. */
    private static final int[][] PERMUTATIONS = {
        {0, 1, 2, 3}, {0, 1, 3, 2}, {0, 2, 1, 3}, {0, 2, 3, 1}, {0, 3, 1, 2}, {0, 3, 2, 1}, {1, 0, 2, 3},
        {1, 0, 3, 2}, {1, 2, 0, 3}, {1, 2, 3, 0}, {1, 3, 0, 2}, {1, 3, 2, 0}, {2, 0, 1, 3}, {2, 0, 3, 1},
        {2, 1, 0, 3}, {2, 1, 3, 0}, {2, 3, 0, 1}, {2, 3, 1, 0}, {3, 0, 1, 2}, {3, 0, 2, 1}, {3, 1, 0, 2},
        {3, 1, 2, 0}, {3, 2, 0, 1}, {3, 2, 1, 0}
    };
}
