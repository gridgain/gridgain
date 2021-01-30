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
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.LongAdder;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ModifiedExpiryPolicy;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSet;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.PartitionUpdateCounter;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.tree.PendingEntriesTree;
import org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor;
import org.apache.ignite.internal.processors.metric.impl.MetricUtils;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.ASYNC;

/**
 *
 */
@RunWith(Parameterized.class)
public class CachePendingEntriesEvictTest extends GridCommonAbstractTest {
    /** */
    public static final int PARTS = 64;

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
        super.beforeTest();

        cleanPersistenceDir();

        IgniteEx crd = startGrid(0);
        crd.cluster().state(ClusterState.ACTIVE);

        Thread worker = cleanupWorkerThread();

        assertTrue(GridTestUtils.waitForCondition(() -> worker.getState() == Thread.State.TIMED_WAITING, 5_000));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     *
     */
    @Test
    @WithSystemProperty(key = "DEFAULT_TOMBSTONE_TTL", value = "500") // Reduce tombstone TTL.
    @WithSystemProperty(key = "CLEANUP_WORKER_SLEEP_INTERVAL", value = "10000000") // Disable async clearing.
    @WithSystemProperty(key = "IGNITE_UNWIND_THROTTLING_TIMEOUT", value = "0") // Disable unwind throttling.
    public void testExplicitTombstonesClearing() throws Exception {
        IgniteEx crd = grid(0);

        IgniteCache<Object, Object> cache = crd.createCache(cacheConfiguration(atomicityMode).setGroupName("test"));

        int part = 0;

        GridCacheContext<Object, Object> ctx = grid(0).cachex(DEFAULT_CACHE_NAME).context();
        CacheGroupContext grpCtx = ctx.group();

        cache.remove(part);
        doSleep(1);
        cache.remove(part + PARTS);

        IgniteCacheOffheapManager.CacheDataStore store = grpCtx.topology().localPartition(part).dataStore();
        PartitionUpdateCounter cntr = store.partUpdateCounter();

        doSleep(1000);

        validateCache(grpCtx, part, 2, 0);

        ctx.ttl().expire(1);

        validateCache(grpCtx, part, 1, 0);
        assertEquals(1, cntr.tombstoneClearCounter());

        ctx.ttl().expire(1);

        validateCache(grpCtx, part, 0, 0);
        assertEquals(2, cntr.tombstoneClearCounter());

        assertTrue(store.pendingTree().isEmpty());
    }

    /**
     *
     */
    @Test
    @WithSystemProperty(key = "DEFAULT_TOMBSTONE_TTL", value = "500") // Reduce tombstone TTl
    @WithSystemProperty(key = "CLEANUP_WORKER_SLEEP_INTERVAL", value = "10000000") // Disable async clearing.
    @WithSystemProperty(key = "IGNITE_UNWIND_THROTTLING_TIMEOUT", value = "0") // Disable throttling
    public void testSystemCacheTombstoneClearing() throws IgniteCheckedException {
        IgniteEx crd = grid(0);

        IgniteInternalCache<Object, Object> cache = crd.context().cache().utilityCache();

        int pk = 0;

        GridCacheContext<Object, Object> ctx = cache.context();
        CacheGroupContext grpCtx = ctx.group();

        cache.remove(pk);

        PartitionUpdateCounter cntr = grpCtx.topology().localPartition(pk).dataStore().partUpdateCounter();

        doSleep(1000);

        validateCache(grpCtx, pk, 1, 0);

        ctx.ttl().expire(5);

        validateCache(grpCtx, pk, 0, 0);
        assertEquals(1, cntr.tombstoneClearCounter());
    }

    /**
     *
     */
    @Test
    @WithSystemProperty(key = "DEFAULT_TOMBSTONE_TTL", value = "500") // Reduce tombstone TTl
    @WithSystemProperty(key = "CLEANUP_WORKER_SLEEP_INTERVAL", value = "10000000") // Disable async clearing.
    @WithSystemProperty(key = "IGNITE_UNWIND_THROTTLING_TIMEOUT", value = "0") // Disable throttling
    public void testDataStructureCacheTombstoneClearing() throws IgniteCheckedException {
        IgniteEx crd = grid(0);

        final String grpName = "testGrp";

        // Trigger ds cache start.
        IgniteSet<Object> set = crd.set("testSet", new CollectionConfiguration().setGroupName(grpName));

        IgniteInternalCache<Object, Object> cache =
            crd.context().cache().internalCache(DataStructuresProcessor.ATOMICS_CACHE_NAME + "@" + grpName);

        int pk = 0;

        GridCacheContext<Object, Object> ctx = cache.context();
        CacheGroupContext grpCtx = ctx.group();

        cache.remove(pk);

        PartitionUpdateCounter cntr = grpCtx.topology().localPartition(pk).dataStore().partUpdateCounter();

        doSleep(1000);

        validateCache(grpCtx, pk, 1, 0);

        ctx.ttl().expire(1);

        validateCache(grpCtx, pk, 0, 0);
        assertEquals(1, cntr.tombstoneClearCounter());
    }

    /**
     *
     */
    @Test
    @WithSystemProperty(key = "DEFAULT_TOMBSTONE_TTL", value = "500") // Reduce tombstone TTl
    @WithSystemProperty(key = "CLEANUP_WORKER_SLEEP_INTERVAL", value = "10000000") // Disable async clearing.
    @WithSystemProperty(key = "IGNITE_UNWIND_THROTTLING_TIMEOUT", value = "0") // Disable throttling
    public void testExplicitTombstonesClearingWithTTL() throws Exception {
        IgniteEx crd = grid(0);

        CacheConfiguration<Object, Object> cacheCfg = cacheConfiguration(atomicityMode).setGroupName("test");
        cacheCfg.setExpiryPolicyFactory(FactoryBuilder.factoryOf(new ModifiedExpiryPolicy(new Duration(MILLISECONDS, 1500))));

        IgniteCache<Object, Object> cache = crd.createCache(cacheCfg);

        int part = 0;

        GridCacheContext<Object, Object> ctx = grid(0).cachex(DEFAULT_CACHE_NAME).context();
        CacheGroupContext grpCtx = ctx.group();

        cache.put(part + PARTS, 0); // Should expire after 1500 msec.
        cache.remove(part);

        PartitionUpdateCounter cntr = grpCtx.topology().localPartition(part).dataStore().partUpdateCounter();

        doSleep(1000);

        validateCache(grpCtx, part, 1, 1);

        ctx.ttl().expire(1);

        validateCache(grpCtx, part, 0, 1);
        assertEquals(2, cntr.tombstoneClearCounter());

        doSleep(1000);

        ctx.ttl().expire(1);

        validateCache(grpCtx, part, 0, 0);
        assertEquals(2, cntr.tombstoneClearCounter());

        cache.put(part + PARTS * 2, 0);

        validateCache(grpCtx, part, 0, 1);
        assertEquals(2, cntr.tombstoneClearCounter());
    }

    /**
     *
     */
    @Test
    @WithSystemProperty(key = "DEFAULT_TOMBSTONE_TTL", value = "500") // Reduce tombstone TTl
    @WithSystemProperty(key = "CLEANUP_WORKER_SLEEP_INTERVAL", value = "10000000") // Disable async clearing.
    @WithSystemProperty(key = "IGNITE_UNWIND_THROTTLING_TIMEOUT", value = "0") // Disable throttling
    public void testExplicitTombstonesClearingWithTTLNoEager() throws Exception {
        IgniteEx crd = grid(0);

        CacheConfiguration<Object, Object> cacheCfg = cacheConfiguration(atomicityMode).setGroupName("test").setEagerTtl(false);
        cacheCfg.setExpiryPolicyFactory(FactoryBuilder.factoryOf(new ModifiedExpiryPolicy(new Duration(MILLISECONDS, 1500))));

        IgniteCache<Object, Object> cache = crd.createCache(cacheCfg);

        int part = 0;

        GridCacheContext<Object, Object> ctx = grid(0).cachex(DEFAULT_CACHE_NAME).context();
        CacheGroupContext grpCtx = ctx.group();

        cache.put(part + PARTS, 0); // Should expire after 1500 msec.
        cache.remove(part);

        PartitionUpdateCounter cntr = grpCtx.topology().localPartition(part).dataStore().partUpdateCounter();

        doSleep(1000);

        validateCache(grpCtx, part, 1, 1);

        ctx.ttl().expire(1);

        validateCache(grpCtx, part, 0, 1);
        assertEquals(2, cntr.tombstoneClearCounter());

        doSleep(1000);

        ctx.ttl().expire(1);

        validateCache(grpCtx, part, 0, 1); // TTL entry should not be removed.
        assertEquals(2, cntr.tombstoneClearCounter());

        cache.put(part + PARTS * 2, 0);

        validateCache(grpCtx, part, 0, 2);
        assertEquals(2, cntr.tombstoneClearCounter());
    }

    /**
     * Tests pending tree consistency in put remove scenario.
     */
    @Test
    @WithSystemProperty(key = "DEFAULT_TOMBSTONE_TTL", value = "500") // Reduce tombstone TTL
    @WithSystemProperty(key = "CLEANUP_WORKER_SLEEP_INTERVAL", value = "10000000") // Disable async clearing.
    @WithSystemProperty(key = "IGNITE_UNWIND_THROTTLING_TIMEOUT", value = "0") // Disable throttling
    @WithSystemProperty(key = "IGNITE_TTL_EXPIRE_BATCH_SIZE", value = "100")
    public void testPutRemoveLoops() throws Exception {
        long stop = U.currentTimeMillis() + 10_000;

        CacheConfiguration<Object, Object> cacheCfg = cacheConfiguration(atomicityMode);
        IgniteCache<Object, Object> cache = grid(0).createCache(cacheCfg);
        GridCacheContext<Object, Object> ctx = grid(0).cachex(DEFAULT_CACHE_NAME).context();

        int part = 0;

        PendingEntriesTree tree = null;

        Random r = new Random(0);

        List<Integer> keys = partitionKeys(cache, part, 10, 0);

        LongAdder puts = new LongAdder();
        LongAdder removes = new LongAdder();

        int c = 0;

        Set<Integer> expTs = new HashSet<>();

        while (U.currentTimeMillis() < stop) {
            c++;

            List<Integer> insertedKeys = new ArrayList<>();
            List<Integer> rmvKeys = new ArrayList<>();

            for (Integer key : keys) {
                //if (!batch)
                cache.put(key, key);

                expTs.remove(key);

                insertedKeys.add(key);

                puts.increment();

                boolean rmv = r.nextFloat() < 0.5;
                if (rmv) {
                    key = insertedKeys.get(r.nextInt(insertedKeys.size()));

                    cache.remove(key);

                    rmvKeys.add(key);

                    expTs.add(key);

                    removes.increment();
                }
            }

            if (tree == null) // Ensure initialized.
                tree = ctx.group().topology().localPartition(part).dataStore().pendingTree();

            assertEquals(expTs.size(), tree.size());
        }

        log.info("Finished loops [puts=" + puts.sum() + ", removes=" + removes.sum() +
            ", size=" + cache.size() + ", pending=" + tree.size() + ", cnt=" + c + ']');

        doSleep(1000);

        if (!tree.isEmpty()) {
            log.info(tree.printTree());

            assertFalse(ctx.ttl().expire(1000));
        }

        assertTrue(tree.isEmpty());
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
        List<CacheDataRow> tsRows = new ArrayList<>();
        List<CacheDataRow> dataRows = new ArrayList<>();

        grpCtx.offheap().partitionIterator(part, IgniteCacheOffheapManager.TOMBSTONES).forEach(tsRows::add);
        grpCtx.offheap().partitionIterator(part, IgniteCacheOffheapManager.DATA).forEach(dataRows::add);

        final LongMetric tombstoneMetric0 = grpCtx.cacheObjectContext().kernalContext().metric().registry(
            MetricUtils.cacheGroupMetricsRegistryName(grpCtx.cacheOrGroupName())).findMetric("Tombstones");

        assertEquals(grpCtx.cacheOrGroupName() + " " + part, expTsCnt, tsRows.size());
        assertEquals(grpCtx.cacheOrGroupName() + " " + part, expDataCnt, dataRows.size());
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

    /**
     * @return TTL worker thread.
     */
    private Thread cleanupWorkerThread() {
        Thread[] threads = new Thread[Thread.activeCount()];

        int cnt = Thread.enumerate(threads);

        for (int i = 0; i < cnt; i++) {
            if (threads[i].getName().contains("ttl-cleanup-worker"))
                return threads[i];
        }

        throw new AssertionError("TTL cleanup worker thread not found");
    }
}
