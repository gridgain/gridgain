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
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.thread.IgniteThreadFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.IntStream.range;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;

/**
 * Tests if a concurrent tombstones cleanup doesn't trigger queue refill by same elements.
 */
@RunWith(Parameterized.class)
@WithSystemProperty(key = "IGNITE_TTL_EXPIRE_BATCH_SIZE", value = "0") // Disable implicit clearing on cache op.
@WithSystemProperty(key = "CLEANUP_WORKER_SLEEP_INTERVAL", value = "100000000") // Disable background cleanup.
@WithSystemProperty(key = "IGNITE_UNWIND_THROTTLING_TIMEOUT", value = "0") // Disable unwind throttling.
@WithSystemProperty(key = "DEFAULT_TOMBSTONE_TTL", value = "500")
public class ConcurrentTombstonesCleanupTest extends GridCommonAbstractTest {
    /** */
    private static final int PARTS = 256;

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
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setSystemThreadPoolSize(8);
        cfg.setClusterStateOnStart(ClusterState.INACTIVE);

        // Need at least 2 threads in pool to avoid deadlock on clearing.
        cfg.setRebalanceThreadPoolSize(1);
        cfg.setConsistentId(igniteInstanceName);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration().
            setWalSegmentSize(1024 * 1024).setMaxWalArchiveSize(4 * 1024 * 1024).setWalSegments(2);
        dsCfg.getDefaultDataRegionConfiguration().setPersistenceEnabled(persistence).setMaxSize(200 * 1024 * 1024);
        cfg.setDataStorageConfiguration(dsCfg);

        cfg.setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME).
            setBackups(1).
            setAtomicityMode(atomicityMode).
            setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC).
            setAffinity(new RendezvousAffinityFunction(persistence, PARTS)));

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
    public void testConcurrentCleanup() throws Exception {
        IgniteEx crd = startGrids(1);
        crd.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Object, Object> cache = crd.cache(DEFAULT_CACHE_NAME);

        int total = 32;

        Map<Integer, Integer> data = range(0, total).boxed().collect(toMap(k -> k, v -> v));

        cache.putAll(data);

        GridCacheContext<Object, Object> ctx0 = crd.cachex(DEFAULT_CACHE_NAME).context();

        assertEquals(0, tombstonesCnt(ctx0.group()));

        cache.removeAll(data.keySet());

        assertEquals(total, tombstonesCnt(ctx0.group()));

        doSleep(1000);

        int conc = 32;

        PartitionsEvictManager evictMgr = ctx0.shared().evict();

        ExecutorService svc = newFixedThreadPool(conc,
            new IgniteThreadFactory("test", "expire-thread", (t,e) -> log.error("Uncaught exception", e)));

        CountDownLatch l = new CountDownLatch(conc);
        CountDownLatch l2 = new CountDownLatch(conc);

        for (int i = 0; i < conc; i++) {
            svc.submit(new Runnable() {
                @Override public void run() {
                    l.countDown();

                    try {
                        l.await();
                    } catch (InterruptedException e) {
                        fail();
                    }

                    ctx0.ttl().expire(1);

                    l2.countDown();
                }
            });
        }

        l2.await();

        svc.shutdownNow();

        svc.awaitTermination(5_000, TimeUnit.MILLISECONDS);

        doSleep(1000);

        assertTrue("Expecting empty queue", evictMgr.evictQueue(true).isEmpty());
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

        assertEquals(grpCtx.cacheOrGroupName() + " " + part, expTsCnt, tsRows.size());
        assertEquals(grpCtx.cacheOrGroupName() + " " + part, expDataCnt, dataRows.size());
    }

    /**
     * @param grpCtx ctx.
     * @return Tombsontes count.
     */
    private int tombstonesCnt(CacheGroupContext grpCtx) {
        AtomicInteger cntr = new AtomicInteger();

        try {
            for (int p = 0; p < PARTS; p++)
                grpCtx.offheap().partitionIterator(p, IgniteCacheOffheapManager.TOMBSTONES).forEach(row -> cntr.getAndIncrement());
        }
        catch (IgniteCheckedException e) {
            fail(X.getFullStackTrace(e));
        }

        return cntr.get();
    }
}
