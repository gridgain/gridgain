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

import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.CacheMetricsImpl;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.tree.PendingRow;
import org.apache.ignite.internal.processors.metastorage.DistributedMetastorageLifecycleListener;
import org.apache.ignite.internal.processors.metastorage.ReadableDistributedMetaStorage;
import org.apache.ignite.internal.processors.resource.DependencyResolver;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lifecycle.LifecycleBean;
import org.apache.ignite.lifecycle.LifecycleEventType;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.INACTIVE;
import static org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPreloader.PRELOADER_FORCE_CLEAR;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/**
 * Tests various scenarios while partition eviction is blocked.
 */
public class BlockedEvictionsTest extends GridCommonAbstractTest {
    /** */
    private static final long SYS_WORKER_BLOCKED_TIMEOUT = 3_000;

    /** */
    private boolean persistence;

    /** */
    private boolean stats;

    /** */
    private int sysPoolSize;

    /** Lifecycle bean that is used for additional configuration of a node on start. */
    private LifecycleBean lifecycleBean;

    /** Custorm failure handler. */
    private FailureHandler failureHandler;

    /** Number of backups. */
    private int backups = 1;

    /**
     * {@inheritDoc}
     */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setRebalanceThreadPoolSize(ThreadLocalRandom.current().nextInt(3) + 2);
        cfg.setSystemThreadPoolSize(sysPoolSize);
        cfg.setConsistentId(igniteInstanceName);

        if (persistence) {
            DataStorageConfiguration dsCfg = new DataStorageConfiguration().setWalSegmentSize(4 * 1024 * 1024);
            dsCfg.getDefaultDataRegionConfiguration().setPersistenceEnabled(persistence).setMaxSize(100 * 1024 * 1024);
            cfg.setDataStorageConfiguration(dsCfg);
        }

        cfg.setSystemWorkerBlockedTimeout(SYS_WORKER_BLOCKED_TIMEOUT);

        if (lifecycleBean != null)
            cfg.setLifecycleBeans(lifecycleBean);

        if (failureHandler != null)
            cfg.setFailureHandler(failureHandler);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();

        stats = false;
        sysPoolSize = IgniteConfiguration.DFLT_SYSTEM_CORE_THREAD_CNT;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    @Test
    @WithSystemProperty(key = PRELOADER_FORCE_CLEAR, value = "true")
    public void testCancellationEvictionTasks() throws Exception {
        persistence = true;
        backups = 2;

        IgniteEx ignite = (IgniteEx) startGridsMultiThreaded(3);

        ignite.cluster().state(ACTIVE);

        IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(cacheConfiguration());

        // Initial loading.
        for (int i = 0; i < 128; i++)
            cache.put(i, i);

        // Stop one node and update data in the cache in order to get rebalancing on restarting the node.
        // It is assumed that the rebalanncing (full rebalance) will trigger partitions clearing.
        stopGrid(2);

        for (int i = 0; i < 128; i++)
            cache.put(i, i);

        CountDownLatch rebalanceLatch = new CountDownLatch(1);

        // Lifecycle bean is needed in order to block rebalance thread pool.
        lifecycleBean = new LifecycleBean() {
            /** Ignite instance. */
            @IgniteInstanceResource
            IgniteEx ignite;

            /** {@inheritDoc} */
            @Override public void onLifecycleEvent(LifecycleEventType evt) throws IgniteException {
                if (evt == LifecycleEventType.BEFORE_NODE_START) {
                    ignite.context().internalSubscriptionProcessor()
                        .registerDistributedMetastorageListener(new DistributedMetastorageLifecycleListener() {
                            @Override public void onReadyForRead(ReadableDistributedMetaStorage metastorage) {
                                ExecutorService service = ignite.context().pools().getRebalanceExecutorService();

                                int poolSize = ignite.configuration().getRebalanceThreadPoolSize();

                                // These tasks will block the rebalance pool to emulate the situation
                                // when the pool is busy with clearing partitions.
                                for (int i = 0; i < poolSize; i++) {
                                    service.execute(() -> {
                                        try {
                                            rebalanceLatch.await();
                                        }
                                        catch (InterruptedException e) {
                                            Thread.currentThread().interrupt();
                                        }
                                    });
                                }
                            }
                        });
                }
            }
        };

        // This failure handler is needed in order to check that the exchange worker is not blocked.
        AtomicReference<FailureContext> failureContext = new AtomicReference<>();
        failureHandler = new FailureHandler() {
            @Override public boolean onFailure(Ignite ignite, FailureContext failureCtx) {
                failureContext.set(failureCtx);
                return false;
            }
        };

        // Restart the node and initiate rebalancing.
        startGrid(2);

        // Stop additional node to initiate a new round of rebalancing
        // and therefore cancelling the previously submitted task to clear partitions.
        stopGrid(1);

        // Wait for SYS_WORKER_BLOCKED_TIMEOUT * 2 seconds to be sure that
        // the failure processor had enough time to detect a possible starvation of the exchange thread.
        doSleep(SYS_WORKER_BLOCKED_TIMEOUT * 2);

        assertNull("Critical failure detected [ctx=" + failureContext.get() + ']', failureContext.get());

        rebalanceLatch.countDown();

        awaitPartitionMapExchange();
    }

    /**
     * Stopping the cache during clearing should be no-op, all partitions are expected to be cleared.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testStopCache_Volatile() throws Exception {
        testOperationDuringEviction(false, 1, p -> {
            IgniteInternalFuture fut = runAsync(() -> grid(0).cache(DEFAULT_CACHE_NAME).close());

            doSleep(500);
        }, this::preload);

        awaitPartitionMapExchange(true, true, null);
        assertPartitionsSame(idleVerify(grid(0), DEFAULT_CACHE_NAME));
    }

    /**
     * Stopping the cache during clearing should be no-op, all partitions are expected to be cleared.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testStopCache_Persistence() throws Exception {
        testOperationDuringEviction(true, 1, p -> {
            IgniteInternalFuture fut = runAsync(() -> grid(0).cache(DEFAULT_CACHE_NAME).close());

            doSleep(500);
        }, this::preload);

        awaitPartitionMapExchange(true, true, null);
        assertPartitionsSame(idleVerify(grid(0), DEFAULT_CACHE_NAME));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeactivation_Volatile() throws Exception {
        AtomicReference<IgniteInternalFuture> ref = new AtomicReference<>();

        testOperationDuringEviction(false, 1, p -> {
            IgniteInternalFuture fut = runAsync(() -> grid(0).cluster().state(INACTIVE));

            ref.set(fut);

            doSleep(1000);

            assertFalse(fut.isDone());
        }, this::preload);

        ref.get().get();

        assertTrue(grid(0).cluster().state() == INACTIVE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeactivation_Persistence() throws Exception {
        AtomicReference<IgniteInternalFuture> ref = new AtomicReference<>();

        testOperationDuringEviction(true, 1, p -> {
            IgniteInternalFuture fut = runAsync(() -> grid(0).cluster().state(INACTIVE));

            ref.set(fut);

            doSleep(1000);

            assertFalse(fut.isDone());
        }, this::preload);

        ref.get().get();

        assertTrue(grid(0).cluster().state() == INACTIVE);
    }

    /** */
    @Test
    public void testEvictionMetrics() throws Exception {
        stats = true;

        testOperationDuringEviction(true, 1, p -> {
            CacheMetricsImpl metrics = grid(0).cachex(DEFAULT_CACHE_NAME).context().cache().metrics0();

            assertTrue(metrics.evictingPartitionsLeft() > 0);
        }, this::preload);

        awaitPartitionMapExchange(true, true, null);
        CacheMetricsImpl metrics = grid(0).cachex(DEFAULT_CACHE_NAME).context().cache().metrics0();
        assertTrue(metrics.evictingPartitionsLeft() == 0);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSysPoolStarvation() throws Exception {
        sysPoolSize = 1;

        testOperationDuringEviction(true, 1, p -> {
            try {
                grid(0).context().closure().runLocalSafe(new Runnable() {
                    @Override public void run() {
                        // No-op.
                    }
                }, true).get(5_000);
            } catch (IgniteCheckedException e) {
                fail(X.getFullStackTrace(e));
            }
        }, this::preload);

        awaitPartitionMapExchange(true, true, null);
        assertPartitionsSame(idleVerify(grid(0), DEFAULT_CACHE_NAME));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCacheGroupDestroy_Volatile() throws Exception {
        AtomicReference<IgniteInternalFuture> ref = new AtomicReference<>();

        testOperationDuringEviction(false, 1, p -> {
            IgniteInternalFuture fut = runAsync(new Runnable() {
                @Override public void run() {
                    grid(0).destroyCache(DEFAULT_CACHE_NAME);
                }
            });

            doSleep(500);

            assertFalse(fut.isDone()); // Cache stop should be blocked by concurrent unfinished eviction.

            ref.set(fut);
        }, this::preload);

        try {
            ref.get().get(10_000);
        }
        catch (IgniteFutureTimeoutCheckedException e) {
            fail(X.getFullStackTrace(e));
        }

        PartitionsEvictManager mgr = grid(0).context().cache().context().evict();

        // Group eviction context should remain in map.
        Map evictionGroupsMap = U.field(mgr, "evictionGroupsMap");

        assertFalse("Group context must be cleaned up", evictionGroupsMap.containsKey(CU.cacheId(DEFAULT_CACHE_NAME)));

        grid(0).getOrCreateCache(cacheConfiguration());

        assertEquals(2, evictionGroupsMap.size());

        assertPartitionsSame(idleVerify(grid(0), DEFAULT_CACHE_NAME));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStopNodeDuringEviction() throws Exception {
        AtomicReference<IgniteInternalFuture> ref = new AtomicReference<>();

        testOperationDuringEviction(false, 1, p -> {
            IgniteInternalFuture fut = runAsync(new Runnable() {
                @Override public void run() {
                    grid(0).close();
                }
            });

            doSleep(500);

            assertFalse(fut.isDone()); // Cache stop should be blocked by concurrent unfinished eviction.

            ref.set(fut);
        }, this::preload);

        try {
            ref.get().get(10_000);
        } catch (IgniteFutureTimeoutCheckedException e) {
            fail(X.getFullStackTrace(e));
        }

        waitForTopology(2);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStopNodeDuringEviction_2() throws Exception {
        AtomicInteger holder = new AtomicInteger();

        CountDownLatch l1 = new CountDownLatch(1);
        CountDownLatch l2 = new CountDownLatch(1);

        IgniteEx g0 = startGrid(0, new DependencyResolver() {
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
                            return new GridDhtLocalPartitionSyncEviction(ctx, grp, id, recovery, 3, l1, l2) {
                                /** */
                                @Override protected void sync() {
                                    if (holder.get() == id)
                                        super.sync();
                                }
                            };
                        }
                    });
                }
                else if (instance instanceof IgniteCacheOffheapManager) {
                    IgniteCacheOffheapManager mgr = (IgniteCacheOffheapManager) instance;

                    IgniteCacheOffheapManager spied = Mockito.spy(mgr);

                    Mockito.doAnswer(new Answer() {
                        @Override public Object answer(InvocationOnMock invocation) throws Throwable {
                            Object ret = invocation.callRealMethod();

                            // Wait is necessary here to guarantee test progress.
                            doSleep(2_000);

                            return ret;
                        }
                    }).when(spied).stop();

                    return (T) spied;
                }

                return instance;
            }
        });

        startGrid(1);

        awaitPartitionMapExchange();

        IgniteCache<Integer, Integer> cache = g0.getOrCreateCache(cacheConfiguration());

        int p0 = evictingPartitionsAfterJoin(g0, cache, 1).get(0);
        holder.set(p0);

        loadDataToPartition(p0, g0.name(), DEFAULT_CACHE_NAME, 5_000, 0, 3);

        startGrid(2);

        U.awaitQuiet(l1);

        GridDhtLocalPartition part = g0.cachex(DEFAULT_CACHE_NAME).context().topology().localPartition(p0);

        PartitionsEvictManager.PartitionEvictionTask task =
            g0.context().cache().context().evict().clearingTask(CU.cacheId(DEFAULT_CACHE_NAME), p0);

        IgniteInternalFuture<Void> finishFut = task.finishFuture();

        IgniteInternalFuture fut = runAsync(g0::close);

        // Give some time to execute cache store destroy.
        doSleep(500);

        l2.countDown();

        fut.get();

        // Partition clearing future should be finished with NodeStoppingException.
        assertTrue(finishFut.error().getMessage(),
            finishFut.error() != null && X.hasCause(finishFut.error(), NodeStoppingException.class));
    }

    /** */
    @Test
    public void testCheckpoint() throws Exception {
        testOperationDuringEviction(true, 1, p -> {
            doSleep(500);

            grid(0).context().cache().context().database().wakeupForCheckpoint("Forced checkpoint");

            doSleep(500);
        }, this::preload);

        awaitPartitionMapExchange(true, true, null);
        assertPartitionsSame(idleVerify(grid(0), DEFAULT_CACHE_NAME));
    }

    /** */
    @Test
    public void testRestart() throws Exception {
        persistence = true;

        AtomicReference<IgniteInternalFuture> ref = new AtomicReference<>();

        testOperationDuringEviction(true, 1, p -> {
            IgniteInternalFuture fut = runAsync(() -> stopAllGrids());

            ref.set(fut);
        }, this::preload);

        ref.get().get();

        IgniteEx crd = startGrids(3);
        crd.cluster().state(ACTIVE);

        awaitPartitionMapExchange();
    }

    /** */
    @Test
    @WithSystemProperty(key = "DEFAULT_TOMBSTONE_TTL", value = "500")
    @WithSystemProperty(key = "IGNITE_TTL_EXPIRE_BATCH_SIZE", value = "0") // Disable implicit clearing on cache op.
    @WithSystemProperty(key = "CLEANUP_WORKER_SLEEP_INTERVAL", value = "100000000") // Disable background cleanup.
    @WithSystemProperty(key = "IGNITE_UNWIND_THROTTLING_TIMEOUT", value = "0") // Disable unwind throttling.
    @WithSystemProperty(key = "IGNITE_SENSITIVE_DATA_LOGGING", value = "plain")
    public void testTombstoneCleanupInEvictingPartition_Volatile() throws Exception {
        doTestTombstoneCleanupInEvictingPartition(false);
    }

    /** */
    @Test
    @WithSystemProperty(key = "DEFAULT_TOMBSTONE_TTL", value = "500")
    @WithSystemProperty(key = "IGNITE_TTL_EXPIRE_BATCH_SIZE", value = "0") // Disable implicit clearing on cache op.
    @WithSystemProperty(key = "CLEANUP_WORKER_SLEEP_INTERVAL", value = "100000000") // Disable background cleanup.
    @WithSystemProperty(key = "IGNITE_UNWIND_THROTTLING_TIMEOUT", value = "0") // Disable unwind throttling.
    @WithSystemProperty(key = "IGNITE_SENSITIVE_DATA_LOGGING", value = "plain")
    public void testTombstoneCleanupInEvictingPartition_Persistence() throws Exception {
        doTestTombstoneCleanupInEvictingPartition(true);
    }

    /**
     * @param persistence Persistence.
     */
    private void doTestTombstoneCleanupInEvictingPartition(boolean persistence) throws Exception {
        int evicted = testOperationDuringEviction(persistence, 2, p -> {
            doSleep(600);

            GridCacheContext<Object, Object> ctx = grid(0).cachex(DEFAULT_CACHE_NAME).context();

            Deque<PendingRow> queue = grid(0).context().cache().context().evict().evictQueue(true);

            ctx.ttl().expire(queue.size());

            assertTrue("Expire should't leave garbage", queue.isEmpty());
        }, new Consumer<Integer>() {
            @Override public void accept(Integer p0) {
                IgniteEx g0 = grid(0);

                final int cnt = 1_000;

                IgniteCache<Object, Object> cache = g0.cache(DEFAULT_CACHE_NAME);

                List<Integer> keys = partitionKeys(cache, p0, cnt, 0);

                try (IgniteDataStreamer<Object, Object> ds = g0.dataStreamer(DEFAULT_CACHE_NAME)) {
                    for (Integer key : keys)
                        ds.addData(key, key);
                }

                for (Integer key : keys)
                    cache.remove(key);
            }
        });

        awaitPartitionMapExchange(true, true, null);
        assertPartitionsSame(idleVerify(grid(0), DEFAULT_CACHE_NAME));

        Deque<PendingRow> queue = grid(0).context().cache().context().evict().evictQueue(true);

        assertTrue(queue.isEmpty());

        if (!persistence) {
            assertTrue(grid(0).cachex(DEFAULT_CACHE_NAME).context().topology().
                localPartition(evicted).dataStore().pendingTree().isEmpty());
        }
    }

    /**
     * @param persistence {@code True} to use persistence.
     * @param mode        Mode: <ul><li>0 - block before clearing start</li>
     *                    <li>1 - block in the middle of clearing</li></ul>
     * @param c           A closure to run while eviction is blocked.
     * @return Evicted partition.
     * @throws Exception If failed.
     */
    protected int testOperationDuringEviction(boolean persistence, int mode, Consumer<Integer> c, Consumer<Integer> initC) throws Exception {
        this.persistence = persistence;

        AtomicInteger holder = new AtomicInteger();
        CountDownLatch l1 = new CountDownLatch(1);
        CountDownLatch l2 = new CountDownLatch(1);

        IgniteEx g0 = startGrid(0, new DependencyResolver() {
            @Override public <T> T resolve(T instance) {
                if (instance instanceof GridDhtPartitionTopologyImpl) {
                    GridDhtPartitionTopologyImpl top = (GridDhtPartitionTopologyImpl) instance;

                    top.partitionFactory(new GridDhtPartitionTopologyImpl.PartitionFactory() {
                        @Override public GridDhtLocalPartition create(GridCacheSharedContext ctx, CacheGroupContext grp, int id, boolean recovery) {
                            return new GridDhtLocalPartitionSyncEviction(ctx, grp, id, recovery, mode, l1, l2) {
                                /** */
                                @Override protected void sync() {
                                    if (holder.get() == id)
                                        super.sync();
                                }
                            };
                        }
                    });
                }

                return instance;
            }
        });

        startGrid(1);

        if (persistence)
            g0.cluster().state(ACTIVE);

        awaitPartitionMapExchange(true, true, null);

        IgniteCache<Integer, Integer> cache = g0.getOrCreateCache(cacheConfiguration());

        List<Integer> allEvicting = evictingPartitionsAfterJoin(g0, cache, 1024);
        int p0 = allEvicting.get(0);
        holder.set(p0);

        initC.accept(p0);

        IgniteEx joining = startGrid(2);

        if (persistence)
            resetBaselineTopology();

        assertTrue(U.await(l1, 30_000, TimeUnit.MILLISECONDS));

        c.accept(p0);

        l2.countDown();

        return p0;
    }

    /** */
    protected CacheConfiguration<Integer, Integer> cacheConfiguration() {
        return new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME).
            setCacheMode(CacheMode.PARTITIONED).
            setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC).
            setBackups(backups).
            setStatisticsEnabled(stats).
            setAffinity(new RendezvousAffinityFunction(false, persistence ? 64 : 1024));
    }

    /**
     * @param part Partition.
     */
    private void preload(Integer part) {
        IgniteEx g0 = grid(0);

        final int cnt = 5_000;

        List<Integer> keys = partitionKeys(g0.cache(DEFAULT_CACHE_NAME), part, cnt, 0);

        try (IgniteDataStreamer<Integer, Integer> ds = g0.dataStreamer(DEFAULT_CACHE_NAME)) {
            for (Integer key : keys)
                ds.addData(key, key);
        }
    }
}
