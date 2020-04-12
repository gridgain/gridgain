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

package org.apache.ignite.internal.processors.cache.persistence.db;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheDataRemoveInProgressException;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.metric.IoStatisticsHolder;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.pendingtask.DurableBackgroundTask;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageHandler;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.internal.visor.cache.VisorFindAndDeleteGarbageInPersistenceJobResult;
import org.apache.ignite.internal.visor.cache.VisorFindAndDeleteGarbageInPersistenceTask;
import org.apache.ignite.internal.visor.cache.VisorFindAndDeleteGarbageInPersistenceTaskArg;
import org.apache.ignite.internal.visor.cache.VisorFindAndDeleteGarbageInPersistenceTaskResult;
import org.apache.ignite.testframework.CallbackExecutorLogListener;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.thread.IgniteThread;
import org.junit.Test;

import static java.util.Collections.singleton;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager.IGNITE_PDS_SKIP_CHECKPOINT_ON_NODE_STOP;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/**
 * Class for testing to clear contents of cache(after destroy) through
 * {@link DurableBackgroundTask}.
 */
@WithSystemProperty(key = IGNITE_PDS_SKIP_CHECKPOINT_ON_NODE_STOP, value = "true")
public class LongDestroyCacheDurableBackgroundTaskTest extends GridCommonAbstractTest {
    /** */
    private static final String SHARED_GROUP_NAME = "sg";

    private static final String SHARED_GROUP_CACHE = SHARED_GROUP_NAME + "Cache";

    private static final String SEPARATE_CACHE_NAME = "separateCache";

    private static final String SHARED_GROUP_CACHE_A = "A";

    private static final String SHARED_GROUP_CACHE_B = "B";

    /**
     * Nodes count.
     */
    private static final int NODES_COUNT = 2;

    /**
     * Number of node that can be restarted during test, if test scenario requires it.
     */
    private static final int RESTARTED_NODE_NUM = 0;

    /**
     * Number of node that is always alive during tests.
     */
    private static final int ALWAYS_ALIVE_NODE_NUM = 1;

    private volatile boolean doDelay = false;

    /**
     * Latch that waits for execution of durable background task.
     */
    private CountDownLatch removeOnAllNodesLatch;

    private CountDownLatch removeOnOneNodeLatch;

    private CountDownLatch backgroundDeletionStartedLatch;

    /**
     * When it is set to true during index deletion, node with number {@link #RESTARTED_NODE_NUM} fails to complete
     * deletion.
     */
    private final AtomicBoolean blockRemoval = new AtomicBoolean(false);

    /**
     *
     */
    private final LogListener pendingDelFinishedLsnr =
        new CallbackExecutorLogListener(".*?Execution of durable background task completed.*", () -> {
            removeOnOneNodeLatch.countDown();
            removeOnAllNodesLatch.countDown();
        });

    /**
     *
     */
    private final ListeningTestLogger testLog = new ListeningTestLogger(
        false,
        log(),
        pendingDelFinishedLsnr
    );

    /**
     * {@inheritDoc}
     */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        BPlusTree.testHndWrapper = (tree, hnd) -> {
            PageHandler<Object, BPlusTree.Result> delegate = (PageHandler<Object, BPlusTree.Result>)hnd;

            return new PageHandler<Object, BPlusTree.Result>() {
                @Override public BPlusTree.Result run(
                    int cacheId,
                    long pageId,
                    long page,
                    long pageAddr,
                    PageIO io,
                    Boolean walPlc,
                    Object arg,
                    int intArg,
                    IoStatisticsHolder statHolder
                ) throws IgniteCheckedException {
                    if (doDelay && cacheId == CU.cacheId(SHARED_GROUP_NAME)) {
                        doSleep(50);

                        backgroundDeletionStartedLatch.countDown();

                        if (Thread.currentThread() instanceof IgniteThread) {
                            IgniteThread thread = (IgniteThread)Thread.currentThread();

                            if (thread.getIgniteInstanceName().endsWith(String.valueOf(RESTARTED_NODE_NUM))
                                && blockRemoval.compareAndSet(true, false))
                                throw new RuntimeException("Aborting destroy (test).");
                        }
                    }

                    return delegate.run(cacheId, pageId, page, pageAddr, io, walPlc, arg, intArg, statHolder);
                }
            };
        };

        doDelay = false;

        blockRemoval.set(false);

        removeOnAllNodesLatch = new CountDownLatch(NODES_COUNT);

        removeOnOneNodeLatch = new CountDownLatch(1);

        backgroundDeletionStartedLatch = new CountDownLatch(2);

        cleanPersistenceDir();
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        BPlusTree.testHndWrapper = null;

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * {@inheritDoc}
     */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(
                new DataStorageConfiguration().setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                        .setInitialSize(10 * 1024 * 1024)
                )
                .setCheckpointFrequency(Long.MAX_VALUE / 2)
            )
            .setClientMode(igniteInstanceName.contains("client"))
            .setGridLogger(testLog);
    }

    private <K, V> CacheConfiguration<K, V> separateCacheConfig(String cache) {
        return new CacheConfiguration<K, V>()
            .setName(cache)
            .setAffinity(new RendezvousAffinityFunction(false, 100));
    }

    private <K, V> CacheConfiguration<K, V> sharedGroupCacheConfig(String cachePostfix) {
        return new CacheConfiguration<K, V>()
            .setName(SHARED_GROUP_CACHE + cachePostfix)
            .setGroupName(SHARED_GROUP_NAME)
            .setAffinity(new RendezvousAffinityFunction(false, 100));
    }

    private void populateCache(IgniteCache<Integer, Integer> cache) {
        int recCnt = 200;

        for (int i = 0; i < recCnt; i++)
            cache.put(i, i + 1);

        // Check data.
        for (int i = 0; i < recCnt; i++)
            assertEquals(i + 1, cache.get(i).intValue());
    }

    private void testLongCacheDestroy(
        boolean withRestart,
        boolean withRebalance,
        boolean createCacheWhenOneNodeStopped,
        boolean dropCacheAfterCreateWhenOneNodeStopped
    ) throws Exception {
        int nodeCnt = NODES_COUNT;

        IgniteEx ignite = startGrids(NODES_COUNT);

        final Ignite ignite0 = ignite;

        IgniteEx restartingNode = grid(RESTARTED_NODE_NUM);

        GridCacheProcessor cacheProc = restartingNode.context().cache();

        Ignite aliveNode = grid(ALWAYS_ALIVE_NODE_NUM);

        ignite.cluster().active(true);

        IgniteCache<Integer, Integer> separateCache = ignite.getOrCreateCache(separateCacheConfig(SEPARATE_CACHE_NAME));

        IgniteCache<Integer, Integer> sgCacheA = ignite.getOrCreateCache(sharedGroupCacheConfig(SHARED_GROUP_CACHE_A));
        IgniteCache<Integer, Integer> sgCacheB = ignite.getOrCreateCache(sharedGroupCacheConfig(SHARED_GROUP_CACHE_B));

        populateCache(separateCache);
        populateCache(sgCacheA);
        populateCache(sgCacheB);

        doDelay = true;

        // Destroy caches.
        ExecutorService asyncExecutor = Executors.newFixedThreadPool(2);

        Future separateCacheDestroyFut = asyncExecutor.submit(separateCache::destroy);
        Future sgCacheADestroyFut = asyncExecutor.submit(sgCacheA::destroy);

        awaitLatch(backgroundDeletionStartedLatch, "Test timed out: Failed to await for deletion start.");

        forceCheckpoint();

        VisorFindAndDeleteGarbageInPersistenceJobResult findGarbageRes = findGarbage(ignite, SHARED_GROUP_NAME);

        assertTrue(findGarbageRes.hasGarbage());

        // Waiting for cache destroy operations finished (background data deletion still in progress).
        separateCacheDestroyFut.get();
        sgCacheADestroyFut.get();

        // Trying to re-create caches with the same name.
        IgniteCache<Integer, Integer> recreatedSeparateCache = ignite.createCache(separateCacheConfig(SEPARATE_CACHE_NAME));

        // Creation of cache A in shared group should fail.
        assertThrows(() -> ignite0.createCache(sharedGroupCacheConfig(SHARED_GROUP_CACHE_A)), CacheDataRemoveInProgressException.class);

        // Checking that re-created cache is ok.
        populateCache(recreatedSeparateCache);

        if (withRebalance) {
            startGrid(NODES_COUNT);

            nodeCnt++;

            Collection<ClusterNode> blt = IntStream.range(0, nodeCnt)
                .mapToObj(i -> grid(i).localNode())
                .collect(toList());

            ignite.cluster().setBaselineTopology(blt);
        }

        if (withRestart) {
            blockRemoval.set(true);

            stopGrid(RESTARTED_NODE_NUM);

            awaitPartitionMapExchange();

            if (createCacheWhenOneNodeStopped) {
                IgniteCache<Integer, Integer> recreatedCacheOnAliveNode =
                    aliveNode.createCache(sharedGroupCacheConfig(SHARED_GROUP_CACHE_A));

                if (dropCacheAfterCreateWhenOneNodeStopped) {
                    removeOnOneNodeLatch = new CountDownLatch(1);

                    recreatedCacheOnAliveNode.destroy();

                    awaitLatch(
                        removeOnOneNodeLatch,
                        "Test timed out: failed to await for durable background task completion on at least one node."
                    );
                }

                doDelay = false;

                aliveNode.cluster().active(false);

                doDelay = true;
            }

            removeOnOneNodeLatch = new CountDownLatch(1);

            ignite = startGrid(RESTARTED_NODE_NUM);

            awaitLatch(
                removeOnOneNodeLatch,
                "Test timed out: failed to await for durable background task completion on at least one node."
            );

            awaitPartitionMapExchange();

            if (createCacheWhenOneNodeStopped) {
                ignite.cluster().active(true);
            }
        }

        awaitLatch(removeOnAllNodesLatch, "Test timed out: failed to await for durable background task completion.");

        asyncExecutor.shutdown();

        //assertFalse(findGarbage(grid(RESTARTED_NODE_NUM), SHARED_GROUP_NAME).hasGarbage());
        //assertFalse(findGarbage(grid(ALWAYS_ALIVE_NODE_NUM), SHARED_GROUP_NAME).hasGarbage());

        // Checkt that now we can successfully create cache.
        doDelay = false;

        sgCacheA = ignite.getOrCreateCache(sharedGroupCacheConfig(SHARED_GROUP_CACHE_A));

        populateCache(sgCacheA);

        awaitPartitionMapExchange();
    }

    /**
     *
     */
    @Test
    public void testSimple() throws Exception {
        testLongCacheDestroy(false, false, false, false);
    }

    /**
     *
     */
    @Test
    public void testWithRestart() throws Exception {
        testLongCacheDestroy(true, false, false, false);
    }

    /**
     *
     */
    @Test
    public void testWithRebalance() throws Exception {
        testLongCacheDestroy(false, true, false, false);
    }

    /**
     *
     */
    @Test
    public void testWithRestartAndCreateCacheWhenOneNodeStopped() throws Exception {
        testLongCacheDestroy(true, false, true, false);
    }

    /**
     *
     */
    @Test
    public void testWithRestartAndCreateAndDeleteCacheWhenOneNodeStopped() throws Exception {
        testLongCacheDestroy(true, false, true, true);
    }

    /**
     * Awaits for latch for 60 seconds and fails, if latch was not counted down.
     *
     * @param latch   Latch.
     * @param failMsg Failure message.
     * @throws InterruptedException If waiting failed.
     */
    private void awaitLatch(CountDownLatch latch, String failMsg) throws InterruptedException {
        if (!latch.await(60, TimeUnit.SECONDS))
            fail(failMsg);
    }

    /**
     * First test.
     *
     * @throws Exception If failed.
     */
    @Test
    public void test0() throws Exception {
        IgniteEx crd = startGrids(1);

        String cacheName = DEFAULT_CACHE_NAME;
        String grpName = "group";

        CacheConfiguration<Object, Object> cacheCfg0 = new CacheConfiguration<>(cacheName).setGroupName(grpName);
        CacheConfiguration<Object, Object> cacheCfg1 = new CacheConfiguration<>(cacheName + "1").setGroupName(grpName);

        createAndPopulateCaches(crd, cacheCfg0/*, cacheCfg1*/);

        crd.destroyCache(cacheName);
        assertNull(crd.cachex(cacheName));

        int stopNodeIdx = 0;
        //stopGrid(stopNodeIdx);
        // = startGrid(stopNodeIdx);

        assertNull(crd.cachex(cacheName));

        //VisorFindAndDeleteGarbageInPersistenceJobResult findGarbageRes = findGarbage(crd, grpName);
        //assertTrue(findGarbageRes.hasGarbage());

        // TODO: 01.03.2020 wait clear through DurableBackgroundTask

        //findGarbageRes = findGarbage(crd, grpName);
        //assertFalse(findGarbageRes.hasGarbage());
    }

    /**
     * Create and populate caches.
     *
     * @param node      Node.
     * @param cacheCfgs Cache configurations.
     */
    private void createAndPopulateCaches(IgniteEx node, CacheConfiguration<Object, Object>... cacheCfgs) {
        requireNonNull(node);
        requireNonNull(cacheCfgs);

        node.cluster().active(true);

        ThreadLocalRandom rand = ThreadLocalRandom.current();

        for (CacheConfiguration<Object, Object> cacheCfg : cacheCfgs) {
            IgniteCache<Object, Object> cache = node.getOrCreateCache(cacheCfg);

            for (int i = 0; i < 50; i++)
                cache.put(i, rand.nextLong());
        }
    }

    /**
     * Search for garbage in an existing cache group.
     *
     * @param node    Node.
     * @param grpName Cache group name.
     * @return Garbage search result.
     */
    private VisorFindAndDeleteGarbageInPersistenceJobResult findGarbage(IgniteEx node, String grpName) {
        requireNonNull(node);
        requireNonNull(grpName);

        UUID nodeId = node.localNode().id();

        VisorFindAndDeleteGarbageInPersistenceTaskResult findGarbageTaskRes = node.compute().execute(
            VisorFindAndDeleteGarbageInPersistenceTask.class,
            new VisorTaskArgument<>(
                nodeId,
                new VisorFindAndDeleteGarbageInPersistenceTaskArg(singleton(grpName), false, null),
                true
            )
        );

        assertTrue(findGarbageTaskRes.exceptions().isEmpty());

        return findGarbageTaskRes.result().get(nodeId);
    }
}
