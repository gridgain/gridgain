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

package org.apache.ignite.internal.processors.cache.checker.processor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.checker.objects.RecheckRequest;
import org.apache.ignite.internal.processors.cache.checker.objects.ReconciliationResult;
import org.apache.ignite.internal.visor.checker.VisorPartitionReconciliationTaskArg;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ThrowUp;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.internal.processors.cache.checker.processor.PartitionReconciliationProcessor.ERROR_REASON;
import static org.apache.ignite.internal.processors.cache.checker.processor.PartitionReconciliationProcessor.TOPOLOGY_CHANGE_MSG;
import static org.apache.ignite.internal.processors.cache.checker.processor.ReconciliationEventListener.WorkLoadStage.FINISHING;

/**
 *
 */
@RunWith(Parameterized.class)
public class PartitionReconciliationInterruptionTest extends PartitionReconciliationAbstractTest {
    /**
     *
     */
    private static final String GRID_SHUTDOWN_MSG = "Failed to execute task due to grid shutdown";

    /** Nodes. */
    private static final int NODES_CNT = 4;

    /** Corrupted keys count. */
    private static final int BROKEN_KEYS_CNT = 500;

    /** Cache atomicity mode. */
    @Parameterized.Parameter(0)
    public CacheAtomicityMode cacheAtomicityMode;

    /** Cache atomicity mode. */
    @Parameterized.Parameter(1)
    public boolean persistence;

    /** Crd server node. */
    private IgniteEx ig;

    /** Client. */
    private IgniteEx client;

    /** Node to node id. */
    private Map<Integer, String> nodeToNodeId = new HashMap<>();

    /** Batch size. */
    private int batchSize = 1;

    /**
     *
     */
    @Parameterized.Parameters(name = "atomicity = {0}, persistence = {1}")
    public static List<Object[]> parameters() {
        ArrayList<Object[]> params = new ArrayList<>();

        CacheAtomicityMode[] atomicityModes = new CacheAtomicityMode[] {
            CacheAtomicityMode.ATOMIC, CacheAtomicityMode.TRANSACTIONAL};

        for (CacheAtomicityMode atomicityMode : atomicityModes) {
            params.add(new Object[] {atomicityMode, true});
            params.add(new Object[] {atomicityMode, false});
        }

        return params;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(persistence)
                .setMaxSize(300L * 1024 * 1024))
        );

        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setName(DEFAULT_CACHE_NAME);
        ccfg.setAtomicityMode(cacheAtomicityMode);
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, 10));
        ccfg.setBackups(NODES_CNT - 1);

        cfg.setCacheConfiguration(ccfg);
        cfg.setConsistentId(name);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        ig = startGrids(NODES_CNT);

        client = startClientGrid(NODES_CNT);

        for (int i = 0; i < NODES_CNT; i++)
            nodeToNodeId.put(i, grid(i).localNode().id().toString());

        ig.cluster().active(true);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        nodeToNodeId.clear();
    }

    /**
     *
     */
    @Test
    public void testStopNodeDuringCheck() throws Exception {
        interruptionDuringCheck(() -> stopGrid(2), res -> {
            assertFalse(res.partitionReconciliationResult().isEmpty());

            assertErrorMsg(res, 0, TOPOLOGY_CHANGE_MSG);
            assertErrorMsg(res, 1, TOPOLOGY_CHANGE_MSG);
            assertErrorMsg(res, 2, GRID_SHUTDOWN_MSG);
        }, false);
    }

    /**
     *
     */
    @Test
    public void testStartNewNodeDuringCheck() throws Exception {
        interruptionDuringCheck(() -> startGrid(5), res -> {
            assertFalse(res.partitionReconciliationResult().isEmpty());

            assertErrorMsg(res, 0, TOPOLOGY_CHANGE_MSG);
            assertErrorMsg(res, 1, TOPOLOGY_CHANGE_MSG);
            assertErrorMsg(res, 2, TOPOLOGY_CHANGE_MSG);
        }, false);
    }

    /**
     *
     */
    @Test
    public void testStartNewClientNodeDuringCheck() throws Exception {
        batchSize = BROKEN_KEYS_CNT / 3;

        interruptionDuringCheck(() -> startClientGrid(5), res -> {
            assertFalse(res.partitionReconciliationResult().isEmpty());

            assertTrue(res.errors().isEmpty());
        }, false);
    }

    /**
     *
     */
    @Test
    public void testStopClientNodeDuringCheck() throws Exception {
        batchSize = BROKEN_KEYS_CNT / 3;

        startClientGrid(5);

        interruptionDuringCheck(() -> stopGrid(5), res -> {
            assertFalse(res.partitionReconciliationResult().isEmpty());

            assertTrue(res.errors().isEmpty());
        }, false);
    }

    /**
     *
     */
    @Test
    public void testStartNewThinClientNodeDuringCheck() throws Exception {
        batchSize = BROKEN_KEYS_CNT / 3;

        interruptionDuringCheck(() -> Ignition.startClient(new ClientConfiguration().setAddresses("127.0.0.1:10800")), res -> {
            assertFalse(res.partitionReconciliationResult().isEmpty());

            assertTrue(res.errors().isEmpty());
        }, false);
    }

    /**
     *
     */
    @Test
    public void testStopThinClientNodeDuringCheck() throws Exception {
        batchSize = BROKEN_KEYS_CNT / 3;

        IgniteClient client = Ignition.startClient(new ClientConfiguration().setAddresses("127.0.0.1:10800"));

        interruptionDuringCheck(client::close, res -> {
            assertFalse(res.partitionReconciliationResult().isEmpty());

            assertTrue(res.errors().isEmpty());
        }, false);
    }

    /**
     *
     */
    @Test
    public void testCreateCacheDuringCheck() throws Exception {
        interruptionDuringCheck(() -> client.createCache("SOME_CACHE"), res -> {
            assertFalse(res.partitionReconciliationResult().isEmpty());

            assertErrorMsg(res, 0, TOPOLOGY_CHANGE_MSG);
            assertErrorMsg(res, 1, TOPOLOGY_CHANGE_MSG);
            assertErrorMsg(res, 2, TOPOLOGY_CHANGE_MSG);
        }, false);
    }

    /**
     *
     */
    @Test
    public void testRemoveNotProcessedCacheDuringCheck() throws Exception {
        IgniteCache<Object, Object> notProcessedCache = client.createCache("SOME_CACHE");
        interruptionDuringCheck(notProcessedCache::destroy, res -> {
            assertFalse(res.partitionReconciliationResult().isEmpty());

            assertErrorMsg(res, 0, TOPOLOGY_CHANGE_MSG);
            assertErrorMsg(res, 1, TOPOLOGY_CHANGE_MSG);
            assertErrorMsg(res, 2, TOPOLOGY_CHANGE_MSG);
        }, false);
    }

    /**
     *
     */
    @Test
    public void testRemoveProcessedCacheDuringCheck() throws Exception {
        final String cacheRemoveError = String.format(ERROR_REASON, null, "class java.lang.NullPointerException");
        interruptionDuringCheck(() -> client.cache(DEFAULT_CACHE_NAME).destroy(), res -> {
            assertFalse(res.partitionReconciliationResult().isEmpty());

            assertEquals(res.errors().size(), 4);
        }, true);
    }

    /**
     *
     */
    private void assertErrorMsg(ReconciliationResult res, int nodeId, String errorMsg) {
        String nodeIdStr = nodeToNodeId.get(nodeId);

        for (String error : res.errors()) {
            if (error.startsWith(nodeIdStr) && error.contains(errorMsg))
                return;
        }

        fail("Expected message [msg=" + errorMsg + "] not found for node: " + nodeIdStr);
    }

    /**
     *
     */
    private <E extends Throwable> void interruptionDuringCheck(ThrowUp<E> act,
        Consumer<ReconciliationResult> assertions,
        boolean waitOnProcessing) throws E, InterruptedException, IgniteInterruptedCheckedException {
        CountDownLatch firstRecheckFinished = new CountDownLatch(1);
        CountDownLatch waitInTask = new CountDownLatch(1);
        CountDownLatch waitOnProcessingBeforeAction = new CountDownLatch(1);

        ReconciliationEventListenerFactory.setDefaultInstance((stage, workload) -> {
            if (firstRecheckFinished.getCount() == 0) {
                try {
                    waitInTask.await();

                    if (waitOnProcessing)
                        waitOnProcessingBeforeAction.await();
                    else
                        Thread.sleep(1_000);
                }
                catch (InterruptedException ignore) {
                }
            }

            if (stage.equals(FINISHING) && workload instanceof RecheckRequest)
                firstRecheckFinished.countDown();
        });

        GridCacheContext[] nodeCacheCtxs = new GridCacheContext[NODES_CNT];

        for (int i = 0; i < NODES_CNT; i++)
            nodeCacheCtxs[i] = grid(i).cachex(DEFAULT_CACHE_NAME).context();

        for (int i = 0; i < BROKEN_KEYS_CNT; i++) {
            client.cache(DEFAULT_CACHE_NAME).put(i, i);

            simulateOutdatedVersionCorruption(nodeCacheCtxs[i % NODES_CNT], i);
        }

        VisorPartitionReconciliationTaskArg.Builder builder = new VisorPartitionReconciliationTaskArg.Builder();
        builder.fixMode(false);
        builder.batchSize(batchSize);
        builder.loadFactor(0.0001);
        builder.caches(Collections.singleton(DEFAULT_CACHE_NAME));
        builder.console(true);
        builder.recheckAttempts(0);

        final AtomicReference<ReconciliationResult> res = new AtomicReference<>();

        GridTestUtils.runMultiThreadedAsync(() -> {
            try {
                res.set(partitionReconciliation(client, builder));
            }
            catch (Exception e) {
                log.error("Test failed.", e);
            }
        }, 1, "partitionReconciliation");

        firstRecheckFinished.await();

        waitInTask.countDown();

        act.run();

        waitOnProcessingBeforeAction.countDown();

        assertTrue(GridTestUtils.waitForCondition(() -> res.get() != null, 20_000));

        assertions.accept(res.get());
    }
}
