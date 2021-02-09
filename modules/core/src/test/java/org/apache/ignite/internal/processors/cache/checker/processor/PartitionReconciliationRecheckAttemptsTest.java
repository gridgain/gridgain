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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManagerImpl;
import org.apache.ignite.internal.processors.cache.checker.objects.RecheckRequest;
import org.apache.ignite.internal.processors.cache.checker.objects.ReconciliationResult;
import org.apache.ignite.internal.visor.checker.VisorPartitionReconciliationTaskArg;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static java.lang.Thread.sleep;
import static org.apache.ignite.internal.processors.cache.checker.processor.ReconciliationEventListener.WorkLoadStage.FINISHED;
import static org.apache.ignite.internal.processors.cache.checker.processor.ReconciliationEventListener.WorkLoadStage.READY;

/**
 * Tests count of calls the recheck process with different inputs.
 */
public class PartitionReconciliationRecheckAttemptsTest extends PartitionReconciliationAbstractTest {
    /** Nodes. */
    protected static final int NODES_CNT = 2;

    /** Crd server node. */
    protected IgniteEx ig;

    /** Client. */
    protected IgniteEx client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        CacheConfiguration ccfg = new CacheConfiguration();
        ccfg.setName(DEFAULT_CACHE_NAME);
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, 2));
        ccfg.setBackups(NODES_CNT - 1);
        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

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

        ig.cluster().active(true);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * Checks that only one call happened.
     */
    @Test
    public void testZeroAttemptMakeOnlyOneRecheck() {
        testRecheckCount(0);
    }

    /**
     * Checks that three additional calls happened.
     */
    @Test
    public void testThreeAdditionalAttempts() {
        testRecheckCount(3);
    }

    /**
     * Check that broken keys are excluded if they are repaired.
     */
    @Test
    public void testBrokenKeysWillFixedDuringRecheck() throws InterruptedException, IgniteInterruptedCheckedException {
        final ConcurrentMap<UUID, AtomicInteger> recheckAttempts = new ConcurrentHashMap<>();

        CountDownLatch waitToStartFirstLastRecheck = new CountDownLatch(1);
        CountDownLatch waitKeyReporation = new CountDownLatch(1);

        ReconciliationEventListenerProvider.defaultListenerInstance((stage, workload) -> {
            if (stage.equals(READY) && workload instanceof RecheckRequest) {
                int attempt = recheckAttempts.computeIfAbsent(workload.workloadChainId(), (key) -> new AtomicInteger(0)).incrementAndGet();

                if (attempt == 2)
                    waitToStartFirstLastRecheck.countDown();
            }

            if (waitToStartFirstLastRecheck.getCount() == 0) {
                try {
                    waitKeyReporation.await();
                }
                catch (InterruptedException ignore) {
                }
            }
        });

        for (int i = 0; i < 15; i++) {
            client.cache(DEFAULT_CACHE_NAME).put(i, i);

            simulateOutdatedVersionCorruption(grid(0).cachex(DEFAULT_CACHE_NAME).context(), i);
        }

        VisorPartitionReconciliationTaskArg.Builder builder = new VisorPartitionReconciliationTaskArg.Builder();
        builder.repair(false);
        builder.parallelism(1);
        builder.caches(Collections.singleton(DEFAULT_CACHE_NAME));
        builder.recheckAttempts(3);
        builder.recheckDelay(0);

        AtomicReference<ReconciliationResult> res = new AtomicReference<>();

        GridTestUtils.runMultiThreadedAsync(() -> res.set(partitionReconciliation(client, builder)), 1, "reconciliation");

        waitToStartFirstLastRecheck.await();

        for (int i = 0; i < 15; i++) // repair keys
            client.cache(DEFAULT_CACHE_NAME).put(i, i);

        waitKeyReporation.countDown();

        GridTestUtils.waitForCondition(() -> res.get() != null, 40_000);

        assertEquals(0, res.get().partitionReconciliationResult().inconsistentKeysCount());
    }

    @Test
    public void test1() throws Exception {

        for (int i = 100; i < 200; i++) {
            client.cache(DEFAULT_CACHE_NAME).put(i, i);
        }

//        doSleep(500);

        VisorPartitionReconciliationTaskArg.Builder builder = new VisorPartitionReconciliationTaskArg.Builder();
        builder.repair(false);
        builder.parallelism(1);
        builder.caches(Collections.singleton(DEFAULT_CACHE_NAME));
        builder.recheckAttempts(3);
        builder.recheckDelay(0);

        AtomicReference<ReconciliationResult> res = new AtomicReference<>();

        IgniteInternalFuture loadFut = GridTestUtils.runAsync(() -> {
            System.out.println("qdrvlikt loadFut");

            for (int i = 200; i < 700; i++) {
                client.cache(DEFAULT_CACHE_NAME).put(i, i);

                try {
                    sleep(10);
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

//            doSleep(500);

            for (int i = 0; i < 10; i++) {
                client.cache(DEFAULT_CACHE_NAME).put(i, i);
            }

            for (int i = 100; i < 200; i++) {
                client.cache(DEFAULT_CACHE_NAME).remove(i);
            }

            Map m = new HashMap();

            for (int i = 10; i < 20; i++) {
                m.put(i, i);
            }

            client.cache(DEFAULT_CACHE_NAME).putAll(m);

            try (Transaction transaction = client.transactions().txStart()) {
                client.cache(DEFAULT_CACHE_NAME).put(110, 110);
                client.cache(DEFAULT_CACHE_NAME).put(111, 111);
                client.cache(DEFAULT_CACHE_NAME).put(112, 112);
                client.cache(DEFAULT_CACHE_NAME).put(113, 113);
                client.cache(DEFAULT_CACHE_NAME).put(114, 114);
                transaction.commit();
            }

//            doSleep(2000);

            System.out.println("qfrbdiu loadFut");
        });

        GridTestUtils.runMultiThreadedAsync(() -> res.set(partitionReconciliation(client, builder)), 1, "reconciliation");

        GridTestUtils.waitForCondition(() -> res.get() != null, 40_000);

        loadFut.get();

//        doSleep(5000);



        Map<UUID, Long> map = ReconciliationResultCollector.Simple.partSizesMap.get(0);
            Collection<Long> values = map.values();
            Iterator<Long> iterator = values.iterator();

            assertTrue(iterator.next() == 300);
            assertTrue(iterator.next() == 300);

        map = ReconciliationResultCollector.Simple.partSizesMap.get(1);
            values = map.values();
            iterator = values.iterator();

            assertTrue(iterator.next() == 300);
            assertTrue(iterator.next() == 300);

        long delta00 = ((IgniteCacheOffheapManagerImpl.CacheDataStoreImpl)(internalCache(grid(0).cache(DEFAULT_CACHE_NAME)).context().topology().localPartition(0).dataStore())).reconciliationCtx().storageSizeDelta.get();
        long delta01 = ((IgniteCacheOffheapManagerImpl.CacheDataStoreImpl)(internalCache(grid(0).cache(DEFAULT_CACHE_NAME)).context().topology().localPartition(1).dataStore())).reconciliationCtx().storageSizeDelta.get();
        long delta10 = ((IgniteCacheOffheapManagerImpl.CacheDataStoreImpl)(internalCache(grid(1).cache(DEFAULT_CACHE_NAME)).context().topology().localPartition(0).dataStore())).reconciliationCtx().storageSizeDelta.get();
        long delta11 = ((IgniteCacheOffheapManagerImpl.CacheDataStoreImpl)(internalCache(grid(1).cache(DEFAULT_CACHE_NAME)).context().topology().localPartition(1).dataStore())).reconciliationCtx().storageSizeDelta.get();

        assertFalse(delta00 == 0);
        assertFalse(delta01 == 0);
        assertFalse(delta10 == 0);
        assertFalse(delta11 == 0);

        assertTrue(300+300+delta00+delta01 == client.cache(DEFAULT_CACHE_NAME).size());
        assertTrue(300+300+delta10+delta11 == client.cache(DEFAULT_CACHE_NAME).size());

        System.out.println();
//        assertEquals(0, res.get().partitionReconciliationResult().inconsistentKeysCount());
//        org.apache.ignite.internal.processors.cache.checker.processor.ReconciliationResultCollector.Simple.partSizesMap
//        internalCache(grid(0).cache(DEFAULT_CACHE_NAME)).context().topology().localPartition(0)
//        PartitionReconciliationProcessor#execute
//        CollectPartitionKeysByBatchTask.CollectPartitionKeysByBatchJob.execute0
    }

    /**
     *
     */
    private void testRecheckCount(int attempts) {
        final ConcurrentMap<UUID, AtomicInteger> recheckAttempts = new ConcurrentHashMap<>();

        ReconciliationEventListenerProvider.defaultListenerInstance((stage, workload) -> {
            if (stage.equals(FINISHED) && workload instanceof RecheckRequest)
                recheckAttempts.computeIfAbsent(workload.workloadChainId(), (key) -> new AtomicInteger(0)).incrementAndGet();
        });

        for (int i = 0; i < 15; i++) {
            client.cache(DEFAULT_CACHE_NAME).put(i, i);

            simulateOutdatedVersionCorruption(grid(0).cachex(DEFAULT_CACHE_NAME).context(), i);
        }

        VisorPartitionReconciliationTaskArg.Builder builder = new VisorPartitionReconciliationTaskArg.Builder();
        builder.repair(false);
        builder.parallelism(1);
        builder.caches(Collections.singleton(DEFAULT_CACHE_NAME));
        builder.recheckAttempts(attempts);
        builder.recheckDelay(0);

        ReconciliationResult res = partitionReconciliation(client, builder);

        assertEquals(15, res.partitionReconciliationResult().inconsistentKeysCount());

        for (Map.Entry<UUID, AtomicInteger> entry : recheckAttempts.entrySet())
            assertEquals("Session: " + entry.getKey() + " has wrong value: " + entry.getValue().get(), 1 + attempts, entry.getValue().get());
    }
}
