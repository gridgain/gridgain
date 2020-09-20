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

package org.apache.ignite.internal.processors.cache.distributed.near;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.GridKernalContextImpl;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheConcurrentMap;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheLocalConcurrentMap;
import org.apache.ignite.internal.processors.cache.GridCacheMapEntry;
import org.apache.ignite.internal.processors.cache.GridCacheMvccCandidate;
import org.apache.ignite.internal.processors.cache.distributed.GridCacheAbstractNodeRestartSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.internal.processors.cache.transactions.TransactionProxyImpl;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.ASYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.testframework.GridTestUtils.mergeExchangeWaitVersion;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;

/**
 * Test node restart.
 * TODO rename touch param in puifobsolete
 */
@WithSystemProperty(key = "IGNITE_DIAGNOSTIC_ENABLED", value = "false")
public class GridCachePartitionedOptimisticTxNodeRestartTest extends GridCacheAbstractNodeRestartSelfTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        cfg.setFailureDetectionTimeout(1000000L);

        cfg.setConsistentId(igniteInstanceName);

        cfg.getTransactionConfiguration().setDefaultTxConcurrency(OPTIMISTIC);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration() {
        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setName(CACHE_NAME);
        cc.setCacheMode(PARTITIONED);
        cc.setWriteSynchronizationMode(FULL_SYNC);
        cc.setRebalanceMode(rebalancMode);
        cc.setRebalanceBatchSize(rebalancBatchSize);
        cc.setAffinity(new RendezvousAffinityFunction(false, partitions));
        cc.setBackups(backups);

        cc.setNearConfiguration(nearEnabled() ? new NearCacheConfiguration() : null);

        return cc;
    }

    /**
     * @return {@code True} if near cache enabled.
     */
    protected boolean nearEnabled() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected TransactionConcurrency txConcurrency() {
        return OPTIMISTIC;
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testRestart() throws Exception {
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testRestartWithPutTwoNodesNoBackups() throws Throwable {
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testRestartWithPutTwoNodesOneBackup() throws Throwable {
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testRestartWithPutFourNodesNoBackups() throws Throwable {
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testRestartWithPutFourNodesOneBackups() throws Throwable {
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testRestartWithPutSixNodesTwoBackups() throws Throwable {
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testRestartWithPutEightNodesTwoBackups() throws Throwable {
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testRestartWithPutTenNodesTwoBackups() throws Throwable {
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testRestartWithTxEightNodesTwoBackups() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testRestartWithTxFourNodesNoBackups() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testRestartWithTxFourNodesOneBackups() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testRestartWithTxSixNodesTwoBackups() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testRestartWithTxFourNodesTwoBackups() throws Throwable {
        super.testRestartWithTxFourNodesOneBackups();
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testRestartWithTxTenNodesTwoBackups() throws Throwable {
        super.testRestartWithTxFourNodesOneBackups();
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testRestartWithTxTwoNodesNoBackups() throws Throwable {
        super.testRestartWithTxFourNodesOneBackups();
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testRestartWithTxTwoNodesOneBackup() throws Throwable {
        super.testRestartWithTxFourNodesOneBackups();
    }



    /** {@inheritDoc} */
    @Test
    public void testRestartWithTxEightNodesTwoBackups_1() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }

    /** {@inheritDoc} */
    @Test
    public void testRestartWithTxFourNodesNoBackups_1() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }

    /** {@inheritDoc} */
    @Test
    public void testRestartWithTxFourNodesOneBackups_1() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }

    /** {@inheritDoc} */
    @Test
    public void testRestartWithTxSixNodesTwoBackups_1() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }

    /** {@inheritDoc} */
    @Test
    public void testRestartWithTxFourNodesTwoBackups_1() throws Throwable {
        super.testRestartWithTxFourNodesOneBackups();
    }

    /** {@inheritDoc} */
    @Test
    public void testRestartWithTxTenNodesTwoBackups_1() throws Throwable {
        super.testRestartWithTxFourNodesOneBackups();
    }

    /** {@inheritDoc} */
    @Test
    public void testRestartWithTxTwoNodesNoBackups_1() throws Throwable {
        super.testRestartWithTxFourNodesOneBackups();
    }

    /** {@inheritDoc} */
    @Test
    public void testRestartWithTxTwoNodesOneBackup_1() throws Throwable {
        super.testRestartWithTxFourNodesOneBackups();
    }





    /** {@inheritDoc} */
    @Test
    public void testRestartWithTxEightNodesTwoBackups_2() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }

    /** {@inheritDoc} */
    @Test
    public void testRestartWithTxFourNodesNoBackups_2() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }

    /** {@inheritDoc} */
    @Test
    public void testRestartWithTxFourNodesOneBackups_2() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }

    /** {@inheritDoc} */
    @Test
    public void testRestartWithTxSixNodesTwoBackups_2() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }

    /** {@inheritDoc} */
    @Test
    public void testRestartWithTxFourNodesTwoBackups_2() throws Throwable {
        super.testRestartWithTxFourNodesOneBackups();
    }

    /** {@inheritDoc} */
    @Test
    public void testRestartWithTxTenNodesTwoBackups_2() throws Throwable {
        super.testRestartWithTxFourNodesOneBackups();
    }

    /** {@inheritDoc} */
    @Test
    public void testRestartWithTxTwoNodesNoBackups_2() throws Throwable {
        super.testRestartWithTxFourNodesOneBackups();
    }

    /** {@inheritDoc} */
    @Test
    public void testRestartWithTxTwoNodesOneBackup_2() throws Throwable {
        super.testRestartWithTxFourNodesOneBackups();
    }

    @Test
    @Ignore
    public void testZzz3() throws Exception {
        backups = 1;
        nodeCnt = 4;
        keyCnt = 10;
        partitions = 128;
        rebalancMode = ASYNC;
        evict = false;

        IgniteEx crd = startGrids(2);
        awaitPartitionMapExchange(true, true, null);

        startGrid(2);
        startGrid(3);
        awaitPartitionMapExchange(true, true, null);

        int cand = -1;

        IgniteEx grid1 = grid(1);

        for (int p = 0; p < partitions; p++) {
            if (!crd.affinity(CACHE_NAME).isPrimaryOrBackup(grid1.cluster().localNode(), p)) {
                cand = p;

                break;
            }
        }

        assert cand != -1;

        // Create near reader on grid1.
        try (Transaction tx = grid1.transactions().txStart(OPTIMISTIC, TransactionIsolation.REPEATABLE_READ)) {
            IgniteCache<Object, Object> nearCache = grid1.cache(CACHE_NAME);

            nearCache.put(cand, 0);

            tx.commit();
        }

        TestRecordingCommunicationSpi.spi(grid(0)).blockMessages(GridDhtPartitionsFullMessage.class, grid(1).name());

        final List<DiscoveryEvent> mergedEvts = new ArrayList<>();

        mergeExchangeWaitVersion(crd, 6, mergedEvts);

        stopGrid(getTestIgniteInstanceName(2), true, false);
        stopGrid(getTestIgniteInstanceName(3), true, false);

        TestRecordingCommunicationSpi.spi(grid(0)).waitForBlocked();

        Collection<ClusterNode> owners = grid(0).affinity(CACHE_NAME).mapKeyToPrimaryAndBackups(cand);

        assertEquals(grid(0).localNode(), owners.iterator().next());

        int finalCand = cand;
        IgniteInternalFuture<?> fut = multithreadedAsync(new Runnable() {
            @Override public void run() {
                // Create near reader on grid1.
                try (Transaction tx = grid(0).transactions().txStart(OPTIMISTIC, TransactionIsolation.REPEATABLE_READ)) {
                    IgniteCache<Object, Object> dhtCache = grid(0).cache(CACHE_NAME);

                    dhtCache.put(finalCand, 1);

                    tx.commit();
                }
            }
        }, 1);

        TestRecordingCommunicationSpi.spi(grid(0)).stopBlock();

        fut.get();

        doSleep(100000);
    }

    @Test
    @Ignore
    public void testCommitReorderBackup() throws Exception {
        backups = 2;
        nodeCnt = 5;
        keyCnt = 10;
        partitions = 128;
        rebalancMode = ASYNC;
        evict = false;

        IgniteEx crd = startGrids(nodeCnt - 1);

        awaitPartitionMapExchange();

        IgniteEx testNode = grid(0);

        List<Integer> primary = IntStream.of(grid(0).affinity(CACHE_NAME).primaryPartitions(testNode.localNode())).boxed().collect(Collectors.toList());
        List<Integer> backups = IntStream.of(grid(0).affinity(CACHE_NAME).backupPartitions(testNode.localNode())).boxed().collect(Collectors.toList());

        IgniteEx g4 = startGrid(nodeCnt - 1);

        awaitPartitionMapExchange(true, true, null);

        // Fins a key what was primary on testNode, and a testNode no longer owner on next topology.
        int k = primary.stream().filter(p -> !crd.affinity(CACHE_NAME).mapKeyToPrimaryAndBackups(p).contains(testNode.localNode())).findFirst().orElse(-1);

        if (k == -1)
            k = backups.stream().filter(p -> !crd.affinity(CACHE_NAME).mapKeyToPrimaryAndBackups(p).contains(testNode.localNode())).findFirst().orElse(-1);

        assertTrue(String.valueOf(k), k != -1);

        try (Transaction tx = grid(testNode.name()).transactions().txStart(OPTIMISTIC, TransactionIsolation.REPEATABLE_READ)) {
            IgniteCache<Object, Object> cache1 = grid(testNode.name()).cache(CACHE_NAME);

            cache1.put(k, 0);

            tx.commit();
        }

        assertEquals(0, testNode.cache(CACHE_NAME).get(k));

        IgniteEx g1 = grid(1);

        CyclicBarrier b = new CyclicBarrier(2);

        AtomicInteger a = new AtomicInteger();

        IgniteEx prim = (IgniteEx) grid(crd.affinity(CACHE_NAME).mapKeyToPrimaryAndBackups(k).iterator().next());

        TestRecordingCommunicationSpi.spi(prim).blockMessages(GridDhtTxFinishRequest.class, testNode.name());

        int finalK = k;
        IgniteInternalFuture<?> fut = multithreadedAsync(new Runnable() {
            @Override public void run() {
                try (Transaction tx = grid(g1.name()).transactions().txStart(OPTIMISTIC, TransactionIsolation.REPEATABLE_READ)) {
                    IgniteCache<Object, Object> cache2 = grid(g1.name()).cache(CACHE_NAME);

                    cache2.put(finalK, a.incrementAndGet());

//                    TransactionProxyImpl p = (TransactionProxyImpl) tx;
//                    p.tx().prepare(true);
//
//                    b.await();
//
//                    System.out.println();

                    //stopNode2();
                    tx.commit();
                }
                catch (Throwable t) {
                    // No-op.
                }
            }
        }, 2, "tx-thread");

        TestRecordingCommunicationSpi.spi(prim).waitForBlocked(2);

        Collection<IgniteInternalTx> txs0 = testNode.context().cache().context().tm().activeTransactions();

        Iterator<IgniteInternalTx> it = txs0.iterator();
        GridNearTxRemote tx1 = (GridNearTxRemote) it.next();
        GridNearTxRemote tx2 = (GridNearTxRemote) it.next();

        IgniteTxEntry entry = tx1.writeEntries().iterator().next();
        GridCacheEntryEx cached = entry.cached();

        List<GridCacheMvccCandidate> rmts = new ArrayList<>(cached.remoteMvccSnapshot());

        GridCacheVersion ver0 = rmts.get(1).version();

        // Release commit messages out of order.
        TestRecordingCommunicationSpi.spi(prim).stopBlock(true, new IgnitePredicate<T2<ClusterNode, GridIoMessage>>() {
            @Override public boolean apply(T2<ClusterNode, GridIoMessage> pair) {
                GridIoMessage ioMsg = pair.get2();
                GridDhtTxFinishRequest req = (GridDhtTxFinishRequest) ioMsg.message();

                return req.version().equals(ver0);
            }
        });

        doSleep(1000);


        IgniteEx nonTxNode = stopNode2(crd, testNode, k);
        nonTxNode.close();

        doSleep(1000);


        Collection<IgniteInternalTx> txs = testNode.context().cache().context().tm().activeTransactions();

        assertTrue(txs.isEmpty());

        TestRecordingCommunicationSpi.spi(prim).stopBlock();

        fut.get();
    }

    @Test
    @Ignore
    public void testZzz4() throws Exception {
        backups = 1;
        nodeCnt = 4;
        keyCnt = 10;
        partitions = 128;
        rebalancMode = ASYNC;
        evict = false;

        IgniteEx crd = startGrids(2);
        awaitPartitionMapExchange(true, true, null);

        startGrid(2);
        startGrid(3);
        awaitPartitionMapExchange(true, true, null);

        int cand = -1;

        for (int p = 0; p < partitions; p++) {
            if (!crd.affinity(CACHE_NAME).isPrimaryOrBackup(crd.cluster().localNode(), p)) {
                cand = p;

                break;
            }
        }

        assert cand != -1;

        try (Transaction tx = crd.transactions().txStart(OPTIMISTIC, TransactionIsolation.REPEATABLE_READ)) {
            IgniteCache<Object, Object> nearCache = crd.cache(CACHE_NAME);

            nearCache.put(cand, 0);

            tx.commit();
        }

        Collection<ClusterNode> nodes = crd.affinity(CACHE_NAME).mapKeyToPrimaryAndBackups(cand);

        IgniteEx owner = (IgniteEx) grid(nodes.iterator().next());

        TestRecordingCommunicationSpi.spi(owner).blockMessages(GridDhtTxFinishRequest.class, crd.name());

        int finalCand = cand;
        IgniteInternalFuture fut = GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
                try (Transaction tx = owner.transactions().txStart(OPTIMISTIC, TransactionIsolation.REPEATABLE_READ)) {
                    IgniteCache<Object, Object> colocatedCache = owner.cache(CACHE_NAME);

                    colocatedCache.put(finalCand, 1);

                    tx.commit();
                }
            }
        });

        TestRecordingCommunicationSpi.spi(owner).waitForBlocked();

        Collection<IgniteInternalTx> txs = crd.context().cache().context().tm().activeTransactions();
        GridNearTxRemote rmtTx = (GridNearTxRemote) txs.iterator().next();

        IgniteInternalFuture fut2 = GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
                try (Transaction tx = crd.transactions().txStart(OPTIMISTIC, TransactionIsolation.REPEATABLE_READ)) {
                    IgniteCache<Object, Object> nearCache = crd.cache(CACHE_NAME);

                    nearCache.put(finalCand, 2);

                    TransactionProxyImpl p = (TransactionProxyImpl) tx;
                    p.tx().prepare(true);

                    //tx.commit();
                } catch (Throwable t) {
                    fail(X.getFullStackTrace(t));
                }
            }
        });

        doSleep(1000);

        TestRecordingCommunicationSpi.spi(owner).stopBlock();

        doSleep(1000);

        Object v1 = crd.cache(CACHE_NAME).get(finalCand);
        Object v2 = owner.cache(CACHE_NAME).get(finalCand);

        GridKernalContextImpl ctx = (GridKernalContextImpl) grid(0).context();
        ctx.dump(finalCand, log);

        System.out.println();


//        List<ClusterNode> nodes0 = new ArrayList<>(crd.cluster().nodes());
//
//        nodes0.removeAll(nodes);
//        nodes0.remove(crd.localNode());
//
//        ClusterNode toStop = nodes0.get(0);
//
//        IgniteInternalFuture fut3 = GridTestUtils.runAsync(new Runnable() {
//            @Override public void run() {
//                grid(toStop).close();
//            }
//        });
//
//        doSleep(1000);
//
//        Collection<IgniteInternalTx> txs2 = crd.context().cache().context().tm().activeTransactions();
//
//        System.out.println();

        //TestRecordingCommunicationSpi.spi(owner).stopBlock();

        //fut.get();

//        System.out.println();
//
//        final List<DiscoveryEvent> mergedEvts = new ArrayList<>();
//
//        mergeExchangeWaitVersion(crd, 8, mergedEvts);
//
//        stopGrid(getTestIgniteInstanceName(2), true, false);
//        stopGrid(getTestIgniteInstanceName(3), true, false);
//
//        awaitPartitionMapExchange();

        // 1 Block near prep
        // remove 2 nodes
        // unblock near prep
        // check if a tx created

        System.out.println();
    }

    @Test
    @Ignore
    public void testRemoveReader() throws Exception {
        backups = 1;
        nodeCnt = 4;
        keyCnt = 10;
        partitions = 128;
        rebalancMode = ASYNC;
        evict = false;

        IgniteEx crd = startGrids(4);
        awaitPartitionMapExchange(true, true, null);

        int cand = -1;

        IgniteEx testNode = grid(0);

        for (int p = 0; p < partitions; p++) {
            if (!crd.affinity(CACHE_NAME).mapPartitionToPrimaryAndBackups(p).contains(testNode.localNode())) {
                cand = p;

                break;
            }
        }

        assertTrue(String.valueOf(cand), cand != -1);

        // Create reader.
        try (Transaction tx = testNode.transactions().txStart(OPTIMISTIC, TransactionIsolation.REPEATABLE_READ)) {
            IgniteCache<Object, Object> nearCache = testNode.cache(CACHE_NAME);

            nearCache.put(cand, 0);

            tx.commit();
        }

        GridCacheContext<Object, Object> ctx = testNode.context().cache().context().cacheContext(CU.cacheId(CACHE_NAME));

        GridNearTransactionalCache cache = (GridNearTransactionalCache) ctx.cache();
        assertEquals(1, cache.map().internalSize());

        Collection<ClusterNode> nodes = crd.affinity(CACHE_NAME).mapKeyToPrimaryAndBackups(cand);

        IgniteEx owner = (IgniteEx) grid(nodes.iterator().next());

        try (Transaction tx = owner.transactions().txStart(OPTIMISTIC, TransactionIsolation.REPEATABLE_READ)) {
            IgniteCache<Object, Object> colocatedCache = owner.cache(CACHE_NAME);

            colocatedCache.put(cand, 1);

            tx.commit();
        }

        GridCacheLocalConcurrentMap map = (GridCacheLocalConcurrentMap) cache.map();

        Collection<GridCacheMapEntry> entries = map.entries(CU.cacheId(CACHE_NAME));

        assertEquals(1, entries.size());

        try (Transaction tx = owner.transactions().txStart(OPTIMISTIC, TransactionIsolation.REPEATABLE_READ)) {
            IgniteCache<Object, Object> colocatedCache = owner.cache(CACHE_NAME);

            colocatedCache.remove(cand);

            tx.commit();
        }

        assertEquals(0, entries.size());

        // Will not create near mapping because readers are cleared.
        try (Transaction tx = owner.transactions().txStart(OPTIMISTIC, TransactionIsolation.REPEATABLE_READ)) {
            IgniteCache<Object, Object> colocatedCache = owner.cache(CACHE_NAME);

            colocatedCache.put(cand, 1);

            tx.commit();
        }
    }

    private IgniteEx stopNode2(IgniteEx crd, IgniteEx testNode, int k) {
        Collection<ClusterNode> txNodes = crd.affinity(CACHE_NAME).mapKeyToPrimaryAndBackups(k);
        txNodes.add(testNode.localNode());

        ClusterNode nonTxNode =
            crd.cluster().nodes().stream().filter(n -> !txNodes.contains(n)).findFirst().orElseGet(null);

        assertNotNull(nonTxNode);

        IgniteEx nonTxIgnite = (IgniteEx) grid(nonTxNode);
        assertTrue(nonTxIgnite.context().cache().context().tm().activeTransactions().isEmpty());

        return nonTxIgnite;
    }

    @Override protected long getTestTimeout() {
        return super.getTestTimeout() * 10000;
    }

    @Override protected long getPartitionMapExchangeTimeout() {
        return super.getPartitionMapExchangeTimeout() * 10000;
    }
}
