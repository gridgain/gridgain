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

import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.distributed.GridCacheAbstractNodeRestartSelfTest;
import org.apache.ignite.internal.processors.cache.transactions.TransactionProxyImpl;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.ASYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;

/**
 * Test node restart.
 */
public class GridCachePartitionedOptimisticTxNodeRestartTest extends GridCacheAbstractNodeRestartSelfTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

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
        super.testRestartWithTxFourNodesNoBackups();
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testRestartWithTxFourNodesOneBackups() throws Throwable {
        super.testRestartWithTxFourNodesOneBackups();
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testRestartWithTxSixNodesTwoBackups() throws Throwable {
        super.testRestartWithTxSixNodesTwoBackups();
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testRestartWithTxFourNodesTwoBackups() throws Throwable {
        super.testRestartWithTxFourNodesTwoBackups();
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testRestartWithTxTenNodesTwoBackups() throws Throwable {
        super.testRestartWithTxTenNodesTwoBackups();
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testRestartWithTxTwoNodesNoBackups() throws Throwable {
        super.testRestartWithTxTwoNodesNoBackups();
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testRestartWithTxTwoNodesOneBackup() throws Throwable {
        super.testRestartWithTxTwoNodesOneBackup();
    }

    @Test
    public void testZzz() throws Exception {
        backups = 2;
        nodeCnt = 4;
        keyCnt = 10;
        partitions = 128;
        rebalancMode = ASYNC;
        evict = false;

        IgniteEx crd = startGrids(3);

        awaitPartitionMapExchange();

        IgniteEx testNode = grid(0);

        List<Integer> primary = IntStream.of(grid(0).affinity(CACHE_NAME).primaryPartitions(testNode.localNode())).boxed().collect(Collectors.toList());
        List<Integer> backups = IntStream.of(grid(0).affinity(CACHE_NAME).backupPartitions(testNode.localNode())).boxed().collect(Collectors.toList());

        IgniteEx g3 = startGrid(3);

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

        try (Transaction tx = grid(g1.name()).transactions().txStart(OPTIMISTIC, TransactionIsolation.REPEATABLE_READ)) {
            IgniteCache<Object, Object> cache2 = grid(g1.name()).cache(CACHE_NAME);

            cache2.put(k, 1);

//            TransactionProxyImpl p = (TransactionProxyImpl) tx;
//            p.tx().prepare(true);
//
//            nodes.remove(owner);
//
//            grid(owner).close();

            tx.commit();
        }

        //stopGrid(grid(3).name(), false, false);
        grid(3).close();

        try (Transaction tx = grid(g1.name()).transactions().txStart(OPTIMISTIC, TransactionIsolation.REPEATABLE_READ)) {
            IgniteCache<Object, Object> cache2 = grid(g1.name()).cache(CACHE_NAME);

            cache2.put(k, 1);

//            TransactionProxyImpl p = (TransactionProxyImpl) tx;
//            p.tx().prepare(true);
//
//            nodes.remove(owner);
//
//            grid(owner).close();

            tx.commit();
        }

//        awaitPartitionMapExchange();
//
//        assertEquals(1, grid(nodes.iterator().next()).cache(CACHE_NAME).get(k));
//
//        checkFutures();

        System.out.println();
    }
}
