/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.cache;

import javax.cache.CacheException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.CacheInvalidStateException;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/**
 * This test is needed for reproducing possible deadlock on concurrent {@link IgniteCache#removeAll()}
 */
public class RemoveAllDeadlockTest extends GridCommonAbstractTest {
    /** Threads number for reproducing deadlock. */
    private static final int THREADS = 4;

    /** */
    private ClusterState clusterStateOnStart = ACTIVE;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setClusterStateOnStart(ACTIVE);

        cfg.setConsistentId(igniteInstanceName);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRemoveAllAtomicPartitioned() throws Exception {
        startGrid(1);

        CacheConfiguration<Integer, Integer> cacheCfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        cacheCfg.setCacheMode(CacheMode.PARTITIONED);

        cacheCfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);

        cacheCfg.setBackups(1);

        IgniteCache<Integer, Integer> cache = grid(1).getOrCreateCache(cacheCfg);

        removeAllConcurrent(cache);
    }

    /**
     * @throws Exception If failed.
     */
    @Ignore("https://ggsystems.atlassian.net/browse/GG-26302")
    @Test
    public void testRemoveAllTransactionalPartitioned() throws Exception {
        startGrid(1);

        CacheConfiguration<Integer, Integer> cacheCfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        cacheCfg.setCacheMode(CacheMode.PARTITIONED);

        cacheCfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

        cacheCfg.setBackups(1);

        IgniteCache<Integer, Integer> cache = grid(1).getOrCreateCache(cacheCfg);

        removeAllConcurrent(cache);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRemoveAllAtomicReplicated() throws Exception {
        startGrid(1);

        CacheConfiguration<Integer, Integer> cacheCfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        cacheCfg.setCacheMode(CacheMode.REPLICATED);

        cacheCfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);

        cacheCfg.setBackups(0);

        IgniteCache<Integer, Integer> cache = grid(1).getOrCreateCache(cacheCfg);

        removeAllConcurrent(cache);
    }

    /**
     * @throws Exception If failed.
     */
    @Ignore("https://ggsystems.atlassian.net/browse/GG-26302")
    @Test
    public void testRemoveAllTransactionalReplicated() throws Exception {
        startGrid(1);

        CacheConfiguration<Integer, Integer> cacheCfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        cacheCfg.setCacheMode(CacheMode.REPLICATED);

        cacheCfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

        cacheCfg.setBackups(0);

        IgniteCache<Integer, Integer> cache = grid(1).getOrCreateCache(cacheCfg);

        removeAllConcurrent(cache);
    }

    /**
     * Ignored, but the problem might not be connected to https://ggsystems.atlassian.net/browse/GG-26302
     * Usually falls with "Cannot serialize transaction due to write conflict (transaction is marked for rollback)",
     * needs additional investigation
     * @throws Exception If failed.
     */
    @Ignore("https://ggsystems.atlassian.net/browse/GG-26302")
    @Test
    public void testRemoveAllTransactionalSnapshotReplicated() throws Exception {
        startGrid(1);

        CacheConfiguration<Integer, Integer> cacheCfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        cacheCfg.setCacheMode(CacheMode.REPLICATED);

        cacheCfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT);

        cacheCfg.setBackups(0);

        IgniteCache<Integer, Integer> cache = grid(1).getOrCreateCache(cacheCfg);

        removeAllConcurrent(cache);
    }

    /**
     * Tests that removeAll works correctly when some partitions are lost.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRemoveAllLostPartitions() throws Exception {
        startGrids(2);

        grid(0).cluster().baselineAutoAdjustEnabled(false);

        grid(0).cluster().state(ACTIVE);

        for (PartitionLossPolicy plc : PartitionLossPolicy.values()) {
            testRemoveAllLostPartitions(plc);

            grid(0).destroyCache(DEFAULT_CACHE_NAME);
        }
    }

    /**
     * Tests that removeAll works correctly when some partitions are lost.
     *
     * @param lossPlc Partition loss policy.
     * @throws Exception If failed.
     */
    private void testRemoveAllLostPartitions(PartitionLossPolicy lossPlc) throws Exception {
        CacheConfiguration<Integer, Integer> cacheCfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        cacheCfg.setPartitionLossPolicy(lossPlc);

        cacheCfg.setAffinity(new RendezvousAffinityFunction(false, 4));

        cacheCfg.setWriteSynchronizationMode(FULL_SYNC);

        IgniteCache<Integer, Integer> cache = grid(1).getOrCreateCache(cacheCfg);

        for (int i = 0; i < 100; ++i)
            cache.put(i, i);

        // Restart one node to lose partitions.
        stopGrid(0);

        startGrid(0);

        assertFalse(grid(0).cache(DEFAULT_CACHE_NAME).lostPartitions().isEmpty());

        CacheException err = assertThrows(
            log,
            () -> grid(0).cache(DEFAULT_CACHE_NAME).removeAll(),
            CacheException.class,
            null
        );

        assertTrue(X.hasCause(err, CacheInvalidStateException.class));
    }

    /**
     * @param cache Cache.
     */
    private void removeAllConcurrent(IgniteCache<Integer, Integer> cache) throws Exception {
        multithreaded(() -> {
            for (int i = 0; i < 1234; i++) {
                final int c = i % 123;

                if (c % 15 != 0) {

                    for (int j = i; j < c + i; j++)
                        cache.put(j, j * c);
                }
                else
                    cache.removeAll();
            }
        }, THREADS);
    }
}
