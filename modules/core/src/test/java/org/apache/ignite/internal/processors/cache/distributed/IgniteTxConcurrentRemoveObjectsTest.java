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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.UUID;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;

import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 *
 */
public class IgniteTxConcurrentRemoveObjectsTest extends GridCommonAbstractTest {
    /** Cache partitions. */
    private static final int CACHE_PARTITIONS = 16;

    /** Cache entries count. */
    private static final int CACHE_ENTRIES_COUNT = 512 * CACHE_PARTITIONS;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        grid(0).destroyCache(DEFAULT_CACHE_NAME);

        super.afterTest();
    }

    /**
     * @return Cache configuration.
     */
    private CacheConfiguration<Integer, String> cacheConfiguration() {
        CacheConfiguration<Integer, String> ccfg = new CacheConfiguration<>();

        ccfg.setName(DEFAULT_CACHE_NAME);

        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

        ccfg.setAffinity(new RendezvousAffinityFunction().setPartitions(CACHE_PARTITIONS));

        return ccfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testOptimisticTxLeavesObjectsInLocalPartition() throws Exception {
        checkTxLeavesObjectsInLocalPartition(cacheConfiguration(), TransactionConcurrency.OPTIMISTIC, SERIALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPessimisticTxLeavesObjectsInLocalPartition() throws Exception {
        checkTxLeavesObjectsInLocalPartition(cacheConfiguration(), TransactionConcurrency.PESSIMISTIC, SERIALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMvccTxLeavesObjectsInLocalPartition() throws Exception {
        checkTxLeavesObjectsInLocalPartition(cacheConfiguration().setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT),
            TransactionConcurrency.PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * Checks if too many deletes in single transaction doesn't corrupt internal map state in {@link GridDhtLocalPartition}.
     *
     * @throws Exception If failed.
     */
    public void checkTxLeavesObjectsInLocalPartition(CacheConfiguration<Integer, String> ccfg,
        TransactionConcurrency optimistic, TransactionIsolation isolation) throws Exception {
        IgniteEx igniteEx = grid(0);

        igniteEx.getOrCreateCache(ccfg);

        try (IgniteDataStreamer<Integer, String> dataStreamer = igniteEx.dataStreamer(DEFAULT_CACHE_NAME)) {
            for (int i = 0; i < CACHE_ENTRIES_COUNT; i++)
                dataStreamer.addData(i, UUID.randomUUID().toString());
        }

        IgniteEx client = startGrid(
            getConfiguration()
                .setClientMode(true)
                .setIgniteInstanceName(UUID.randomUUID().toString())
        );

        awaitPartitionMapExchange();

        assertEquals(CACHE_ENTRIES_COUNT, client.getOrCreateCache(DEFAULT_CACHE_NAME).size());

        try (Transaction tx = client.transactions().txStart(optimistic, isolation)) {
            IgniteCache<Integer, String> cache = client.getOrCreateCache(cacheConfiguration());

            for (int v = 0; v < CACHE_ENTRIES_COUNT; v++) {
                cache.get(v);

                cache.remove(v);
            }

            tx.commit();
        }

        GridTestUtils.waitForCondition(
            () -> igniteEx.context().cache().cacheGroups().stream()
                .filter(CacheGroupContext::userCache)
                .flatMap(cgctx -> cgctx.topology().localPartitions().stream())
                .mapToInt(GridDhtLocalPartition::internalSize)
                .max().orElse(-1) == 0,
            500L
        );
    }
}
