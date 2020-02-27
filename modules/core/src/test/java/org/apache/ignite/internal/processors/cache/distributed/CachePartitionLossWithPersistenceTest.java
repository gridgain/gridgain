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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.PartitionLossPolicy.READ_WRITE_SAFE;

/**
 * Test scenario: supplier is left during rebalancing leaving partition in OWNING state (but actually LOST) because only
 * one owner left. <p> Expected result: no assertions are triggered.
 */
public class CachePartitionLossWithPersistenceTest extends GridCommonAbstractTest {
    public static final int WAIT = 2_000;

    /** */
    private static final int PARTS_CNT = 32;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setFailureDetectionTimeout(100000000L);
        cfg.setClientFailureDetectionTimeout(100000000L);

        cfg.setConsistentId(igniteInstanceName);
        cfg.setClientMode(igniteInstanceName.startsWith("client"));

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setWalMode(WALMode.LOG_ONLY)
                .setWalSegmentSize(4 * 1024 * 1024)
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                        .setMaxSize(100L * 1024 * 1024))
        );

        cfg.setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME).
            setPartitionLossPolicy(READ_WRITE_SAFE).
            setBackups(1).
            setAffinity(new RendezvousAffinityFunction(false, PARTS_CNT)));

        return cfg;
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    @Test
    public void testDataLost() throws Exception {
        IgniteEx crd = startGrids(3);
        crd.cluster().active(true);

        //printPartitionState(DEFAULT_CACHE_NAME, 0);

        grid(2).close();

        awaitPartitionMapExchange();

        //printPartitionState(DEFAULT_CACHE_NAME, 0);

        //logCacheSize("DBG: server_3 left");

        final int keys = 1_000;

        load(crd, DEFAULT_CACHE_NAME, IntStream.range(0, keys).boxed());

        //logCacheSize("DBG: 100k loaded");

        //printPartitionState(DEFAULT_CACHE_NAME, 0);

        grid(1).close();
        doSleep(WAIT);

        //logCacheSize("DBG: server_2 left");
        //printPartitionState(DEFAULT_CACHE_NAME, 0);

        startGrid(2);

        awaitPartitionMapExchange();

        //logCacheSize("DBG: server_2 ret");
        //printPartitionState(DEFAULT_CACHE_NAME, 0);

        startGrid(1);

        awaitPartitionMapExchange();

        //logCacheSize("DBG: server_1 ret");
        //printPartitionState(DEFAULT_CACHE_NAME, 0);

        //doSleep(WAIT + 5_000);
        //logCacheSize("DBG: all servers are up");

        crd.resetLostPartitions(Collections.singleton(DEFAULT_CACHE_NAME));

        //doSleep(WAIT + 5_000);

        awaitPartitionMapExchange();

        //logCacheSize("DBG: after reset loss parts");

        assertPartitionsSame(idleVerify(crd, DEFAULT_CACHE_NAME));
    }

    @Test
    public void testDataLost2() throws Exception {
        IgniteEx crd = startGrids(2);
        crd.cluster().active(true);

        final List<Integer> moving = movingKeysAfterJoin(grid(1), DEFAULT_CACHE_NAME, PARTS_CNT);

        startGrid(2);
        resetBaselineTopology();
        awaitPartitionMapExchange();

        // Find a lost partition.
        int part = moving.stream().filter(
            p -> !crd.affinity(DEFAULT_CACHE_NAME).mapPartitionToPrimaryAndBackups(p).
                contains(crd.localNode())).findFirst().orElseThrow(AssertionError::new);

        stopGrid(1);

        crd.cache(DEFAULT_CACHE_NAME).put(part, 0);

        stopGrid(2);

        assertTrue(crd.cache(DEFAULT_CACHE_NAME).lostPartitions().contains(part));

        startGrid(1);

        awaitPartitionMapExchange(); // Will assign new primaries on g1.

        startGrid(2);

        awaitPartitionMapExchange();

        printPartitionState(DEFAULT_CACHE_NAME, 0);

        crd.resetLostPartitions(Collections.singleton(DEFAULT_CACHE_NAME));

        awaitPartitionMapExchange();

        assertPartitionsSame(idleVerify(crd, DEFAULT_CACHE_NAME));
    }

    private void logCacheSize(String prefix) {
        final IgniteCache<Object, Object> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        String msg = prefix + " size=" + cache.size();
        msg += ", partsLost=" + cache.lostPartitions();

        log.info(msg);
    }
}
