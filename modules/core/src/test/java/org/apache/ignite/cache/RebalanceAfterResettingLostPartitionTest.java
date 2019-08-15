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

package org.apache.ignite.cache;

import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Collections;
import java.util.Objects;
import java.util.UUID;

import static org.apache.ignite.configuration.WALMode.LOG_ONLY;

/**
 *
 */
@RunWith(JUnit4.class)
public class RebalanceAfterResettingLostPartitionTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String CACHE_NAME = "cache" + UUID.randomUUID().toString();

    /** Cache size */
    public static final int CACHE_SIZE = GridTestUtils.SF.applyLB(100_000, 10_000);

    /** Stop all grids and cleanup persistence directory. */
    @Before
    public void before() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** Stop all grids and cleanup persistence directory. */
    @After
    public void after() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        DataStorageConfiguration storageCfg = new DataStorageConfiguration();

        storageCfg.setPageSize(1024).setWalMode(LOG_ONLY).setWalSegmentSize(8 * 1024 * 1024);

        storageCfg.getDefaultDataRegionConfiguration()
            .setPersistenceEnabled(true)
            .setMaxSize(500L * 1024 * 1024);

        cfg.setDataStorageConfiguration(storageCfg);

        cfg.setRebalanceBatchSize(100);

        cfg.setCacheConfiguration(new CacheConfiguration()
            .setName(CACHE_NAME)
            .setCacheMode(CacheMode.PARTITIONED)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setRebalanceMode(CacheRebalanceMode.ASYNC)
            .setBackups(1)
            .setPartitionLossPolicy(PartitionLossPolicy.READ_ONLY_SAFE)
            .setStatisticsEnabled(true));

        return cfg;
    }

    /**
     * Test to restore lost partitions and rebalance data on working grid with two nodes.
     *
     * @throws Exception if fail.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testRebalanceAfterPartitionsWereLost() throws Exception {
        startGrids(2);

        grid(0).cluster().active(true);

        for (int j = 0; j < CACHE_SIZE; j++)
            grid(0).cache(CACHE_NAME).put(j, "Value" + j);

        String g1Name = grid(1).name();

        // Stopping the the second node.
        stopGrid(1);

        // Cleaning the persistence for second node.
        cleanPersistenceDir(g1Name);

        // Starting second node again(with the same consistent id).
        startGrid(1);

        // Killing the first node at the moment of rebalancing.
        stopGrid(0);

        // Returning first node to the cluster.
        startGrid(0);

        assertTrue(Objects.requireNonNull(
            grid(0).cachex(CACHE_NAME)).context().topology().localPartitions().stream().allMatch(
            p -> p.state() == GridDhtPartitionState.OWNING));

        // Verify that partition loss is detected.
        assertTrue(Objects.requireNonNull(
            grid(1).cachex(CACHE_NAME)).context().topology().localPartitions().stream().allMatch(
            p -> p.state() == GridDhtPartitionState.LOST));

        // Reset lost partitions and wait for PME.
        grid(1).resetLostPartitions(Collections.singletonList(CACHE_NAME));

        awaitPartitionMapExchange();

        assertTrue(Objects.requireNonNull(
            grid(0).cachex(CACHE_NAME)).context().topology().localPartitions().stream().allMatch(
            p -> p.state() == GridDhtPartitionState.OWNING));

        // Verify that partitions are in owning state.
        assertTrue(Objects.requireNonNull(
            grid(1).cachex(CACHE_NAME)).context().topology().localPartitions().stream().allMatch(
            p -> p.state() == GridDhtPartitionState.OWNING));

        // Verify that data was successfully rebalanced.
        for (int i = 0; i < CACHE_SIZE; i++)
            assertEquals("Value" + i, grid(0).cache(CACHE_NAME).get(i));

        for (int i = 0; i < CACHE_SIZE; i++)
            assertEquals("Value" + i, grid(1).cache(CACHE_NAME).get(i));
    }
}
