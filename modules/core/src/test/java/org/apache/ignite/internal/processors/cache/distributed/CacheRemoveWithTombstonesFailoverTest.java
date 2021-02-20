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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.GridCacheGroupIdMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.processors.metric.impl.MetricUtils;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.ASYNC;

/**
 * Tests to check failover scenarios over tombstones.
 */
@RunWith(Parameterized.class)
@WithSystemProperty(key = "DEFAULT_TOMBSTONE_TTL", value = "3000")
@WithSystemProperty(key = "CLEANUP_WORKER_SLEEP_INTERVAL", value = "10000000")
public class CacheRemoveWithTombstonesFailoverTest extends GridCommonAbstractTest {
    /** */
    private static final String TS_METRIC_NAME = "Tombstones";

    /** */
    @Parameterized.Parameter(value = 0)
    public CacheAtomicityMode atomicityMode;

    /**
     * @return List of test parameters.
     */
    @Parameterized.Parameters(name = "mode={0}")
    public static List<Object[]> parameters() {
        ArrayList<Object[]> params = new ArrayList<>();

        params.add(new Object[]{ATOMIC});
        params.add(new Object[]{TRANSACTIONAL});

        return params;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TestRecordingCommunicationSpi commSpi = new TestRecordingCommunicationSpi();

        cfg.setConsistentId(gridName);

        cfg.setCommunicationSpi(commSpi);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setInitialSize(256L * 1024 * 1024)
                    .setMaxSize(256L * 1024 * 1024)
                    .setPersistenceEnabled(true)
            )
            .setCheckpointFrequency(1024 * 1024 * 1024)
            .setWalSegmentSize(4 * 1024 * 1024);

        cfg.setDataStorageConfiguration(dsCfg);

        cfg.setCacheConfiguration(cacheConfiguration());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * Test check that tombstones reside in persistent partition will be cleared after node restart.
     */
    @Test
    public void testTombstonesClearedAfterRestart() throws Exception {
        IgniteEx crd = startGrid(0);
        crd.cluster().state(ClusterState.ACTIVE);

        final int KEYS = 1024;

        for (int k = 0; k < KEYS; k++)
            crd.cache(DEFAULT_CACHE_NAME).put(k, k);

        blockRebalance(crd);

        IgniteEx demander = startGrid(1);
        resetBaselineTopology();

        TestRecordingCommunicationSpi.spi(crd).waitForBlocked();

        Set<Integer> keysWithTombstone = new HashSet<>();

        // Do removes while rebalance is in progress.
        for (int i = 0; i < KEYS; i += 2) {
            keysWithTombstone.add(i);

            crd.cache(DEFAULT_CACHE_NAME).remove(i); // Should create tombstones on both nodes.
        }

        // Cache group context is not initialized properly on demander while calculating metric.
        long tsCnt = demander.context().cache().cacheGroup(CU.cacheId(DEFAULT_CACHE_NAME)).offheap().tombstonesCount();

        Assert.assertEquals(keysWithTombstone.size(), tsCnt);

        doSleep(3000);

        // Resume rebalance.
        TestRecordingCommunicationSpi.spi(crd).stopBlock();

        // Partitions should be in OWNING state.
        awaitPartitionMapExchange();

        assertPartitionsSame(idleVerify(demander, DEFAULT_CACHE_NAME));

        stopAllGrids();

        // Startup demander with tombstones in inactive state.
        demander = startGrid(1);

        final int grpId = groupIdForCache(demander, DEFAULT_CACHE_NAME);

        // Tombstone metrics are unavailable before join to topology, using internal api.
        tsCnt = demander.context().cache().cacheGroup(grpId).offheap().tombstonesCount();

        Assert.assertEquals(keysWithTombstone.size(), tsCnt);

        crd = startGrid(0);

        crd.cluster().state(ClusterState.ACTIVE);

        awaitPartitionMapExchange();

        final LongMetric tombstoneMetric1 = demander.context().metric().registry(
            MetricUtils.cacheGroupMetricsRegistryName(DEFAULT_CACHE_NAME)).findMetric(TS_METRIC_NAME);

        // Tombstones should be removed after join to topology.
        assertTrue(GridTestUtils.waitForCondition(() -> tombstoneMetric1.value() == 0, 30_000));
    }

    /**
     *
     */
    private static void blockRebalance(IgniteEx node) {
        final int grpId = groupIdForCache(node, DEFAULT_CACHE_NAME);

        TestRecordingCommunicationSpi.spi(node).blockMessages((node0, msg) ->
            (msg instanceof GridDhtPartitionSupplyMessage)
                && ((GridCacheGroupIdMessage)msg).groupId() == grpId
        );
    }

    /**
     * @return Cache configuration.
     */
    private CacheConfiguration<Object, Object> cacheConfiguration() {
        return new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAtomicityMode(TRANSACTIONAL)
            .setCacheMode(PARTITIONED)
            .setBackups(1)
            .setRebalanceMode(ASYNC)
            .setReadFromBackup(true)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setAffinity(new RendezvousAffinityFunction(false, 64));
    }
}
