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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.events.CacheRebalancingEvent;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.internal.SupportFeaturesUtils.IGNITE_BASELINE_AUTO_ADJUST_FEATURE;
import static org.apache.ignite.internal.SupportFeaturesUtils.IGNITE_BASELINE_FOR_IN_MEMORY_CACHES_FEATURE;
import static org.apache.ignite.internal.SupportFeaturesUtils.IGNITE_DISTRIBUTED_META_STORAGE_FEATURE;
import static org.apache.ignite.testframework.GridTestUtils.mergeExchangeWaitVersion;

/**
 * Tests partition loss detection in various configurations.
 * TODO persistent mode shoud be moved to cache7 suite.
 */
public class CachePartitionLossDetectionOnNodeLeftTest extends GridCommonAbstractTest {
    /** */
    private static final int PARTS_CNT = 32;

    /** */
    private boolean enableBaseline;

    /** */
    private boolean persistence;

    /** */
    private PartitionLossPolicy lossPlc;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        // Enable baseline for volatile caches.
        if (enableBaseline)
            cfg.setActiveOnStart(false);

        cfg.setConsistentId(igniteInstanceName);

        cfg.setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME).
            setAtomicityMode(TRANSACTIONAL).
            setCacheMode(PARTITIONED).
            setPartitionLossPolicy(lossPlc).
            setAffinity(new RendezvousAffinityFunction(false, PARTS_CNT)).
            setBackups(0));

        cfg.setIncludeEventTypes(EventType.EVTS_ALL);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration();
        dsCfg.setWalSegmentSize(4 * 1024 * 1024);
        dsCfg.setWalMode(WALMode.LOG_ONLY);

        final int size = 50 * 1024 * 1024;

        DataRegionConfiguration drCfg = new DataRegionConfiguration();
        drCfg.setName("default").setInitialSize(size).setMaxSize(size).setPersistenceEnabled(true);

        dsCfg.setDefaultDataRegionConfiguration(drCfg);

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
     * In this mode the assignment for volatile caches computed ignoring BLT because BLT is disabled.
     * Partitions are expected to be LOST after migrating to remaining nodes.
     */
    @Test
    public void testPartitionLossDetectionOnNodeLeft_Volatile_Safe_Merge_NoBLT() throws Exception {
        doTestPartitionLossDetectionOnNodeLeft(false, PartitionLossPolicy.READ_WRITE_SAFE, true, false, true);
    }

    /**
     * In this mode the assignment for volatile caches computed ignoring BLT because BLT is disabled.
     * Partitions are expected to be OWNING after migrating to remaining nodes.
     */
    @Test
    public void testPartitionLossDetectionOnNodeLeft_Volatile_Unsafe_Merge_NoBLT() throws Exception {
        doTestPartitionLossDetectionOnNodeLeft(false, PartitionLossPolicy.IGNORE, true, false, true);
    }

//    /** */
//    @Test
//    @WithSystemProperty(key = IGNITE_BASELINE_FOR_IN_MEMORY_CACHES_FEATURE, value = "true")
//    @WithSystemProperty(key = IGNITE_BASELINE_AUTO_ADJUST_FEATURE, value = "true")
//    @WithSystemProperty(key = IGNITE_DISTRIBUTED_META_STORAGE_FEATURE, value = "true")
//    public void testPartitionLossDetectionOnNodeLeft_Volatile_Safe_Merge_BLT_AutoReset() throws Exception {
//        /** In this mode the assignment for volatile caches computed ignoring BLT because BLT is disabled for volatile
//         * caches. */
//        doTestPartitionLossDetectionOnNodeLeft(false, PartitionLossPolicy.READ_WRITE_SAFE, true, false, true);
//    }
//
//    /** */
//    @Test
//    @WithSystemProperty(key = IGNITE_BASELINE_FOR_IN_MEMORY_CACHES_FEATURE, value = "true")
//    public void testPartitionLossDetectionOnNodeLeft_Volatile_Safe_Merge_BLT_NoReset() throws Exception {
//        /** In this mode the assignment for volatile caches computed ignoring BLT because BLT is disabled for volatile
//         * caches. */
//        doTestPartitionLossDetectionOnNodeLeft(false, PartitionLossPolicy.READ_WRITE_SAFE, true, false, true);
//    }

    /**
     * Test correct partition loss detection for merged exchanges.
     *
     * @param merge {@code True} to enable persistence.
     * @param lossPlc Loss policy.
     * @param merge {@code True} to merge exchanges (also disables baseline for in-memory caches).
     * @param resetBaseline {@code True} to reset baseline after nodes are left.
     */
    private void doTestPartitionLossDetectionOnNodeLeft (
        boolean persistence,
        PartitionLossPolicy lossPlc,
        boolean merge,
        boolean resetBaseline,
        boolean expectPartitionsMoved
    ) throws Exception {
        enableBaseline = !merge;
        this.persistence = persistence;
        this.lossPlc = lossPlc;

        final Ignite srv0 = startGrids(5);
        srv0.cluster().active(true);

        List<Integer> lostEvt0 = Collections.synchronizedList(new ArrayList<>());
        List<Integer> lostEvt1 = Collections.synchronizedList(new ArrayList<>());

        grid(0).events().localListen(evt -> {
            lostEvt0.add(((CacheRebalancingEvent)evt).partition());

            return true;
        }, EventType.EVT_CACHE_REBALANCE_PART_DATA_LOST);

        grid(1).events().localListen(evt -> {
            lostEvt1.add(((CacheRebalancingEvent)evt).partition());

            return true;
        }, EventType.EVT_CACHE_REBALANCE_PART_DATA_LOST);

        awaitPartitionMapExchange();

        if (merge)
            mergeExchangeWaitVersion(srv0, 8, null);

        int[] p2 = srv0.affinity(DEFAULT_CACHE_NAME).primaryPartitions(grid(2).localNode());
        int[] p3 = srv0.affinity(DEFAULT_CACHE_NAME).primaryPartitions(grid(3).localNode());
        int[] p4 = srv0.affinity(DEFAULT_CACHE_NAME).primaryPartitions(grid(4).localNode());

        List<Integer> expLostParts = new ArrayList<>();

        for (int i = 0; i < p2.length; i++)
            expLostParts.add(p2[i]);
        for (int i = 0; i < p3.length; i++)
            expLostParts.add(p3[i]);
        for (int i = 0; i < p4.length; i++)
            expLostParts.add(p4[i]);

        Collections.sort(expLostParts);

        stopGrid(getTestIgniteInstanceName(4), true, !merge);
        stopGrid(getTestIgniteInstanceName(3), true, !merge);
        stopGrid(getTestIgniteInstanceName(2), true, !merge);

        if (resetBaseline)
            resetBaselineTopology();

        waitForReadyTopology(internalCache(1, DEFAULT_CACHE_NAME).context().topology(), new AffinityTopologyVersion(8, 0));

        if (lossPlc != PartitionLossPolicy.IGNORE) {
            assertEquals(new HashSet<>(expLostParts), grid(0).cache(DEFAULT_CACHE_NAME).lostPartitions());
            assertEquals(new HashSet<>(expLostParts), grid(1).cache(DEFAULT_CACHE_NAME).lostPartitions());

            srv0.resetLostPartitions(Collections.singletonList(DEFAULT_CACHE_NAME));

            awaitPartitionMapExchange();
        }

        assertTrue(grid(0).cache(DEFAULT_CACHE_NAME).lostPartitions().isEmpty());
        assertTrue(grid(1).cache(DEFAULT_CACHE_NAME).lostPartitions().isEmpty());

        if (expectPartitionsMoved) {
            final List<GridDhtPartitionTopology> tops = Arrays.asList(
                grid(0).cachex(DEFAULT_CACHE_NAME).context().topology(),
                grid(1).cachex(DEFAULT_CACHE_NAME).context().topology());

            for (int p = 0; p < PARTS_CNT; p++) {
                for (GridDhtPartitionTopology top : tops) {
                    final GridDhtLocalPartition p0 = top.localPartition(p);

                    if (p0 != null && p0.state() != GridDhtPartitionState.EVICTED) {
                        assertTrue(lossPlc == PartitionLossPolicy.IGNORE ? p0.state() == GridDhtPartitionState.OWNING :
                            p0.state() != GridDhtPartitionState.LOST || expLostParts.contains(p));
                    }
                }
            }
        }

        if (lossPlc == PartitionLossPolicy.IGNORE) {
            // Events should not be fired for IGNORE policy.
            assertTrue(lostEvt0.isEmpty());
            assertTrue(lostEvt1.isEmpty());
        }
        else {
            // Event must be fired only once for any mode.
            assertEquals("Node0", expLostParts, lostEvt0);
            assertEquals("Node1", expLostParts, lostEvt1);
        }

        assertTrue(grid(0).cache(DEFAULT_CACHE_NAME).lostPartitions().isEmpty());
        assertTrue(grid(1).cache(DEFAULT_CACHE_NAME).lostPartitions().isEmpty());

        // Check if writes are allowed after resetting lost state (or ignore mode processing)
        for (int i = 0; i < PARTS_CNT; i++) {
            for (Ignite ig : G.allGrids())
                ig.cache(DEFAULT_CACHE_NAME).put(i, i);
        }

        // Graceful shutdown.
        srv0.cluster().active(false);
    }
}
