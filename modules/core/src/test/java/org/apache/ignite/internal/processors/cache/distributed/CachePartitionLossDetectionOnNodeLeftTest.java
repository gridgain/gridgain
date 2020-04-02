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
import java.util.Set;
import java.util.function.IntConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.util.AttributeNodeFilter;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.mergeExchangeWaitVersion;

/**
 * Tests partition loss detection in various configurations.
 * TODO persistent mode shoud be moved to cache7 suite.
 *
 * TODO do not start txlog region if no mvcc.
 * TODO finish test!
 */
public class CachePartitionLossDetectionOnNodeLeftTest extends GridCommonAbstractTest {
    /** */
    private static final int PARTS_CNT = 32;

    /** Should be wiped out after enabling BLT for volatile caches. TODO maybe remove it and use autoadjust=0. */
    private boolean enableBaseline;

    /** */
    private int nonAffinityIdx;

    /** */
    private boolean startClientCache;

    /** */
    private boolean dfltRegionPersistence;

    /** */
    private PartitionLossPolicy lossPlc;

    /** */
    private static final String START_CACHE_ATTR = "has_cache";

    /** */
    private Boolean otherRegionPersistence;

    /** */
    private static final String OTHER_CACHE = "other";

    /** */
    private int backups;

    /** */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setFailureDetectionTimeout(10000000L);
        cfg.setClientFailureDetectionTimeout(10000000L);

        // Enable baseline for volatile caches.
        if (enableBaseline)
            cfg.setActiveOnStart(false);

        cfg.setConsistentId(igniteInstanceName);

        if (client) {
            cfg.setClientMode(true);

            client = false;
        }

        cfg.setIncludeEventTypes(EventType.EVTS_ALL);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration();
        dsCfg.setWalSegmentSize(4 * 1024 * 1024);
        dsCfg.setWalMode(WALMode.LOG_ONLY);

        final int size = 50 * 1024 * 1024;

        DataRegionConfiguration dfltRegCfg = new DataRegionConfiguration();
        dfltRegCfg.setName(DEFAULT_CACHE_NAME).setInitialSize(size).setMaxSize(size).setPersistenceEnabled(dfltRegionPersistence);

        dsCfg.setDefaultDataRegionConfiguration(dfltRegCfg);

        if (otherRegionPersistence != null) {
            DataRegionConfiguration otherRegCfg = new DataRegionConfiguration();
            otherRegCfg.setName(OTHER_CACHE).setInitialSize(size).setMaxSize(size).setPersistenceEnabled(otherRegionPersistence);

            dsCfg.setDataRegionConfigurations(otherRegCfg);
        }

        cfg.setDataStorageConfiguration(dsCfg);


        // Do not start cache on non-affinity node.
        CacheConfiguration ccfg = defaultCacheConfiguration().setNearConfiguration(null).
                setNodeFilter(new AttributeNodeFilter(START_CACHE_ATTR, Boolean.TRUE)).
                setBackups(backups).
                setPartitionLossPolicy(lossPlc).
                setAffinity(new RendezvousAffinityFunction(false, PARTS_CNT));

        if (startClientCache)
            cfg.setCacheConfiguration(ccfg);

        if (getTestIgniteInstanceIndex(igniteInstanceName) != nonAffinityIdx) {
            cfg.setUserAttributes(F.asMap(START_CACHE_ATTR, Boolean.TRUE));

            if (!startClientCache)
                cfg.setCacheConfiguration(ccfg);
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        enableBaseline = false;
        nonAffinityIdx = -1;
        dfltRegionPersistence = false;
        lossPlc = PartitionLossPolicy.IGNORE;
        otherRegionPersistence = null;
        backups = 0;

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * TODO implement test difference with testPartitionLossDetectionOnNodeLeft_Volatile_FreeSwitch.
     */
    @Test
    public void testPartitionLossDetectionOnNodeLeft_Volatile_Merge_AutoAdjust() throws Exception {
        dfltRegionPersistence = false;

        doTestPartitionLossDetectionOnNodeLeft(false, false);
    }

    /**
     *
     */
    @Test
    public void testPartitionLossDetectionOnNodeLeft_Volatile_FreeSwitch() throws Exception {
        dfltRegionPersistence = false;

        doTestPartitionLossDetectionOnNodeLeft(false, false);
    }

//    /** */
//    @Test
//    @WithSystemProperty(key = IGNITE_BASELINE_FOR_IN_MEMORY_CACHES_FEATURE, value = "true")
//    @WithSystemProperty(key = IGNITE_BASELINE_AUTO_ADJUST_FEATURE, value = "true")
//    @WithSystemProperty(key = IGNITE_DISTRIBUTED_META_STORAGE_FEATURE, value = "true")
//    public void testPartitionLossDetectionOnNodeLeft_Volatile_Safe_Merge_BLT_AutoReset() throws Exception {
//        doTestPartitionLossDetectionOnNodeLeft(false, PartitionLossPolicy.IGNORE, true, true);
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
     * Tests if lost is correctly detected if addinity is loaded on cache start.
     */
    @Test
    public void testPartitionLossDetectionOnClientTopology_Volatile() throws Exception {
        final IgniteEx crd = startGrid(0);
        final IgniteEx g1 = startGrid(1);
        final IgniteEx g2 = startGrid(2);

        crd.cluster().active(true);

        assertTrue(grid(1).cache(DEFAULT_CACHE_NAME).lostPartitions().isEmpty());
        assertTrue(grid(2).cache(DEFAULT_CACHE_NAME).lostPartitions().isEmpty());

        client = true;

        // TODO client topology is not removed on client after start.
        final IgniteEx client = startGrid(3);

        stopGrid(1);

        final Set<Integer> lost1 = new HashSet<>(crd.cache(DEFAULT_CACHE_NAME).lostPartitions());
        final Set<Integer> lost2 = new HashSet<>(grid(2).cache(DEFAULT_CACHE_NAME).lostPartitions());
        final Set<Integer> lost3 = new HashSet<>(client.cache(DEFAULT_CACHE_NAME).lostPartitions());

        assertTrue(lost1.isEmpty());

        assertEquals(lost1, lost2);
        assertEquals(lost1, lost3);
    }

    /**
     * Tests if lost is correctly detected if addinity is loaded on cache start.
     */
    @Test
    public void testPartitionLossDetectionOnClientTopology_Persistent() throws Exception {
        dfltRegionPersistence = true;

        final IgniteEx crd = startGrids(3);

        crd.cluster().active(true);

        assertTrue(grid(1).cache(DEFAULT_CACHE_NAME).lostPartitions().isEmpty());
        assertTrue(grid(2).cache(DEFAULT_CACHE_NAME).lostPartitions().isEmpty());

        final IgniteEx client = startGrid(3);

        stopGrid(1);

        final Set<Integer> lost1 = new HashSet<>(crd.cache(DEFAULT_CACHE_NAME).lostPartitions());
        final Set<Integer> lost2 = new HashSet<>(grid(2).cache(DEFAULT_CACHE_NAME).lostPartitions());
        final Set<Integer> lost3 = new HashSet<>(client.cache(DEFAULT_CACHE_NAME).lostPartitions());

        assertFalse(lost1.isEmpty());

        assertEquals(lost1, lost2);
        assertEquals(lost1, lost3);
    }

    /**
     * Tests if lost is correctly detected if addinity is loaded on cache start.
     */
    @Test
    public void testPartitionLossDetectionOnClientTopology_Persistent_NonAffCrd() throws Exception {
        dfltRegionPersistence = true;
        nonAffinityIdx = 0;

        final IgniteEx crd = startGrids(3);

        crd.cluster().active(true);

        assertTrue(grid(1).cache(DEFAULT_CACHE_NAME).lostPartitions().isEmpty());
        assertTrue(grid(2).cache(DEFAULT_CACHE_NAME).lostPartitions().isEmpty());

        final IgniteEx client = startGrid(3);

        stopGrid(1);

        final Set<Integer> lost1 = new HashSet<>(crd.cache(DEFAULT_CACHE_NAME).lostPartitions());
        final Set<Integer> lost2 = new HashSet<>(grid(2).cache(DEFAULT_CACHE_NAME).lostPartitions());
        final Set<Integer> lost3 = new HashSet<>(client.cache(DEFAULT_CACHE_NAME).lostPartitions());

        assertFalse(lost1.isEmpty());

        assertEquals(lost1, lost2);
        assertEquals(lost1, lost3);
    }

    /**
     * Tests if lost is correctly detected if affinity is loaded on cache start.
     */
    @Test
    public void testPartitionLossDetectionOnClientTopology_Persistent_NonAffCrd2() throws Exception {
        dfltRegionPersistence = true;
        nonAffinityIdx = 0;

        final IgniteEx crd = startGrids(3);

        crd.cluster().active(true);

        assertTrue(grid(1).cache(DEFAULT_CACHE_NAME).lostPartitions().isEmpty());
        assertTrue(grid(2).cache(DEFAULT_CACHE_NAME).lostPartitions().isEmpty());

        final IgniteEx g3 = startGrid(3);

        awaitPartitionMapExchange();

        stopGrid(1);

        final Set<Integer> lost1 = new HashSet<>(crd.cache(DEFAULT_CACHE_NAME).lostPartitions());
        final Set<Integer> lost2 = new HashSet<>(grid(2).cache(DEFAULT_CACHE_NAME).lostPartitions());
        final Set<Integer> lost3 = new HashSet<>(g3.cache(DEFAULT_CACHE_NAME).lostPartitions());

        assertFalse(lost1.isEmpty());

        assertEquals(lost1, lost2);
        assertEquals(lost1, lost3);

        GridDhtPartitionTopology top = startGrid(1).cachex(DEFAULT_CACHE_NAME).context().topology();
        assertEquals(lost1, top.lostPartitions());

        crd.resetLostPartitions(Collections.singleton(DEFAULT_CACHE_NAME));

        awaitPartitionMapExchange();
    }

    /**
     * Tests if lost is correctly detected if addinity is loaded on cache start.
     */
    @Test
    public void doTestPartitionLossDetectionOnClientTopology_Persistent_NonAffCrd() throws Exception {
        doTestPartitionLossDetectionOnClientTopology(true, 0, false, true);
    }

    /**
     * Tests if lost is correctly detected if addinity is loaded on cache start.
     * TODO broken
     */
    @Test
    public void doTestPartitionLossDetectionOnClientTopology_Persistent_NonAffCrd2() throws Exception {
        doTestPartitionLossDetectionOnClientTopology(true, 0, false, false);
    }

    /**
     *
     */
    @Test
    public void testResetOnLesserTopologyAfterRestart() throws Exception {
        backups = 1;
        dfltRegionPersistence = true;

        IgniteEx crd = startGrids(5);
        crd.cluster().active(true);

        stopAllGrids();

        crd = startGrids(2);
        crd.cluster().active(true);

        resetBaselineTopology();

        assertFalse(grid(0).cache(DEFAULT_CACHE_NAME).lostPartitions().isEmpty());
        assertFalse(grid(1).cache(DEFAULT_CACHE_NAME).lostPartitions().isEmpty());

        crd.resetLostPartitions(Collections.singleton(DEFAULT_CACHE_NAME));

        awaitPartitionMapExchange();
    }

    /**
     * Tests if lost is correctly detected if addinity is loaded on cache start.
     */
    private void doTestPartitionLossDetectionOnClientTopology(
            boolean dfltRegionPersistence,
            int nonAffinityIdx,
            boolean joinsClientNode,
            boolean startClientCache
            ) throws Exception {
        this.dfltRegionPersistence = dfltRegionPersistence;
        this.nonAffinityIdx = nonAffinityIdx;
        this.startClientCache = startClientCache;

        final int gridCnt = 3;

        final IgniteEx crd = startGrids(gridCnt);

        crd.cluster().active(true);

        for (int i = 0; i < gridCnt; i++) {
            if (i == nonAffinityIdx)
                continue;

            IgniteEx grid = grid(i);

            assertTrue(grid(1).cache(DEFAULT_CACHE_NAME).lostPartitions().isEmpty());
        }

        client = joinsClientNode;

        final IgniteEx g3 = startGrid(3);

        awaitPartitionMapExchange();

        boolean lossExpected = dfltRegionPersistence;

        Set<Integer> lostPartsExpected = lossExpected ?
                IntStream.of(crd.affinity(DEFAULT_CACHE_NAME).allPartitions(grid(1).localNode())).boxed().collect(Collectors.toSet()) :
                Collections.emptySet();

        stopGrid(1);
        startGrid(1);

        for (Ignite g0 : G.allGrids())
            assertEquals(g0.toString(), lostPartsExpected, g0.cache(DEFAULT_CACHE_NAME).lostPartitions());

        crd.resetLostPartitions(Collections.singleton(DEFAULT_CACHE_NAME));

        awaitPartitionMapExchange();
    }

    /**
     * Test correct partition loss detection for merged exchanges.
     *  @param merge {@code True} to enable persistence.
     * @param merge {@code True} to merge exchanges (also disables baseline for in-memory caches).
     */
    private void doTestPartitionLossDetectionOnNodeLeft(
        boolean persistence,
        boolean expectLostPartitions
    ) throws Exception {
        enableBaseline = false;
        this.dfltRegionPersistence = persistence;
        this.lossPlc = lossPlc;

        final int gridCnt = 5;

        final Ignite srv0 = startGrids(gridCnt);
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

        mergeExchangeWaitVersion(srv0, 8, null);

        Set<Integer> expLostParts = new HashSet<>();

        IntStream.of(2,3,4).forEach(new IntConsumer() {
            @Override public void accept(int idx) {
                int[] p0 = srv0.affinity(DEFAULT_CACHE_NAME).allPartitions(grid(idx).localNode());

                for (int p : p0)
                    expLostParts.add(p);
            }
        });

        IntStream.of(0, 1).forEach(new IntConsumer() {
            @Override public void accept(int idx) {
                expLostParts.removeIf(p ->
                        Arrays.binarySearch(srv0.affinity(DEFAULT_CACHE_NAME).allPartitions(grid(idx).localNode()), p) >= 0);
            }
        });

        stopGrid(getTestIgniteInstanceName(4), true, false);
        stopGrid(getTestIgniteInstanceName(3), true, false);
        stopGrid(getTestIgniteInstanceName(2), true, false);

        final AffinityTopologyVersion lastVer = new AffinityTopologyVersion(8, 0);

        waitForReadyTopology(internalCache(0, DEFAULT_CACHE_NAME).context().topology(), lastVer);
        waitForReadyTopology(internalCache(1, DEFAULT_CACHE_NAME).context().topology(), lastVer);

        final List<GridDhtPartitionTopology> tops = Arrays.asList(
            grid(0).cachex(DEFAULT_CACHE_NAME).context().topology(),
            grid(1).cachex(DEFAULT_CACHE_NAME).context().topology());

        for (int p = 0; p < PARTS_CNT; p++) {
            for (GridDhtPartitionTopology top : tops) {
                final GridDhtLocalPartition p0 = top.localPartition(p);

                if (p0 != null && p0.state() != GridDhtPartitionState.EVICTED) {
                    assertTrue(p0.state().toString(), !expectLostPartitions ? p0.state() == GridDhtPartitionState.OWNING :
                        !expLostParts.contains(p) || p0.state() == GridDhtPartitionState.LOST);
                }
            }
        }

        if (expectLostPartitions) {
            assertEquals(new HashSet<>(expLostParts), grid(0).cache(DEFAULT_CACHE_NAME).lostPartitions());
            assertEquals(new HashSet<>(expLostParts), grid(1).cache(DEFAULT_CACHE_NAME).lostPartitions());

            srv0.resetLostPartitions(Collections.singletonList(DEFAULT_CACHE_NAME));

            awaitPartitionMapExchange();
        }

        assertTrue(grid(0).cache(DEFAULT_CACHE_NAME).lostPartitions().isEmpty());
        assertTrue(grid(1).cache(DEFAULT_CACHE_NAME).lostPartitions().isEmpty());

        for (int p = 0; p < PARTS_CNT; p++) {
            for (GridDhtPartitionTopology top : tops) {
                final GridDhtLocalPartition p0 = top.localPartition(p);

                if (p0 != null && p0.state() != GridDhtPartitionState.EVICTED)
                    assertEquals(GridDhtPartitionState.OWNING, p0.state());
            }
        }

        if (!expectLostPartitions) {
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
