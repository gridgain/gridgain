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
import java.util.HashSet;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.CacheRebalancingEvent;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.testframework.GridTestUtils.mergeExchangeWaitVersion;

/**
 * Tests if lost partitions are same on left nodes after other owners removal.
 */
public class CachePartitionLossDetectionOnNodeLeftTest extends GridCommonAbstractTest {
    /** */
    private PartitionLossPolicy lossPlc;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        cfg.setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME).
            setAtomicityMode(TRANSACTIONAL).
            setCacheMode(PARTITIONED).
            setPartitionLossPolicy(lossPlc).
            setAffinity(new RendezvousAffinityFunction(false, 32)).
            setBackups(0));

        cfg.setIncludeEventTypes(EventType.EVTS_ALL);

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
     * Test correct partition loss detection for merged exchanges.
     */
    @Test
    public void testPartitionLossDetectionOnNodeLeft_Safe_Merge() throws Exception {
        doTestPartitionLossDetectionOnNodeLeft(PartitionLossPolicy.READ_WRITE_SAFE, true);
    }

    /**
     * Test correct partition loss detection for merged exchanges.
     *
     * @param lossPlc Loss policy.
     * @param merge {@code True} to merge exchanges.
     */
    private void doTestPartitionLossDetectionOnNodeLeft(PartitionLossPolicy lossPlc, boolean merge) throws Exception {
        this.lossPlc = lossPlc;

        final Ignite srv0 = startGrids(5);

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

        if (merge)
            waitForReadyTopology(internalCache(1, DEFAULT_CACHE_NAME).context().topology(), new AffinityTopologyVersion(8, 0));

        printPartitionState(DEFAULT_CACHE_NAME, 0);

        if (lossPlc != PartitionLossPolicy.IGNORE) {
            assertEquals(new HashSet<>(expLostParts), grid(0).cache(DEFAULT_CACHE_NAME).lostPartitions());
            assertEquals(new HashSet<>(expLostParts), grid(1).cache(DEFAULT_CACHE_NAME).lostPartitions());

            srv0.resetLostPartitions(Collections.singletonList(DEFAULT_CACHE_NAME));
        }
        else {
            assertTrue(grid(0).cache(DEFAULT_CACHE_NAME).lostPartitions().isEmpty());
            assertTrue(grid(1).cache(DEFAULT_CACHE_NAME).lostPartitions().isEmpty());
        }

        // Event must be fired only once for any mode.
        assertEquals("Node0", expLostParts, lostEvt0);
        assertEquals("Node1", expLostParts, lostEvt1);

        assertTrue(grid(0).cache(DEFAULT_CACHE_NAME).lostPartitions().isEmpty());
        assertTrue(grid(1).cache(DEFAULT_CACHE_NAME).lostPartitions().isEmpty());
    }
}
