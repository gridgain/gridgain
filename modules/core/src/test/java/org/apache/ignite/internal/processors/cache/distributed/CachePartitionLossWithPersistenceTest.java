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

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Ignore;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.cache.PartitionLossPolicy.READ_WRITE_SAFE;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.EVICTED;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.LOST;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.OWNING;
import static org.apache.ignite.internal.processors.cache.persistence.CheckpointState.MARKER_STORED_TO_DISK;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/**
 *
 */
public class CachePartitionLossWithPersistenceTest extends GridCommonAbstractTest {
    /** */
    public static final int WAIT = 2_000;

    /** */
    private static final int PARTS_CNT = 32;

    /** */
    private PartitionLossPolicy lossPlc;

    /** Predicate that allows to block GridDhtPartitionSupplyMessage from the supplier node. */
    private IgniteBiPredicate<ClusterNode, Message> supplyMessagePred;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

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
            setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL).
            setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC).
            setPartitionLossPolicy(lossPlc).
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

    /**
     * Tests activation on partial baseline with lost partitions.
     */
    @Test
    public void testResetOnLesserTopologyAfterRestart() throws Exception {
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
     *
     */
    @Test
    @Ignore("https://ggsystems.atlassian.net/browse/GG-28521")
    public void testConsistencyAfterResettingLostPartitions_1() throws Exception {
        doTestConsistencyAfterResettingLostPartitions(0, false);
    }

    /**
     * Lagging node cannot be rebalanced from joining node.
     */
    @Test
    public void testConsistencyAfterResettingLostPartitions_2() throws Exception {
        doTestConsistencyAfterResettingLostPartitions(1, true);
    }

    /**
     *
     */
    @Test
    public void testConsistencyAfterResettingLostPartitions_3() throws Exception {
        doTestConsistencyAfterResettingLostPartitions(2, false);
    }

    /**
     * Tests data recovery after restarting the cluster with disabled WAL on rebalancing.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testConsistencyAfterClusterRestartWithDisabledWalOnRebalancing() throws Exception {
        doTestConsistencyAfterClusterRestartWithDisabledWalOnRebalancing(false);
    }

    /**
     * Tests data recovery after restarting the cluster in the case where one node of the cluster was stopped in the middle of checkpoint.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testConsistencyAfterClusterRestart() throws Exception {
        doTestConsistencyAfterClusterRestartWithDisabledWalOnRebalancing(true);
    }

    /**
     * Tests data recovery after restarting the cluster with disabled WAL on rebalancing.
     *
     * @throws Exception If failed.
     */
    public void doTestConsistencyAfterClusterRestartWithDisabledWalOnRebalancing(boolean stopInTheMiddleOfCheckpoint) throws Exception {
        lossPlc = READ_WRITE_SAFE;

        IgniteEx ig0 = startGrids(2);

        ig0.cluster().state(ACTIVE);

        IgniteCache<Integer, Integer> cache = ig0.cache(DEFAULT_CACHE_NAME);

        // Initial load.
        for (int k = 0; k < 2500; k++)
            cache.put(k, k);

        // Stop the second node and update data in the cache to trigger rebalancing when the node will be restarted.
        stopGrid(1);

        int owningPartId = 12;

        // Rewrite data to trigger further rebalance.
        for (int k = 2500; k < 5000; k++) {
            if (!stopInTheMiddleOfCheckpoint) {
                // Update all partitions in oder to disable WAL on rebalancing.
                // It is possible when there are no partitions in OWNING state.
                cache.put(k, k);
            }
            else {
                // Should skip one random partition to be sure that after restarting the second node,
                // it will have at least one partition in OWNING state, and so WAL will not be disabled while rebalancing.
                if (ig0.affinity(DEFAULT_CACHE_NAME).partition(k) != owningPartId)
                    cache.put(k, k);
            }
        }

        // Block supply messages to prevent partitions from being moved to the second node.
        TestRecordingCommunicationSpi spi0 = (TestRecordingCommunicationSpi) ig0.configuration().getCommunicationSpi();
        spi0.blockMessages(TestRecordingCommunicationSpi.blockSupplyMessageForGroup(CU.cacheId(DEFAULT_CACHE_NAME)));

        // Restart the second node.
        IgniteEx ig1 = startGrid(1);

        // Wait for exchange. All partitions should be moved to the MOVING state.
        ig1.context().cache().context().exchange().lastTopologyFuture().get();

        // Stopping the coordinator node leads to moving all partitions to the LOST state.
        stopGrid(0);

        for (int partId = 0; partId < PARTS_CNT; partId++) {
            if (stopInTheMiddleOfCheckpoint && partId == owningPartId)
                continue;

            checkLostPartitionAcrossCluster(DEFAULT_CACHE_NAME, partId);
        }

        assertEquals(
            "WAL should be " + (stopInTheMiddleOfCheckpoint ? "enabled" : "disabled") + " for the cache + " + DEFAULT_CACHE_NAME,
            stopInTheMiddleOfCheckpoint,
            ig1.context().cache().cacheGroup(CU.cacheId(DEFAULT_CACHE_NAME)).localWalEnabled());

        if (!stopInTheMiddleOfCheckpoint)
            stopGrid(1);
        else {
            String ig0Folder = ig1.context().pdsFolderResolver().resolveFolders().folderName();
            File dbDir = U.resolveWorkDirectory(ig1.configuration().getWorkDirectory(), "db", false);

            File ig0LfsDir = new File(dbDir, ig0Folder);
            File ig0CpDir = new File(ig0LfsDir, "cp");

            CountDownLatch stopLatch = new CountDownLatch(1);

            GridCacheDatabaseSharedManager dbMrg1 = (GridCacheDatabaseSharedManager) ig1.context().cache().context().database();
            dbMrg1.forceCheckpoint("test-checkpoint").futureFor(MARKER_STORED_TO_DISK).listen(f -> {
                runAsync(
                    () -> {
                        stopGrid(1, true);
                        stopLatch.countDown();
                    }
                );
            });

            assertTrue("Failed to stop the node in 10 sec.", stopLatch.await(10, SECONDS));

            // Make sure that the checkpoint end marker does not exixst.
            File[] cpMarkers = ig0CpDir.listFiles();

            for (File cpMark : cpMarkers) {
                if (cpMark.getName().contains("-END"))
                    cpMark.delete();
            }
        }

        // Restart the cluster and check that all partitions are restored.
        ig0 = startGrid(0);
        ig1 = startGrid(1);

        awaitPartitionMapExchange();

        // Check that there are no lost partitions.
        for (int partId = 0; partId < PARTS_CNT; partId++)
            checkNoLostPartitionAcrossCluster(DEFAULT_CACHE_NAME, partId);

        assertPartitionsSame(idleVerify(grid(0), DEFAULT_CACHE_NAME));

        // Check that WAL is enabled on all nodes.
        assertTrue(
            "WAL should be enabled for the cache + " + DEFAULT_CACHE_NAME,
            ig0.context().cache().cacheGroup(CU.cacheId(DEFAULT_CACHE_NAME)).localWalEnabled());
        assertTrue(
            "WAL should be enabled for the cache + " + DEFAULT_CACHE_NAME,
            ig0.context().cache().cacheGroup(CU.cacheId(DEFAULT_CACHE_NAME)).localWalEnabled());
    }

    /**
     * Test scenario: two nodes are left causing data loss and later returned to topology in various order.
     * <p>
     * Expected result: after resetting lost state partitions are synced and no data loss occured. No assertions happened
     * due to updates going to moving partitions.
     *
     * @param partResetMode Reset mode:
     * <ul>
     *     <li>0 - reset then only lagging node is returned.</li>
     *     <li>1 - reset then both nodes are returned.</li>
     *     <li>2 - reset then both nodes are returned and new node is added to baseline causing partition movement.</li>
     * </ul>
     * @param testPutToMovingPart {@code True} to try updating moving partition.
     */
    private void doTestConsistencyAfterResettingLostPartitions(int partResetMode, boolean testPutToMovingPart) throws Exception {
        lossPlc = READ_WRITE_SAFE;

        IgniteEx crd = startGrids(2);
        crd.cluster().active(true);

        startGrid(2);
        resetBaselineTopology();
        awaitPartitionMapExchange();

        // Find a lost partition which is primary for g1.
        int part = IntStream.range(0, PARTS_CNT).boxed().filter(new Predicate<Integer>() {
            @Override public boolean test(Integer p) {
                final List<ClusterNode> nodes = new ArrayList<>(crd.affinity(DEFAULT_CACHE_NAME).mapPartitionToPrimaryAndBackups(p));

                return nodes.get(0).equals(grid(1).localNode()) && nodes.get(1).equals(grid(2).localNode());
            }
        }).findFirst().orElseThrow(AssertionError::new);

        stopGrid(1);

        final IgniteInternalCache<Object, Object> cachex = crd.cachex(DEFAULT_CACHE_NAME);

        for (int p = 0; p < PARTS_CNT; p++)
            cachex.put(p, 0);

        stopGrid(2); // g1 now lags behind g2.

        final Collection<Integer> lostParts = crd.cache(DEFAULT_CACHE_NAME).lostPartitions();

        assertEquals(PARTS_CNT, cachex.context().topology().localPartitions().size() + lostParts.size());

        assertTrue(lostParts.contains(part));

        // Start lagging node first.
        final IgniteEx g1 = startGrid(1);

        final Collection<Integer> g1LostParts = g1.cache(DEFAULT_CACHE_NAME).lostPartitions();

        assertEquals(lostParts, g1LostParts);

        if (testPutToMovingPart) {
            // Block rebalancing from g2 to g1 to ensure a primary partition is in moving state.
            TestRecordingCommunicationSpi.spi(g1).blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
                @Override public boolean apply(ClusterNode node, Message msg) {
                    return msg instanceof GridDhtPartitionDemandMessage;
                }
            });
        }

        // Check that all lost partitions have the same state on all cluster nodes.
        for (Integer lostPart : lostParts)
            checkLostPartitionAcrossCluster(DEFAULT_CACHE_NAME, lostPart);

        if (partResetMode == 0)
            crd.resetLostPartitions(Collections.singleton(DEFAULT_CACHE_NAME));

        final IgniteEx g2 = startGrid(2);

        final Collection<Integer> g2LostParts = g2.cache(DEFAULT_CACHE_NAME).lostPartitions();

        if (partResetMode != 0) {
            // Check that all lost partitions have the same state on all cluster nodes.
            for (Integer lostPart : lostParts)
                checkLostPartitionAcrossCluster(DEFAULT_CACHE_NAME, lostPart);

            assertEquals(lostParts, g2LostParts);
        }

        if (partResetMode == 1)
            crd.resetLostPartitions(Collections.singleton(DEFAULT_CACHE_NAME));

        if (testPutToMovingPart) {
            TestRecordingCommunicationSpi.spi(g1).waitForBlocked();

            /** Try put to moving partition. Due to forced reassignment g2 should be a primary for the partition. */
            cachex.put(part, 0);

            TestRecordingCommunicationSpi.spi(g1).stopBlock();
        }

        if (partResetMode == 2) {
            final IgniteEx g3 = startGrid(3);

            resetBaselineTopology();

            crd.resetLostPartitions(Collections.singleton(DEFAULT_CACHE_NAME));
        }

        awaitPartitionMapExchange();

        assertPartitionsSame(idleVerify(crd, DEFAULT_CACHE_NAME));

        // Read validation.
        for (int p = 0; p < PARTS_CNT; p++) {
            for (Ignite ignite : G.allGrids())
                assertEquals("Partition " + p, 0, ignite.cache(DEFAULT_CACHE_NAME).get(p));
        }
    }

    /**
     * Checks the given partition has {@code LOST} state on all nodes in the cluster.
     *
     * @param cacheName Cache name to check.
     * @param partId Partition to check.
     */
    public static void checkLostPartitionAcrossCluster(String cacheName, int partId) {
        for (Ignite grid : G.allGrids()) {
            IgniteEx g = (IgniteEx)grid;

            GridDhtPartitionTopology top = g.context().cache().cacheGroup(CU.cacheId(cacheName)).topology();

            GridDhtLocalPartition p = top.localPartition(partId);

            if (p != null) {
                GridDhtPartitionState actualState = p.state();

                // check lost partitions
                if (!top.lostPartitions().contains(partId)) {
                    fail("Unexpected partition state [partId=" + partId + ", nodeId=" + g.localNode().id() +
                        ", actualPartState=" + actualState + ", expectedPartState=" + LOST + ", markedAsLost=false]");
                }

                // check actual partition state
                if (actualState != LOST && actualState != EVICTED) {
                    fail("Unexpected partition state [partId=" + partId + ", nodeId=" + g.localNode().id() +
                        ", actualPartState=" + actualState + ", expectedPartState=" + LOST + ", markedAsLost=true]");
                }

                // check node2part mapping
                for (Ignite node : G.allGrids()) {
                    IgniteEx n = (IgniteEx)node;

                    GridDhtPartitionState s = top.partitionState(n.localNode().id(), partId);

                    if (s != LOST && s != EVICTED) {
                        fail("Unexpected partition state [partId=" + partId + ", nodeId=" + g.localNode().id() +
                            ", node2partNodeId=" + n.localNode().id() +
                            ", node2partState=" + s + ", expectedPartState=" + LOST + ", markedAsLost=true]");
                    }
                }
            }
        }
    }

    /**
     * Checks that the given partition for the given cache is not in {@code LOST} state on any node.
     *
     * @param cacheName Cache name to check.
     * @param partId Partition to check.
     */
    public static void checkNoLostPartitionAcrossCluster(String cacheName, int partId) {
        for (Ignite grid : G.allGrids()) {
            IgniteEx g = (IgniteEx)grid;

            GridDhtPartitionTopology top = g.context().cache().cacheGroup(CU.cacheId(cacheName)).topology();

            if (!top.lostPartitions().isEmpty())
                fail("Unexpected partition states [lostParts=" + top.lostPartitions() + ']');

            GridDhtLocalPartition p = top.localPartition(partId);

            if (p != null) {
                GridDhtPartitionState actualState = p.state();

                // check actual partition state
                if (actualState == LOST) {
                    fail("Unexpected partition state [partId=" + partId + ", nodeId=" + g.localNode().id() +
                        ", actualPartState=" + actualState + ", expectedPartState=" + OWNING + ']');
                }

                // check node2part mapping
                for (Ignite node : G.allGrids()) {
                    IgniteEx n = (IgniteEx)node;

                    GridDhtPartitionState s = top.partitionState(n.localNode().id(), partId);

                    if (s == LOST) {
                        fail("Unexpected partition state [partId=" + partId + ", nodeId=" + g.localNode().id() +
                            ", node2partNodeId=" + n.localNode().id() +
                            ", node2partState=" + s + ", expectedPartState=" + OWNING + ", markedAsLost=true]");
                    }
                }
            }
        }
    }
}
