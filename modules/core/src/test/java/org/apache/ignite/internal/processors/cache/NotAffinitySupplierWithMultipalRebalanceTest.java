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

package org.apache.ignite.internal.processors.cache;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD;
import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_PART_MISSED;

/**
 * The test is checking multiple demander supplying from non-affinity owner.
 */
public class NotAffinitySupplierWithMultipalRebalanceTest extends GridCommonAbstractTest {
    /** Start cluster nodes. */
    public static final int NODES_CNT = 3;

    /** Count of backup partitions. */
    public static final int BACKUPS = 2;

    /** New nodes count. */
    public static final int NEW_NODES = 3;

    /** Cache with randezvous affinity. */
    public static final String RENDEZVOUS_CACHE = DEFAULT_CACHE_NAME + "_rendezvous_aff";

    /** Cache with custom affinity. */
    public static final String CUSTOM_CACHE = DEFAULT_CACHE_NAME + "_specific_aff";

    /** Persistence enabled. */
    public boolean persistenceEnabled;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setConsistentId(igniteInstanceName)
            .setIncludeEventTypes(EVT_CACHE_REBALANCE_PART_MISSED)
            .setCommunicationSpi(new TestRecordingCommunicationSpi())
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(persistenceEnabled)))
            .setCacheConfiguration(
                new CacheConfiguration(RENDEZVOUS_CACHE)
                    .setBackups(BACKUPS),
                new CacheConfiguration(CUSTOM_CACHE)
                    .setBackups(BACKUPS)
                    .setAffinity(new TestAffinity(4)));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
        cleanPersistenceDir();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPersistenceFullRebalance() throws Exception {
        supplingFromOldBackup(true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testInMemoryFullRebalance() throws Exception {
        supplingFromOldBackup(false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_PDS_WAL_REBALANCE_THRESHOLD, value = "0")
    public void testPersistenceHistoricalRebalance() throws Exception {
        this.persistenceEnabled = true;

        IgniteEx ignite0 = startGrids(NODES_CNT);

        for (int i = 1; i < 4; i++)
            startGrid("new_" + i);

        ignite0.cluster().active(true);

        loadData(ignite0, RENDEZVOUS_CACHE);
        loadData(ignite0, CUSTOM_CACHE);

        ignite0.cluster().baselineAutoAdjustEnabled(false);

        awaitPartitionMapExchange();

        stopGrid("new_1");
        stopGrid("new_2");

        awaitPartitionMapExchange();

        loadData(ignite0, RENDEZVOUS_CACHE);
        loadData(ignite0, CUSTOM_CACHE);

        TestRecordingCommunicationSpi testCommunicationSpi1 = startNodeWithBlockingRebalance("new_1", true);
        TestRecordingCommunicationSpi testCommunicationSpi2 = startNodeWithBlockingRebalance("new_2", true);

        testCommunicationSpi1.waitForBlocked();
        testCommunicationSpi2.waitForBlocked();

        checkPartitionsState(GridDhtPartitionState.RENTING, RENDEZVOUS_CACHE);
        checkPartitionsState(GridDhtPartitionState.RENTING, CUSTOM_CACHE);

        AtomicBoolean hasMissed = new AtomicBoolean();

        for (Ignite ign : G.allGrids()) {
            ign.events().localListen(event -> {
                info("Partition missing event: " + event);

                hasMissed.compareAndSet(false, true);

                return false;
            }, EVT_CACHE_REBALANCE_PART_MISSED);
        }

        TestRecordingCommunicationSpi testCommunicationSpi0 = (TestRecordingCommunicationSpi)ignite0
            .configuration().getCommunicationSpi();

        testCommunicationSpi0.record(GridDhtPartitionsFullMessage.class);

        testCommunicationSpi1.stopBlock();

        testCommunicationSpi0.waitForRecorded();

        checkPartitionsState(GridDhtPartitionState.RENTING, CUSTOM_CACHE);

        testCommunicationSpi2.stopBlock();

        awaitPartitionMapExchange();

        checkPartitionsState(GridDhtPartitionState.MOVING, RENDEZVOUS_CACHE);
        checkPartitionsState(GridDhtPartitionState.MOVING, CUSTOM_CACHE);

        assertFalse(hasMissed.get());
    }

    /**
     * @throws Exception If failed.
     */
    public void supplingFromOldBackup(boolean persistenceEnabled) throws Exception {
        this.persistenceEnabled = persistenceEnabled;

        IgniteEx ignite0 = startGrids(NODES_CNT);

        ignite0.cluster().active(true);

        TestRecordingCommunicationSpi testCommunicationSpi0 = (TestRecordingCommunicationSpi)ignite0
            .configuration().getCommunicationSpi();

        loadData(ignite0, RENDEZVOUS_CACHE);
        loadData(ignite0, CUSTOM_CACHE);

        ignite0.cluster().baselineAutoAdjustEnabled(false);

        awaitPartitionMapExchange();

        TestRecordingCommunicationSpi testCommunicationSpi1 = startNodeWithBlockingRebalance("new_1", false);
        TestRecordingCommunicationSpi testCommunicationSpi2 = startNodeWithBlockingRebalance("new_2", false);
        TestRecordingCommunicationSpi testCommunicationSpi3 = startNodeWithBlockingRebalance("new_3", false);

        ignite0.cluster().setBaselineTopology(ignite0.cluster().topologyVersion());

        testCommunicationSpi1.waitForBlocked();
        testCommunicationSpi2.waitForBlocked();
        testCommunicationSpi3.waitForBlocked();

        checkPartitionsState(GridDhtPartitionState.RENTING, RENDEZVOUS_CACHE);
        checkPartitionsState(GridDhtPartitionState.RENTING, CUSTOM_CACHE);

        AtomicBoolean hasMissed = new AtomicBoolean();

        for (Ignite ign : G.allGrids()) {
            ign.events().localListen(event -> {
                info("Partition missing event: " + event);

                hasMissed.compareAndSet(false, true);

                return false;
            }, EVT_CACHE_REBALANCE_PART_MISSED);
        }

        testCommunicationSpi0.record(GridDhtPartitionsFullMessage.class);

        testCommunicationSpi1.stopBlock();
        testCommunicationSpi2.stopBlock();

        testCommunicationSpi0.waitForRecorded();

        checkPartitionsState(GridDhtPartitionState.RENTING, CUSTOM_CACHE);

        testCommunicationSpi3.stopBlock();

        awaitPartitionMapExchange();

        checkPartitionsState(GridDhtPartitionState.MOVING, RENDEZVOUS_CACHE);
        checkPartitionsState(GridDhtPartitionState.MOVING, CUSTOM_CACHE);

        assertFalse(hasMissed.get());
    }

    /**
     * Checks partitions state on all nodes by all caches.
     */
    private void checkPartitionsState(GridDhtPartitionState state, String cacheName) {
        for (Ignite ign : G.allGrids())
            checkPartitionState((IgniteEx)ign, state, cacheName);
    }

    /**
     * Checks a sate of partition on specific node.
     *
     * @param igniteEx Ignite.
     * @param state Partiton state.
     * @param cacheName Cache name.
     */
    private void checkPartitionState(IgniteEx igniteEx, GridDhtPartitionState state, String cacheName) {
        for (GridDhtLocalPartition p : igniteEx.cachex(cacheName).context().topology().currentLocalPartitions()) {
            assertTrue("Cache " + cacheName + " partiotn " + p.id() + " in " + state + " state on " + igniteEx.name(),
                p.state() != state);
        }
    }

    /**
     * @param name Node instance name.
     * @return Test communication SPI.
     * @throws Exception If failed.
     */
    @NotNull private TestRecordingCommunicationSpi startNodeWithBlockingRebalance(String name,
        boolean historicalRebalance) throws Exception {
        IgniteConfiguration cfg = optimize(getConfiguration(name));

        TestRecordingCommunicationSpi communicationSpi = (TestRecordingCommunicationSpi)cfg.getCommunicationSpi();

        communicationSpi.blockMessages((node, msg) -> {
            if (msg instanceof GridDhtPartitionDemandMessage) {
                GridDhtPartitionDemandMessage demandMessage = (GridDhtPartitionDemandMessage)msg;

                long rebalanceId = U.field(demandMessage, "rebalanceId");

                if ((CU.cacheId(RENDEZVOUS_CACHE) != demandMessage.groupId()
                    && CU.cacheId(CUSTOM_CACHE) != demandMessage.groupId())
                    || rebalanceId < 0)
                    return false;

                if (historicalRebalance)
                    assertTrue("Waited from historical rebalance, msg: " + demandMessage, demandMessage.partitions().hasHistorical());
                else
                    assertTrue("Waited from full rebalance, msg: " + demandMessage, demandMessage.partitions().hasFull());

                info("Message was caught: " + msg.getClass().getSimpleName()
                    + " to: " + node.consistentId()
                    + " by cache id: " + demandMessage.groupId());

                return true;
            }

            return false;
        });

        startGrid(cfg);

        return communicationSpi;
    }

    /**
     * @param ignite Ignite.
     * @param cacheName Cache name.
     */
    private void loadData(Ignite ignite, String cacheName) {
        try (IgniteDataStreamer streamer = ignite.dataStreamer(cacheName)) {
            streamer.allowOverwrite(true);

            for (int i = 0; i < 100; i++)
                streamer.addData(i, System.nanoTime());
        }
    }

    /** The test's affinity which mowing all partitions. */
    private static class TestAffinity extends RendezvousAffinityFunction {
        /** Count of whole partitions copy - primary and backups. */
        private static int WHOLE_PARTITIONS_COPY = BACKUPS + 1;

        /**
         * @param parts Partitions.
         */
        public TestAffinity(int parts) {
            super(false, parts);
        }

        /** {@inheritDoc} */
        @Override public List<ClusterNode> assignPartition(int part, List<ClusterNode> nodes, int backups,
            @Nullable Map<UUID, Collection<ClusterNode>> neighborhoodCache) {
            if (backups == BACKUPS && nodes.size() == NODES_CNT + NEW_NODES) {
                ClusterNode[] list = new ClusterNode[WHOLE_PARTITIONS_COPY];

                int ownerPosition = part % WHOLE_PARTITIONS_COPY;

                for (ClusterNode node : nodes) {
                    if (node.consistentId().equals("new_1"))
                        list[ownerPosition] = node;
                    else if (node.consistentId().equals("new_2"))
                        list[ownerPosition] = node;
                    else if (node.consistentId().equals("new_3"))
                        list[ownerPosition] = node;
                }

                if (isNodesCorrectAssigned(list))
                    return Arrays.asList(list);

            }
            else if (backups == BACKUPS && nodes.size() == NODES_CNT) {
                ClusterNode[] list = new ClusterNode[WHOLE_PARTITIONS_COPY];

                for (ClusterNode node : nodes) {
                    if (node.consistentId().equals("cache.NotAffinitySupplierWithMultipalRebalanceTest2"))
                        list[0] = node;
                    else if (node.consistentId().equals("cache.NotAffinitySupplierWithMultipalRebalanceTest1"))
                        list[1] = node;
                    else if (node.consistentId().equals("cache.NotAffinitySupplierWithMultipalRebalanceTest0"))
                        list[2] = node;
                }

                if (isNodesCorrectAssigned(list))
                    return Arrays.asList(list);
            }

            return super.assignPartition(part, nodes, backups, neighborhoodCache);
        }

        /**
         * @param list List of assigned nodes.
         * @return True is all nodes assignment, false otherwise.
         */
        private boolean isNodesCorrectAssigned(ClusterNode[] list) {
            for (int i = 0; i < list.length; i++) {
                if (list[i] == null)
                    return false;
            }

            return true;
        }
    }
}
