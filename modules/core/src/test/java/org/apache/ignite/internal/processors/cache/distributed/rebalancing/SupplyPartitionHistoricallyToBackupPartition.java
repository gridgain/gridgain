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

package org.apache.ignite.internal.processors.cache.distributed.rebalancing;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.affinity.AffinityFunctionContext;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicSingleUpdateRequest;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Supplay a partition from a beckup in a cluster.
 */
@WithSystemProperty(key = IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD, value = "0")
public class SupplyPartitionHistoricallyToBackupPartition extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCommunicationSpi(new TestRecordingCommunicationSpi())
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setMaxSize(200L * 1024 * 1024)
                    .setPersistenceEnabled(true)))
            .setConsistentId(igniteInstanceName)
            .setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME)
                .setAffinity(new TestAffinity(getTestIgniteInstanceName(0), getTestIgniteInstanceName(1))));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNothingSupply() throws Exception {
        restartsBackupWithoutReorderedUpdate(false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSupplyLatestUpdates() throws Exception {
        restartsBackupWithoutReorderedUpdate(true);
    }

    private void restartsBackupWithoutReorderedUpdate(boolean loadDataAfterCheckpoint) throws Exception {
        IgniteEx ignite0 = startGrids(2);

        ignite0.cluster().state(ClusterState.ACTIVE);

        IgniteCache cache = ignite0.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 10; i++)
            cache.put(i, i);

        AtomicBoolean blocked = new AtomicBoolean();

        TestRecordingCommunicationSpi.spi(ignite0).blockMessages((node, msg) -> {
            info("Msg: " + msg.getClass().getSimpleName() + " to node: " + node.consistentId());

            return msg instanceof GridDhtAtomicSingleUpdateRequest &&
                getTestIgniteInstanceName(1).equals(node.consistentId()) &&
                blocked.compareAndSet(false, true);
        });

        for (int i = 10; i < 20; i++)
            cache.put(i, i);

        TestRecordingCommunicationSpi.spi(ignite0).waitForBlocked();

        forceCheckpoint();

        if (loadDataAfterCheckpoint) {
            for (int i = 20; i < 30; i++)
                cache.put(i, i);
        }

        stopGrid(1);

        TestRecordingCommunicationSpi.spi(ignite0).stopBlock();

        startGrid(1);

        awaitPartitionMapExchange();

        assertPartitionsSame(idleVerify(ignite0, DEFAULT_CACHE_NAME));
    }

    /**
     * Tets affinity function with one partition. This implementation maps primary partition to first node and backup
     * partition to second.
     */
    public static class TestAffinity extends RendezvousAffinityFunction {
        /** Nodes consistence ids. */
        String[] nodeConsistentIds;

        /**
         * @param nodes Nodes consistence ids.
         */
        public TestAffinity(String... nodes) {
            super(false, 1);

            this.nodeConsistentIds = nodes;
        }

        /** {@inheritDoc} */
        @Override public List<List<ClusterNode>> assignPartitions(AffinityFunctionContext affCtx) {
            int nodes = affCtx.currentTopologySnapshot().size();

            if (nodes != 2)
                return super.assignPartitions(affCtx);

            List<List<ClusterNode>> assignment = new ArrayList<>();

            assignment.add(new ArrayList<>(2));

            assignment.get(0).add(null);
            assignment.get(0).add(null);

            for (ClusterNode node : affCtx.currentTopologySnapshot())
                if (nodeConsistentIds[0].equals(node.consistentId()))
                    assignment.get(0).set(0, node);
                else if (nodeConsistentIds[1].equals(node.consistentId()))
                    assignment.get(0).set(1, node);
                else
                    throw new AssertionError("Unexpected node consistent id is " + node.consistentId());

            return assignment;
        }
    }
}
