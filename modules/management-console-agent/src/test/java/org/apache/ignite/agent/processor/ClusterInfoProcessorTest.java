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

package org.apache.ignite.agent.processor;

import java.util.Collection;
import java.util.Set;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.agent.AgentCommonAbstractTest;
import org.apache.ignite.agent.dto.cluster.ClusterInfo;
import org.apache.ignite.agent.dto.topology.Node;
import org.apache.ignite.agent.dto.topology.TopologySnapshot;
import org.apache.ignite.agent.utils.AgentUtils;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.cluster.IgniteClusterEx;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.junit.Test;

import static org.apache.ignite.agent.StompDestinationsUtils.buildClusterDest;
import static org.apache.ignite.agent.StompDestinationsUtils.buildClusterTopologyDest;

/**
 * Cluster info processor test.
 */
public class ClusterInfoProcessorTest extends AgentCommonAbstractTest {
    /**
     * Should send initial states to backend.
     */
    @Test
    public void shouldSendInitialStates() throws Exception {
        IgniteEx ignite = (IgniteEx) startGrid();

        changeManagementConsoleConfig(ignite);

        IgniteCluster cluster = ignite.cluster();

        cluster.active(true);

        assertWithPoll(() -> interceptor.getPayload(buildClusterTopologyDest(cluster.id())) != null);

        assertWithPoll(() -> {
            ClusterInfo info = interceptor.getPayload(buildClusterDest(cluster.id()), ClusterInfo.class);

            if (info == null)
                return false;

            Set<String> features = AgentUtils.getClusterFeatures(ignite.context(), cluster.nodes());

            assertEquals(cluster.id(), info.getId());
            assertEquals(cluster.tag(), info.getTag());
            assertEquals(cluster.baselineAutoAdjustTimeout(), info.getBaselineParameters().getAutoAdjustAwaitingTime());
            assertEquals(cluster.isBaselineAutoAdjustEnabled(), info.getBaselineParameters().isAutoAdjustEnabled());
            assertEquals(CU.isPersistenceEnabled(ignite.configuration()), info.isPersistenceEnabled());
            assertEquals(features, info.getFeatures());

            return true;
        });
    }

    /**
     * Should send changed cluster topology.
     */
    @Test
    public void shouldSendChangedClusterTopology() throws Exception {
        IgniteEx ignite = startGrid(0);

        changeManagementConsoleConfig(ignite);

        IgniteClusterEx cluster = ignite.cluster();

        cluster.active(true);

        startGrid(1);

        assertWithPoll(
            () -> {
                TopologySnapshot top = interceptor.getPayload(buildClusterTopologyDest(cluster.id()), TopologySnapshot.class);

                return top != null && top.getNodes().size() == 2;
            }
        );
    }

    /**
     * Should send changed baseline topology.
     */
    @Test
    public void shouldSendChangedTopologyWhenBaselineWasChanged() throws Exception {
        IgniteEx ignite_1 = startGrid(0);

        changeManagementConsoleConfig(ignite_1);

        ignite_1.cluster().baselineAutoAdjustEnabled(false);

        IgniteCluster cluster = ignite_1.cluster();

        cluster.active(true);

        assertWithPoll(
            () -> {
                TopologySnapshot top = interceptor.getPayload(buildClusterTopologyDest(cluster.id()), TopologySnapshot.class);

                return top != null && top.getNodes().stream().filter(Node::isBaselineNode).count() == 1;
            }
        );

        Ignite ignite_2 = startGrid(1);

        Collection<ClusterNode> nodes = ignite_1.cluster().forServers().nodes();

        ignite_1.cluster().setBaselineTopology(nodes);

        assertWithPoll(
            () -> {
                TopologySnapshot top = interceptor.getPayload(buildClusterTopologyDest(cluster.id()), TopologySnapshot.class);

                return top != null && top.getNodes().stream().filter(Node::isBaselineNode).count() == 2;
            }
        );
    }

    /**
     * Should send changed active state.
     */
    @Test
    public void shouldSendChangedActiveState() throws Exception {
        IgniteEx ignite_1 = startGrid(0);

        changeManagementConsoleConfig(ignite_1);

        IgniteCluster cluster = ignite_1.cluster();

        cluster.active(true);

        assertWithPoll(
            () -> {
                ClusterInfo info = interceptor.getPayload(buildClusterDest(cluster.id()), ClusterInfo.class);

                return info != null && info.isActive();
            }
        );

        cluster.active(false);

        assertWithPoll(
            () -> {
                ClusterInfo info = interceptor.getPayload(buildClusterDest(cluster.id()), ClusterInfo.class);

                return info != null && !info.isActive();
            }
        );
    }
}
