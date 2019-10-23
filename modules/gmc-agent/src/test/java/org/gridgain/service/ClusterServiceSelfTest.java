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

package org.gridgain.service;

import java.util.Collection;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.cluster.IgniteClusterEx;
import org.gridgain.AbstractGridWithAgentTest;
import org.gridgain.dto.cluster.ClusterInfo;
import org.gridgain.dto.topology.TopologySnapshot;
import org.junit.Test;

import static org.gridgain.agent.StompDestinationsUtils.buildClusterDest;
import static org.gridgain.agent.StompDestinationsUtils.buildClusterTopologyDest;

/**
 * Topology service test.
 */
public class ClusterServiceSelfTest extends AbstractGridWithAgentTest {
    /**
     * Should send initial states to backend.
     */
    @Test
    public void shouldSendInitialStates() throws Exception {
        IgniteEx ignite = (IgniteEx) startGrid();
        changeGmcUri(ignite);

        IgniteCluster cluster = ignite.cluster();
        cluster.active(true);

        assertWithPoll(() -> interceptor.getPayload(buildClusterDest(cluster.id())) != null);
        assertWithPoll(() -> interceptor.getPayload(buildClusterTopologyDest(cluster.id())) != null);
    }

    /**
     * Should send changed cluster topology.
     */
    @Test
    public void shouldSendChangedClusterTopology() throws Exception {
        IgniteEx ignite = startGrid(0);
        changeGmcUri(ignite);

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
        changeGmcUri(ignite_1);
        ignite_1.cluster().baselineAutoAdjustEnabled(false);

        IgniteCluster cluster = ignite_1.cluster();
        cluster.active(true);

        assertWithPoll(
            () -> {
                TopologySnapshot top = interceptor.getPayload(buildClusterTopologyDest(cluster.id()), TopologySnapshot.class);
                return top != null && top.getNodes().size() == 1;
            }
        );

        Ignite ignite_2 = startGrid(1);

        Collection<ClusterNode> nodes = ignite_1.cluster().forServers().nodes();
        ignite_1.cluster().setBaselineTopology(nodes);

        assertWithPoll(
            () -> {
                TopologySnapshot top = interceptor.getPayload(buildClusterTopologyDest(cluster.id()), TopologySnapshot.class);
                return top != null && top.getNodes().size() == 2;
            }
        );
    }

    /**
     * Should send changed active state.
     */
    @Test
    public void shouldSendChangedActiveState() throws Exception {
        IgniteEx ignite_1 = startGrid(0);
        changeGmcUri(ignite_1);

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
