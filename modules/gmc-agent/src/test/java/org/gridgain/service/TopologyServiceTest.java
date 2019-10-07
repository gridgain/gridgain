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

import com.google.common.collect.Lists;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.cluster.IgniteClusterEx;
import org.apache.ignite.internal.managers.eventstorage.GridEventStorageManager;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.gridgain.TestDiscoveryMetricsProvider;
import org.gridgain.dto.topology.TopologySnapshot;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.gridgain.agent.StompDestinationsUtils.buildBaselineTopologyDest;
import static org.gridgain.agent.StompDestinationsUtils.buildClusterActiveStateDest;
import static org.gridgain.agent.StompDestinationsUtils.buildClusterTopologyDest;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Topology service test.
 */
public class TopologyServiceTest extends AbstractServiceTest {
    /**
     * Should send topology with cluster nodes.
     */
    @Test
    public void sendTopologyUpdate() {
        TopologyService srvc = new TopologyService(getMockContext(), mgr);

        srvc.sendTopologyUpdate(null, null);

        ArgumentCaptor<String> destCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Object> payloadCaptor = ArgumentCaptor.forClass(Object.class);
        verify(mgr, times(1)).send(destCaptor.capture(), payloadCaptor.capture());

        TopologySnapshot actualTop = (TopologySnapshot) payloadCaptor.getValue();

        Assert.assertEquals(buildClusterTopologyDest(UUID.fromString("a-a-a-a-a")), destCaptor.getValue());
        Assert.assertEquals(1, actualTop.getTopologyVersion());
        Assert.assertEquals(1, actualTop.getNodes().size());
        Assert.assertEquals(UUID.fromString("b-b-b-b-b"), actualTop.getNodes().get(0).getNodeId());
        Assert.assertEquals(UUID.fromString("c-c-c-c-c").toString(), actualTop.getNodes().get(0).getConsistentId());
        Assert.assertTrue(actualTop.getNodes().get(0).isClient());
    }

    /**
     * Should send baseline topology.
     */
    @Test
    public void sendBaseline() {
        TopologyService srvc = new TopologyService(getMockContext(), mgr);

        srvc.sendBaseline();

        ArgumentCaptor<String> destCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Object> payloadCaptor = ArgumentCaptor.forClass(Object.class);
        verify(mgr, times(1)).send(destCaptor.capture(), payloadCaptor.capture());

        TopologySnapshot actualTop = (TopologySnapshot) payloadCaptor.getValue();

        Assert.assertEquals(buildBaselineTopologyDest(UUID.fromString("a-a-a-a-a")), destCaptor.getValue());
        Assert.assertEquals(1, actualTop.getTopologyVersion());
        Assert.assertEquals(1, actualTop.getNodes().size());
        Assert.assertNull(actualTop.getNodes().get(0).getNodeId());
        Assert.assertEquals(UUID.fromString("d-d-d-d-d").toString(), actualTop.getNodes().get(0).getConsistentId());
        Assert.assertFalse(actualTop.getNodes().get(0).isClient());
    }

    /**
     * Should send cluster active state.
     */
    @Test
    public void sendClusterActiveState() {
        TopologyService srvc = new TopologyService(getMockContext(), mgr);

        srvc.sendClusterActiveState(null);

        ArgumentCaptor<String> destCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Object> payloadCaptor = ArgumentCaptor.forClass(Object.class);
        verify(mgr, times(1)).send(destCaptor.capture(), payloadCaptor.capture());

        Boolean activeState = (Boolean) payloadCaptor.getValue();

        Assert.assertEquals(buildClusterActiveStateDest(UUID.fromString("a-a-a-a-a")), destCaptor.getValue());
        Assert.assertTrue(activeState);
    }

    /** {@inheritDoc} */
    @Override protected GridKernalContext getMockContext() {
        GridKernalContext ctx = super.getMockContext();
        IgniteClusterEx cluster = ctx.grid().cluster();
        GridEventStorageManager evtMgr = mock(GridEventStorageManager.class);

        when(cluster.topologyVersion()).thenReturn(1L);
        when(cluster.active()).thenReturn(true);
        when(ctx.event()).thenReturn(evtMgr);

        Map<String, Object> attrs = new HashMap<>();
        attrs.put(IgniteNodeAttributes.ATTR_CLIENT_MODE, true);
        TcpDiscoveryNode clusterNode = new TcpDiscoveryNode(
                UUID.fromString("b-b-b-b-b"),
                Lists.newArrayList("127.0.0.1"),
                Collections.emptyList(),
                8080,
                new TestDiscoveryMetricsProvider(),
                IgniteProductVersion.fromString("1.2.3-0-DEV"),
                UUID.fromString("c-c-c-c-c").toString()
        );
        clusterNode.setAttributes(attrs);
        when(cluster.nodes()).thenReturn(Lists.newArrayList(clusterNode));

        TcpDiscoveryNode baseNode = new TcpDiscoveryNode(
                UUID.fromString("e-e-e-e-e"),
                Lists.newArrayList("127.0.0.1"),
                Collections.emptyList(),
                8080,
                new TestDiscoveryMetricsProvider(),
                IgniteProductVersion.fromString("1.2.3-0-DEV"),
                UUID.fromString("d-d-d-d-d").toString()
        );
        baseNode.setAttributes(attrs);
        when(cluster.currentBaselineTopology()).thenReturn(Lists.newArrayList(baseNode));

        return ctx;
    }
}
