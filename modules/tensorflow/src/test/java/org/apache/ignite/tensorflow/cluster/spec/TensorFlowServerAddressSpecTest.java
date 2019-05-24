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

package org.apache.ignite.tensorflow.cluster.spec;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.cluster.ClusterNode;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

/**
 * Tests for {@link TensorFlowServerAddressSpec}.
 */
public class TensorFlowServerAddressSpecTest {
    /** Node addresses. */
    private final String[][] allAddrs = new String[][] {
        {"192.168.1.1", "192.165.1.1"},
        {"192.164.2.1", "192.168.2.1"},
        {"192.168.3.1"}
    };

    /** */
    @Test
    public void testFormat() {
        Ignite ignite = mock(Ignite.class);
        IgniteCluster igniteCluster = mock(IgniteCluster.class);

        List<UUID> nodeIds = new ArrayList<>();
        List<ClusterNode> nodes = new ArrayList<>();
        for (String[] nodeAddresses : allAddrs) {
            UUID nodeId = UUID.randomUUID();
            ClusterNode node = mock(ClusterNode.class);

            nodeIds.add(nodeId);
            nodes.add(node);

            doReturn(nodeId).when(node).id();
            doReturn(Arrays.asList(nodeAddresses)).when(node).addresses();
            doReturn(node).when(igniteCluster).node(eq(nodeId));
        }

        doReturn(nodes).when(igniteCluster).nodes();
        doReturn(igniteCluster).when(ignite).cluster();

        assertEquals("192.168.1.1:42", new TensorFlowServerAddressSpec(nodeIds.get(0), 42).format(ignite));
        assertEquals("192.168.2.1:42", new TensorFlowServerAddressSpec(nodeIds.get(1), 42).format(ignite));
        assertEquals("192.168.3.1:42", new TensorFlowServerAddressSpec(nodeIds.get(2), 42).format(ignite));
    }
}
