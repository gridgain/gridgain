/*
 * Copyright 2024 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.spi.discovery.tcp.internal;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteVersionUtils;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.spi.discovery.DiscoveryMetricsProvider;
import org.junit.Test;

import static com.google.common.collect.Lists.newArrayList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

/** Compacted topology history unit tests. */
public class CompactedTopologyHistorySelfTest {

    @SafeVarargs
    private final TcpDiscoveryNode newNode(UUID id, String consistentId, T2<String, Object>... attrs) {
        TcpDiscoveryNode node = new TcpDiscoveryNode(
                id,
                singletonList("127.0.0.1"),
                singletonList("localhost"),
                0,
                mock(DiscoveryMetricsProvider.class),
                IgniteVersionUtils.VER,
                consistentId
        );

        Map<String, Object> attrMap = new HashMap<>();

        for (T2<String, Object> attr : attrs) {
            attrMap.put(attr.getKey(), attr.getValue());
        }

        node.setAttributes(attrMap);

        return node;
    }

    private void assertTopologyHistoryEquals(Map<Long, Collection<ClusterNode>> expected, Map<Long, Collection<ClusterNode>> actual) {
        assertEquals(expected, actual); // Only checks map keys and node ids but not attributes.

        for (Map.Entry<Long, Collection<ClusterNode>> e : expected.entrySet()) {
            Long topVer = e.getKey();
            Map<UUID, TcpDiscoveryNode> historyNodes = e.getValue().stream()
                    .collect(Collectors.toMap(ClusterNode::id, node -> (TcpDiscoveryNode) node));

            Map<UUID, TcpDiscoveryNode> restoredNodes = actual.get(topVer).stream()
                    .collect(Collectors.toMap(ClusterNode::id, node -> (TcpDiscoveryNode) node));

            for (Map.Entry<UUID, TcpDiscoveryNode> entry : historyNodes.entrySet()) {
                UUID nodeId = entry.getKey();
                TcpDiscoveryNode historyNode = entry.getValue();
                TcpDiscoveryNode restoredNode = restoredNodes.get(nodeId);

                assertNodeEquals(historyNode, restoredNode);
            }
        }
    }

    private void assertNodeEquals(TcpDiscoveryNode expected, TcpDiscoveryNode actual) {
        assertEquals("Node id", expected.id(), actual.id());

        assertEquals("Node attributes", expected.getAttributes(), actual.getAttributes());

        assertEquals("Node addresses",expected.addresses(), actual.addresses());

        assertEquals("Node hostname", expected.hostNames(), actual.hostNames());

        assertEquals("Node discovery port", expected.discoveryPort(), actual.discoveryPort());

        assertEquals(expected.metrics(), actual.metrics());

        assertEquals(expected.order(), actual.order());

        assertEquals(expected.internalOrder(), actual.internalOrder());

        assertEquals(expected.clientRouterNodeId(), actual.clientRouterNodeId());
    }

    @Test
    public void testSameNodesLeaveAndJoinCluster() {
        Map<Long, Collection<ClusterNode>> history = new TreeMap<>();

        TcpDiscoveryNode n1 = newNode(
                UUID.randomUUID(),
                "n1",
                new T2<>("name", "node-1"),
                new T2<>("foo", "bar"),
                new T2<>("qwe", "xyz")
        );

        TcpDiscoveryNode n2 = newNode(
                UUID.randomUUID(),
                "n2",
                new T2<>("name", "node-2"),
                new T2<>("foo", "bar"),
                new T2<>("qwe", "xyz")
        );

        TcpDiscoveryNode n3 = newNode(
                UUID.randomUUID(),
                "n3",
                new T2<>("name", "node-3"),
                new T2<>("foo", "bar"),
                new T2<>("qwe", "xyz")
        );

        long lastTopVer = 1;

        history.put(lastTopVer++, newArrayList(n1));
        history.put(lastTopVer++, newArrayList(n1, n2));
        history.put(lastTopVer++, newArrayList(n1, n2, n3));

        history.put(lastTopVer++, newArrayList(n1, n2));
        history.put(lastTopVer++, newArrayList(n1, n2, n3));

        history.put(lastTopVer++, newArrayList(n2, n3));
        history.put(lastTopVer++, newArrayList(n2));
        history.put(lastTopVer++, newArrayList(n1, n2));
        history.put(lastTopVer++, newArrayList(n1, n2, n3));

        CompactedTopologyHistory compactedTopologyHistory = new CompactedTopologyHistory(history);

        assertTopologyHistoryEquals(history, compactedTopologyHistory.asMap());
    }

    @Test
    public void testNodeChangeTheirAttributes() {
        Map<Long, Collection<ClusterNode>> history = new TreeMap<>();

        TcpDiscoveryNode n1 = newNode(
                UUID.randomUUID(),
                "n1",
                new T2<>("name", "node-1"),
                new T2<>("foo", "bar"),
                new T2<>("qwe", "xyz")
        );

        TcpDiscoveryNode n2 = newNode(
                UUID.randomUUID(),
                "n2",
                new T2<>("name", "node-2"),
                new T2<>("foo", "bar"),
                new T2<>("qwe", "xyz")
        );

        TcpDiscoveryNode n3 = newNode(
                UUID.randomUUID(),
                "n3",
                new T2<>("name", "node-3"),
                new T2<>("foo", "bar"),
                new T2<>("qwe", "xyz")
        );

        TcpDiscoveryNode n1_addedAttribute = newNode(
                UUID.randomUUID(),
                "n1",
                new T2<>("name", "node-1"),
                new T2<>("foo", "bar"),
                new T2<>("qwe", "xyz"),
                new T2<>("abc", "42")
        );

        TcpDiscoveryNode n2_changedAttribute = newNode(
                UUID.randomUUID(),
                "n2",
                new T2<>("name", "node-2"),
                new T2<>("foo", "bar"),
                new T2<>("qwe", "XYZ")
        );

        TcpDiscoveryNode n3_removedAttribute = newNode(
                UUID.randomUUID(),
                "n3",
                new T2<>("name", "node-3"),
                new T2<>("foo", "bar")
        );

        long lastTopVer = 1;

        history.put(lastTopVer++, newArrayList(n1));
        history.put(lastTopVer++, newArrayList(n1, n2));
        history.put(lastTopVer++, newArrayList(n1, n2, n3));

        history.put(lastTopVer++, newArrayList(n1, n2));
        history.put(lastTopVer++, newArrayList(n1, n2, n3_removedAttribute));

        history.put(lastTopVer++, newArrayList(n2, n3_removedAttribute));
        history.put(lastTopVer++, newArrayList(n2));
        history.put(lastTopVer++, newArrayList(n1_addedAttribute, n2));
        history.put(lastTopVer++, newArrayList(n1_addedAttribute, n2, n3));

        history.put(lastTopVer++, newArrayList(n1_addedAttribute, n3));
        history.put(lastTopVer++, newArrayList(n1_addedAttribute));
        history.put(lastTopVer++, newArrayList(n1_addedAttribute, n3_removedAttribute));
        history.put(lastTopVer++, newArrayList(n1_addedAttribute, n2_changedAttribute, n3_removedAttribute));

        CompactedTopologyHistory compacted = new CompactedTopologyHistory(history);

        assertTopologyHistoryEquals(history, compacted.asMap());
    }
}
