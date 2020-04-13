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

package org.apache.ignite.agent.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.ignite.agent.dto.IgniteConfigurationWrapper;
import org.apache.ignite.agent.dto.NodeConfiguration;
import org.apache.ignite.agent.dto.metric.MetricResponse;
import org.apache.ignite.agent.dto.topology.Node;
import org.apache.ignite.agent.ws.WebSocketManager;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgnitionEx;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.ignite.agent.ManagementConsoleAgent.TOPIC_MANAGEMENT_CONSOLE;
import static org.apache.ignite.agent.StompDestinationsUtils.buildMetricsDest;
import static org.apache.ignite.internal.util.GridUnsafe.BYTE_ARR_OFF;
import static org.apache.ignite.internal.util.GridUnsafe.copyMemory;
import static org.apache.ignite.internal.util.GridUnsafe.getInt;
import static org.apache.ignite.internal.util.GridUnsafe.putInt;

/**
 * Utility class to generate face cluster.
 */
public class FakeUtils {
    /** */
    private static final int FAKE_NODES_CNT = 10;

    /** */
    private static final List<Node> FAKE_NODES = new ArrayList<>();

    /** */
    private static final int BASE_HEADER_SIZE = 54;

    /** */
    private static final int USER_TAG_SIZE_OFF = 50;

    /**
     * GG-28545 Generating fake nodes to simulate large cluster.
     *
     * @return List of fake nodes.
     */
    private synchronized static List<Node> fakeNodes() {
        if (FAKE_NODES.isEmpty()) {
            int order = 1;

            Map<String, Object> attrs = IgnitionEx.allGrids().get(0).cluster().localNode().attributes();

            for (int i = 1; i < FAKE_NODES_CNT; i++) {
                Node fakeNode = new Node(
                    UUID.randomUUID(),
                    UUID.randomUUID(),
                    ++order,
                    false,
                    true,
                    attrs
                );

                fakeNode.setOnline(true);
                fakeNode.setBaselineNode(true);

                FAKE_NODES.add(fakeNode);
            }
        }

        return FAKE_NODES;
    }

    /**
     * Add fake nodes to real nodes.
     *
     * @param nodes Real nodes.
     */
    public static void prepareFakeTopology(Map<String, Node> nodes) {
        // GG-28545 Generating fake nodes to simulate large cluster.
        for (Node fakeNode: fakeNodes())
            nodes.put(fakeNode.getConsistentId(), fakeNode);
    }

    /**
     * Export fake node configurations.
     *
     * @param ignite Ignite.
     * @param cfgWrapper Real config wrapper.
     * @param mapper JSON mapper.
     * @param oldestNode Oldest node.
     * @throws JsonProcessingException If failed to generate JSON for fake configuration.
     */
    public static void exportFakeNodeConfigs(
        IgniteEx ignite,
        IgniteConfigurationWrapper cfgWrapper,
        ObjectMapper mapper,
        ClusterGroup oldestNode
    ) throws JsonProcessingException {
        // GG-28545 Fake cluster.
        for (Node fakeNode: fakeNodes()) {
            String fakeConsistentId = fakeNode.getConsistentId();

            cfgWrapper.setConsistentId(fakeConsistentId);

            String fakeJson = mapper.writeValueAsString(cfgWrapper);

            NodeConfiguration fakeNodeCfg = new NodeConfiguration(fakeNode.getConsistentId(), fakeJson);

            ignite.message(oldestNode).send(TOPIC_MANAGEMENT_CONSOLE, fakeNodeCfg);
        }
    }

    /**
     * Send fake metrics.
     *
     * @param res Metrics.
     * @param mgr Websocket manager.
     */
    public static void sendFakeMetrics(MetricResponse res, WebSocketManager mgr) {
        // GG-28545 Fake metrics.
        byte[] realBody = res.body();

        for (Node fakeNode: fakeNodes()) {
            byte[] fakeBody = realBody.clone();

            byte[] consistentIdBytes = fakeNode.getConsistentId().getBytes(UTF_8);

            int off = BASE_HEADER_SIZE + getInt(fakeBody, BYTE_ARR_OFF + USER_TAG_SIZE_OFF);

            putInt(fakeBody, BYTE_ARR_OFF + off, consistentIdBytes.length);

            off += Integer.BYTES;

            copyMemory(consistentIdBytes, BYTE_ARR_OFF, fakeBody, BYTE_ARR_OFF + off, consistentIdBytes.length);

            mgr.send(buildMetricsDest(), res.body());
        }
    }
}
