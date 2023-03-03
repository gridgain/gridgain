/*
 * Copyright 2023 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.commandline.util;

import org.apache.ignite.internal.client.GridClientCompute;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.client.GridClientNode;

import java.util.Comparator;
import java.util.Optional;
import java.util.UUID;

/**
 * Class provides utility methods to work with nodes in topology:
 * finding coordinator, filtering client nodes and so on.
 */
public final class TopologyUtils {
    /**
     * Obtains ID of coordinator node from the topology
     * visible through {@link GridClientCompute client compute} interface at the moment of method call.
     *
     * The method relies on the discovery implementation detail that coordinator role is always assigned
     * to the oldest server node in topology which means
     * the {@link GridClientNode#order() order} of the node is minimal among all alive server nodes in topology.
     *
     * @param compute {@link GridClientCompute} instance to obtain list of nodes in topology.
     * @return Id of coordinator node or null if topology is empty.
     * @throws GridClientException If client doesn't have an actual topology version.
     */
    public static UUID coordinatorId(GridClientCompute compute) throws GridClientException {
        return compute
            //Only non compute node can be coordinator.
            .nodes(node -> !node.isClient())
            .stream()
            .min(Comparator.comparingLong(GridClientNode::order))
            .map(GridClientNode::nodeId)
            .orElse(null);
    }

    /**
     * Obtains any connectable server node from the topology
     * visible through {@link GridClientCompute client compute} interface at the moment of method call.
     *
     * Method returns only a node which is connectable for the local client which means a direct p2p connection can be
     * established from the client to the node.
     *
     * @param compute {@link GridClientCompute} instance to obtain list of nodes in topology.
     * @return {@link Optional} object containing any connectable server node.
     * @throws GridClientException If client doesn't have an actual topology version.
     */
    public static Optional<GridClientNode> anyConnectableServerNode(GridClientCompute compute) throws GridClientException {
        return compute
            .nodes()
            .stream()
            .filter(n -> n.connectable() && !n.isClient())
            .findAny();
    }
}
