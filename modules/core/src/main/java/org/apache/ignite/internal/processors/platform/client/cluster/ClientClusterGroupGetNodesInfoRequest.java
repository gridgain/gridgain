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

package org.apache.ignite.internal.processors.platform.client.cluster;

import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.internal.cluster.IgniteClusterEx;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientRequest;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;

import java.util.Arrays;
import java.util.UUID;

/**
 * Cluster group get nodes information request.
 */
public class ClientClusterGroupGetNodesInfoRequest extends ClientRequest {
    /** Node ids. */
    private final UUID[] nodeIds;

    /**
     * Constructor.
     *
     * @param reader Reader.
     */
    public ClientClusterGroupGetNodesInfoRequest(BinaryRawReader reader) {
        super(reader);
        nodeIds = reader.readUuidArray();
    }

    @Override
    public ClientResponse process(ClientConnectionContext ctx) {
        // todo: is it ok to re-use cluster API?
        IgniteClusterEx cluster = ctx.kernalContext().grid().cluster();
        ClusterGroup clusterGroup = cluster.forNodeIds(Arrays.asList(nodeIds));
        return new ClientClusterGroupGetNodesInfoResponse(requestId(), clusterGroup.nodes());
    }
}
