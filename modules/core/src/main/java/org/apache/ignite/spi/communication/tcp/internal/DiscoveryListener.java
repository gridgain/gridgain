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

package org.apache.ignite.spi.communication.tcp.internal;

import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.managers.eventstorage.HighPriorityListener;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationMetricsListener;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;

/**
 * Listener on discovery events.
 */
public class DiscoveryListener implements GridLocalEventListener, HighPriorityListener {
    /** Client pool. */
    private final ConnectionClientPool clientPool;

    /** Statistics. */
    private final TcpCommunicationMetricsListener metricsLsnr;

    /**
     * @param clientPool Client pool.
     * @param metricsLsnr Metrics listener.
     */
    public DiscoveryListener(
        ConnectionClientPool clientPool,
        TcpCommunicationMetricsListener metricsLsnr
    ) {
        this.clientPool = clientPool;
        this.metricsLsnr = metricsLsnr;
    }

    /** {@inheritDoc} */
    @Override public void onEvent(Event evt) {
        assert evt instanceof DiscoveryEvent : evt;
        assert evt.type() == EVT_NODE_LEFT || evt.type() == EVT_NODE_FAILED;

        ClusterNode node = ((DiscoveryEvent)evt).eventNode();

        onNodeLeft(node.consistentId(), node.id());
    }

    /** {@inheritDoc} */
    @Override public int order() {
        return 0;
    }

    /**
     * @param consistentId Consistent id of the node.
     * @param nodeId Left node ID.
     */
    private void onNodeLeft(Object consistentId, UUID nodeId) {
        assert nodeId != null;

        metricsLsnr.onNodeLeft(consistentId);

        clientPool.onNodeLeft(consistentId);
    }
}
