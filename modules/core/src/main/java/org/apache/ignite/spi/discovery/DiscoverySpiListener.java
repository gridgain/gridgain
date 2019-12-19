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

package org.apache.ignite.spi.discovery;

import java.util.Collection;
import java.util.Map;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.lang.IgniteFuture;

/**
 * Listener for grid node discovery events. See {@link DiscoverySpi} for information on how grid nodes get discovered.
 */
public interface DiscoverySpiListener {
    /**
     * Notification of local node initialization. At the time this method is called, it is guaranteed that local node
     * consistent ID is available, but the discovery process is not started yet. This method should not block for a long
     * time since it blocks discovery.
     *
     * @param locNode Initialized local node.
     */
    void onLocalNodeInitialized(ClusterNode locNode);

    /**
     * Notification for grid node discovery events.
     *
     * @param type Node discovery event type. See {@link DiscoveryEvent}
     * @param topVer Topology version or {@code 0} if configured discovery SPI implementation does not support
     * versioning.
     * @param node Node affected (e.g. newly joined node, left node, failed node or local node).
     * @param topSnapshot Topology snapshot after event has been occurred (e.g. if event is {@code EVT_NODE_JOINED},
     * then joined node will be in snapshot).
     * @param topHist Topology snapshots history, {@code null} if first discovery event.
     * @param data Data for custom event, {@code null} if not a discovery event.
     * @return A future that will be completed when notification process has finished.
     * @deprecated Use {@link DiscoverySpiListener#onDiscovery(DiscoveryNotification)}
     */
    @Deprecated
    IgniteFuture<?> onDiscovery(
        int type,
        long topVer,
        ClusterNode node,
        Collection<ClusterNode> topSnapshot,
        Map<Long, Collection<ClusterNode>> topHist,
        DiscoverySpiCustomMessage data);

    /**
     * Notification for grid node discovery events.
     *
     * @param notification Discovery notification object.
     * @return A future that will be completed when notification process has finished.
     */
    default IgniteFuture<?> onDiscovery(DiscoveryNotification notification) {
        return onDiscovery(
            notification.type(),
            notification.getTopVer(),
            notification.getNode(),
            notification.getTopSnapshot(),
            notification.getTopHist(),
            notification.getCustomMsgData()
        );
    }
}