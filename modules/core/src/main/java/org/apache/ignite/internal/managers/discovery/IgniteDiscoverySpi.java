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

package org.apache.ignite.internal.managers.discovery;

import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteFeatures;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.DiscoverySpi;

/**
 *
 */
public interface IgniteDiscoverySpi extends DiscoverySpi {
    /**
     * Predicate that is always true. Might be used in
     * {@link IgniteDiscoverySpi#allNodesSupport(IgniteFeatures, IgnitePredicate)}.
     */
    IgnitePredicate<ClusterNode> ALL_NODES = node -> true;

    /**
     * @param nodeId Node ID.
     * @return {@code True} if node joining or already joined topology.
     */
    public boolean knownNode(UUID nodeId);

    /**
     *
     * @return {@code True} if SPI supports client reconnect.
     */
    public boolean clientReconnectSupported();

    /**
     *
     */
    public void clientReconnect();

    /**
     * @param feature Feature to check.
     * @return {@code true} if all nodes support the given feature.
     */
    public boolean allNodesSupport(IgniteFeatures feature);

    /**
     * @param feature Feature to check.
     * @param nodesPred Predicate to filter cluster nodes.
     * @return {@code true} if all nodes support the given feature.
     */
    public boolean allNodesSupport(IgniteFeatures feature, IgnitePredicate<ClusterNode> nodesPred);

    /**
     * For TESTING only.
     */
    public void simulateNodeFailure();

    /**
     * For TESTING only.
     *
     * @param lsnr Listener.
     */
    public void setInternalListener(IgniteDiscoverySpiInternalListener lsnr);

    /**
     * @return {@code True} if supports communication error resolve.
     */
    public boolean supportsCommunicationFailureResolve();

    /**
     * @param node Problem node.
     * @param err Connection error.
     */
    public void resolveCommunicationFailure(ClusterNode node, Exception err);
}
