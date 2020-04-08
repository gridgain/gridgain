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
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryRequiredFeatureSupport;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.IgniteFeatures.INVERSE_TCP_CONNECTION;

/**
 * Message is part of communication via discovery protocol.
 *
 * It is used when a node (say node A) cannot establish a communication connection to other node (node B) in topology
 * due to firewall or network configuration and sends this message requesting inverse connection:
 * node B receives request and opens communication connection to node A
 * thus allowing both nodes to communicate to each other.
 */
@TcpDiscoveryRequiredFeatureSupport(feature = INVERSE_TCP_CONNECTION)
public class TcpConnectionRequestDiscoveryMessage implements DiscoveryCustomMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final IgniteUuid id = IgniteUuid.randomUuid();

    /** */
    @GridToStringInclude
    private final UUID receiverNodeId;

    /** */
    @GridToStringInclude
    private final int connIdx;

    /** */
    public TcpConnectionRequestDiscoveryMessage(UUID receiverNodeId, int connIdx) {
        this.receiverNodeId = receiverNodeId;
        this.connIdx = connIdx;
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid id() {
        return id;
    }

    /** */
    public UUID receiverNodeId() {
        return receiverNodeId;
    }

    /** */
    public int connectionIndex() {
        return connIdx;
    }

    /** {@inheritDoc} */
    @Nullable @Override public DiscoveryCustomMessage ackMessage() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean isMutable() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean stopProcess() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public DiscoCache createDiscoCache(
        GridDiscoveryManager mgr,
        AffinityTopologyVersion topVer,
        DiscoCache discoCache
    ) {
        throw new UnsupportedOperationException("createDiscoCache");
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TcpConnectionRequestDiscoveryMessage.class, this);
    }
}
