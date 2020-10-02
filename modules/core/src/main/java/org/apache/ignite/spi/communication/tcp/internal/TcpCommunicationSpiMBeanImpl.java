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

import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;

import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiMBeanAdapter;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationMetricsListener;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpiMBean;

/**
 * MBean implementation for TcpCommunicationSpi.
 */
public class TcpCommunicationSpiMBeanImpl extends IgniteSpiMBeanAdapter implements TcpCommunicationSpiMBean {
    /** Statistics. */
    private final Supplier<TcpCommunicationMetricsListener> metricLsnrSupplier;

    /** Config. */
    private final TcpCommunicationConfiguration cfg;

    /** State provider. */
    private final ClusterStateProvider stateProvider;
    
    /** {@inheritDoc} */
    public TcpCommunicationSpiMBeanImpl(
        IgniteSpiAdapter spiAdapter,
        Supplier<TcpCommunicationMetricsListener> metricLsnrSupplier,
        TcpCommunicationConfiguration cfg,
        ClusterStateProvider stateProvider
    ) {
        super(spiAdapter);
        this.metricLsnrSupplier = metricLsnrSupplier;
        this.cfg = cfg;
        this.stateProvider = stateProvider;
    }

    /** {@inheritDoc} */
    @Override public String getLocalAddress() {
        return cfg.localAddress();
    }

    /** {@inheritDoc} */
    @Override public int getLocalPort() {
        return cfg.localPort();
    }

    /** {@inheritDoc} */
    @Override public int getLocalPortRange() {
        return cfg.localPortRange();
    }

    /** {@inheritDoc} */
    @Override public boolean isUsePairedConnections() {
        return cfg.usePairedConnections();
    }

    /** {@inheritDoc} */
    @Override public int getConnectionsPerNode() {
        return cfg.connectionsPerNode();
    }

    /** {@inheritDoc} */
    @Override public int getSharedMemoryPort() {
        return cfg.shmemPort();
    }

    /** {@inheritDoc} */
    @Override public long getIdleConnectionTimeout() {
        return cfg.idleConnectionTimeout();
    }

    /** {@inheritDoc} */
    @Override public long getSocketWriteTimeout() {
        return cfg.socketWriteTimeout();
    }

    /** {@inheritDoc} */
    @Override public int getAckSendThreshold() {
        return cfg.ackSendThreshold();
    }

    /** {@inheritDoc} */
    @Override public int getUnacknowledgedMessagesBufferSize() {
        return cfg.unackedMsgsBufferSize();
    }

    /** {@inheritDoc} */
    @Override public long getConnectTimeout() {
        return cfg.connectionTimeout();
    }

    /** {@inheritDoc} */
    @Override public long getMaxConnectTimeout() {
        return cfg.maxConnectionTimeout();
    }

    /** {@inheritDoc} */
    @Override public int getReconnectCount() {
        return cfg.reconCount();
    }

    /** {@inheritDoc} */
    @Override public boolean isDirectBuffer() {
        return cfg.directBuffer();
    }

    /** {@inheritDoc} */
    @Override public boolean isDirectSendBuffer() {
        return cfg.directSendBuffer();
    }

    /** {@inheritDoc} */
    @Override public int getSelectorsCount() {
        return cfg.selectorsCount();
    }

    /** {@inheritDoc} */
    @Override public long getSelectorSpins() {
        return cfg.selectorSpins();
    }

    /** {@inheritDoc} */
    @Override public boolean isTcpNoDelay() {
        return cfg.tcpNoDelay();
    }

    /** {@inheritDoc} */
    @Override public int getSocketReceiveBuffer() {
        return cfg.socketReceiveBuffer();
    }

    /** {@inheritDoc} */
    @Override public int getSocketSendBuffer() {
        return cfg.socketSendBuffer();
    }

    /** {@inheritDoc} */
    @Override public int getMessageQueueLimit() {
        return cfg.messageQueueLimit();
    }

    /** {@inheritDoc} */
    @Override public int getSlowClientQueueLimit() {
        return cfg.slowClientQueueLimit();
    }

    /** {@inheritDoc} */
    @Override public void dumpStats() {
        stateProvider.dumpStats();
    }

    /** {@inheritDoc} */
    @Override public int getSentMessagesCount() {
        return metricLsnrSupplier.get().sentMessagesCount();
    }

    /** {@inheritDoc} */
    @Override public long getSentBytesCount() {
        return metricLsnrSupplier.get().sentBytesCount();
    }

    /** {@inheritDoc} */
    @Override public int getReceivedMessagesCount() {
        return metricLsnrSupplier.get().receivedMessagesCount();
    }

    /** {@inheritDoc} */
    @Override public long getReceivedBytesCount() {
        return metricLsnrSupplier.get().receivedBytesCount();
    }

    /** {@inheritDoc} */
    @Override public Map<String, Long> getReceivedMessagesByType() {
        return metricLsnrSupplier.get().receivedMessagesByType();
    }

    /** {@inheritDoc} */
    @Override public Map<UUID, Long> getReceivedMessagesByNode() {
        return metricLsnrSupplier.get().receivedMessagesByNode();
    }

    /** {@inheritDoc} */
    @Override public Map<String, Long> getSentMessagesByType() {
        return metricLsnrSupplier.get().sentMessagesByType();
    }

    /** {@inheritDoc} */
    @Override public Map<UUID, Long> getSentMessagesByNode() {
        return metricLsnrSupplier.get().sentMessagesByNode();
    }

    /** {@inheritDoc} */
    @Override public int getOutboundMessagesQueueSize() {
        return stateProvider.getOutboundMessagesQueueSize();
    }
}
