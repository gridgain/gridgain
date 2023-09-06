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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteClientDisconnectedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.internal.IgniteTooManyOpenFilesException;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.failure.FailureProcessor;
import org.apache.ignite.internal.util.IgniteExceptionRegistry;
import org.apache.ignite.internal.util.nio.GridCommunicationClient;
import org.apache.ignite.internal.util.nio.GridNioRecoveryDescriptor;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.nio.GridTcpNioCommunicationClient;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.internal.worker.WorkersRegistry;
import org.apache.ignite.spi.communication.tcp.AttributeNames;
import org.apache.ignite.spi.communication.tcp.messages.RecoveryLastReceivedMessage;

import static org.apache.ignite.failure.FailureType.CRITICAL_ERROR;
import static org.apache.ignite.failure.FailureType.SYSTEM_WORKER_TERMINATION;
import static org.apache.ignite.spi.communication.tcp.internal.CommunicationTcpUtils.usePairedConnections;

/**
 * Works with connections states.
 */
public class CommunicationWorker extends GridWorker {
    /** Worker name. */
    public static final String WORKER_NAME = "tcp-comm-worker";

    /** Config. */
    private final TcpCommunicationConfiguration cfg;

    /** Attributes. */
    private final AttributeNames attrs;

    /** */
    private final BlockingQueue<DisconnectedSessionInfo> disconnectRequestsQueue = new LinkedBlockingQueue<>();

    /** Client pool. */
    private final ConnectionClientPool clientPool;

    /** Failure processor supplier. */
    private final Supplier<FailureProcessor> failureProcessorSupplier;

    /** Node getter. */
    private final Function<UUID, ClusterNode> nodeGetter;

    /** Ping node. */
    private final Function<UUID, Boolean> pingNode;

    /** Exception registry supplier. */
    private final Supplier<IgniteExceptionRegistry> eRegistrySupplier;

    /** Nio server wrapper. */
    private final GridNioServerWrapper nioSrvWrapper;

    /** SPI name. */
    private final String spiName;

    /** Stopping flag (set to {@code true} when SPI gets stopping signal). */
    private volatile boolean stopping = false;

    /**
     * @param igniteInstanceName Ignite instance name.
     * @param log Logger.
     * @param cfg Config.
     * @param attrs Attributes.
     * @param clientPool Client pool.
     * @param failureProcessorSupplier Failure processor supplier.
     * @param nodeGetter Node getter.
     * @param pingNode Ping node.
     * @param eRegistrySupplier Exception registry supplier.
     * @param nioSrvWrapper Nio server wrapper.
     * @param workersRegistry Workers registry.
     * @param spiName Spi name.
     */
    public CommunicationWorker(
        String igniteInstanceName,
        IgniteLogger log,
        TcpCommunicationConfiguration cfg,
        AttributeNames attrs,
        ConnectionClientPool clientPool,
        Supplier<FailureProcessor> failureProcessorSupplier,
        Function<UUID, ClusterNode> nodeGetter,
        Function<UUID, Boolean> pingNode,
        Supplier<IgniteExceptionRegistry> eRegistrySupplier,
        GridNioServerWrapper nioSrvWrapper,
        WorkersRegistry workersRegistry,
        String spiName
    ) {
        super(igniteInstanceName, WORKER_NAME, log, workersRegistry);

        this.cfg = cfg;
        this.attrs = attrs;
        this.clientPool = clientPool;
        this.failureProcessorSupplier = failureProcessorSupplier;
        this.nodeGetter = nodeGetter;
        this.pingNode = pingNode;
        this.eRegistrySupplier = eRegistrySupplier;
        this.nioSrvWrapper = nioSrvWrapper;
        this.spiName = spiName;
    }

    /**
     * @param sesInfo Disconnected session information.
     */
    public void addProcessDisconnectRequest(DisconnectedSessionInfo sesInfo) {
        boolean add = disconnectRequestsQueue.add(sesInfo);

        assert add;
    }

    /**
     * Marks that instance must destroyed.
     */
    public void stop() {
        this.stopping = true;
    }

    /**
     * Connection stat processing.
     */
    @Override protected void body() throws InterruptedException {
        if (log.isDebugEnabled())
            log.debug("Tcp communication worker has been started.");

        Throwable err = null;

        long lastConnMaintenanceTs = U.currentTimeMillis();
        long lastAckSendingTs = U.currentTimeMillis();

        long awakeEachMs = Math.min(cfg.idleConnectionTimeout(), cfg.ackSendThresholdMillis());

        try {
            while (!isCancelled()) {
                DisconnectedSessionInfo disconnectRequest;

                blockingSectionBegin();

                try {
                    disconnectRequest = disconnectRequestsQueue.poll(awakeEachMs, TimeUnit.MILLISECONDS);
                }
                finally {
                    blockingSectionEnd();
                }

                if (disconnectRequest != null)
                    processDisconnect(disconnectRequest);

                long now = U.currentTimeMillis();
                if (now - lastConnMaintenanceTs > cfg.idleConnectionTimeout()) {
                    sendAcksAndDoMaintenance();

                    lastAckSendingTs = now;
                    lastConnMaintenanceTs = now;
                } else if (now - lastAckSendingTs > cfg.ackSendThresholdMillis()) {
                    sendAcks();

                    lastAckSendingTs = now;
                }

                onIdle();
            }
        }
        catch (Throwable t) {
            if (!(t instanceof InterruptedException))
                err = t;

            throw t;
        }
        finally {
            FailureProcessor failureProcessor = failureProcessorSupplier.get();

            if (failureProcessor != null) {
                if (err == null && !stopping)
                    err = new IllegalStateException("Thread  " + spiName + " is terminated unexpectedly.");

                if (err instanceof OutOfMemoryError)
                    failureProcessor.process(new FailureContext(CRITICAL_ERROR, err));
                else if (err != null)
                    failureProcessor.process(new FailureContext(SYSTEM_WORKER_TERMINATION, err));
            }
        }
    }

    /**
     * Sends acks for connections where there are unacked messages.
     */
    private void sendAcks() {
        sendAcksAndMaybeDoMaintenance(false);
    }

    /**
     * Sends acks for connections where there are unacked messages and does connection maintenance
     * closing stale clients and idle connections and cleaning up recovery descriptors.
     */
    private void sendAcksAndDoMaintenance() {
        sendAcksAndMaybeDoMaintenance(true);
    }

    /**
     * Sends acks for connections where there are unacked messages and (if requested) does connection maintenance
     * closing stale clients and idle connections and cleaning up recovery descriptors.
     *
     * @param doMaintenance Whether connection/client maintenance should be done.
     */
    private void sendAcksAndMaybeDoMaintenance(boolean doMaintenance) {
        if (doMaintenance)
            cleanupRecovery();

        for (Map.Entry<UUID, GridCommunicationClient[]> e : clientPool.entrySet()) {
            UUID nodeId = e.getKey();

            for (GridCommunicationClient client : e.getValue()) {
                if (client == null)
                    continue;

                ClusterNode node = nodeGetter.apply(nodeId);

                if (node == null) {
                    if (doMaintenance)
                        forceCloseAndRemove(client, nodeId);

                    continue;
                }

                GridNioRecoveryDescriptor recovery = null;

                if (!usingPairedConnectionsWith(node) && client instanceof GridTcpNioCommunicationClient) {
                    recovery = nioSrvWrapper.recoveryDescs().get(new ConnectionKey(
                        node.id(), client.connectionIndex(), -1)
                    );

                    if (recovery != null && recovery.lastAcknowledged() != recovery.received()) {
                        sendRecoveryAckOnTimeout(((GridTcpNioCommunicationClient) client).session(), recovery);

                        continue;
                    }
                }

                if (doMaintenance)
                    closeConnectionIfIdleAndHasNoUnackedMessages(nodeId, node, client, recovery);
            }
        }

        sendAcksOnSessionsUsingPairedConnections();
    }

    /***/
    private void forceCloseAndRemove(GridCommunicationClient client, UUID nodeId) {
        if (log.isDebugEnabled())
            log.debug("Forcing close of non-existent node connection: " + nodeId);

        client.forceClose();

        clientPool.removeNodeClient(nodeId, client);
    }

    /***/
    private boolean usingPairedConnectionsWith(ClusterNode node) {
        return cfg.usePairedConnections() && usePairedConnections(node, attrs.pairedConnection());
    }

    /***/
    private void sendRecoveryAckOnTimeout(GridNioSession ses, GridNioRecoveryDescriptor recovery) {
        RecoveryLastReceivedMessage msg = new RecoveryLastReceivedMessage(recovery.received());

        if (log.isDebugEnabled()) {
            log.debug("Send recovery acknowledgement on timeout [rmtNode=" + recovery.node().id() +
                ", rcvCnt=" + msg.received() +
                ", lastAcked=" + recovery.lastAcknowledged() + ']');
        }

        try {
            nioSrvWrapper.nio().sendSystem(ses, msg);

            recovery.lastAcknowledged(msg.received());
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to send message: " + e, e);
        }
    }

    /***/
    private void closeConnectionIfIdleAndHasNoUnackedMessages(
        UUID nodeId,
        ClusterNode node,
        GridCommunicationClient client,
        GridNioRecoveryDescriptor recovery
    ) {
        long idleTime = client.getIdleTime();

        if (idleTime < cfg.idleConnectionTimeout()) {
            return;
        }

        if (recovery == null && cfg.usePairedConnections() && usePairedConnections(node, attrs.pairedConnection()))
            recovery = nioSrvWrapper.outRecDescs().get(new ConnectionKey(
                node.id(), client.connectionIndex(), -1)
            );

        if (recovery != null &&
            recovery.nodeAlive(nodeGetter.apply(nodeId)) &&
            !recovery.messagesRequests().isEmpty()) {
            if (log.isDebugEnabled())
                log.debug("Node connection is idle, but there are unacknowledged messages, " +
                    "will wait: " + nodeId);

            return;
        }

        if (log.isDebugEnabled())
            log.debug("Closing idle node connection: " + nodeId);

        if (client.close() || client.closed())
            clientPool.removeNodeClient(nodeId, client);
    }

    /***/
    private void sendAcksOnSessionsUsingPairedConnections() {
        for (GridNioSession ses : nioSrvWrapper.nio().sessions()) {
            GridNioRecoveryDescriptor recovery = ses.inRecoveryDescriptor();

            if (recovery != null && usingPairedConnectionsWith(recovery.node())) {
                assert ses.accepted() : ses;

                if (recovery != null && recovery.lastAcknowledged() != recovery.received()) {
                    sendRecoveryAckOnTimeout(ses, recovery);
                }
            }
        }
    }

    /**
     * Cleanup recovery.
     */
    private void cleanupRecovery() {
        cleanupRecovery(nioSrvWrapper.recoveryDescs());
        cleanupRecovery(nioSrvWrapper.inRecDescs());
        cleanupRecovery(nioSrvWrapper.outRecDescs());
    }

    /**
     * @param recoveryDescs Recovery descriptors to cleanup.
     */
    private void cleanupRecovery(ConcurrentMap<ConnectionKey, GridNioRecoveryDescriptor> recoveryDescs) {
        Set<ConnectionKey> left = null;

        for (Map.Entry<ConnectionKey, GridNioRecoveryDescriptor> e : recoveryDescs.entrySet()) {
            if (left != null && left.contains(e.getKey()))
                continue;

            GridNioRecoveryDescriptor recoveryDesc = e.getValue();

            if (!recoveryDesc.nodeAlive(nodeGetter.apply(e.getKey().nodeId()))) {
                if (left == null)
                    left = new HashSet<>();

                left.add(e.getKey());
            }
        }

        if (left != null) {
            assert !left.isEmpty();

            for (ConnectionKey id : left) {
                GridNioRecoveryDescriptor recoveryDesc = recoveryDescs.get(id);

                if (recoveryDesc != null && recoveryDesc.onNodeLeft())
                    recoveryDescs.remove(id, recoveryDesc);
            }
        }
    }

    /**
     * @param sesInfo Disconnected session information.
     */
    private void processDisconnect(DisconnectedSessionInfo sesInfo) {
        GridNioRecoveryDescriptor recoveryDesc = sesInfo.recoveryDescription();

        ClusterNode node = recoveryDesc.node();

        if (!recoveryDesc.nodeAlive(nodeGetter.apply(node.id())))
            return;

        try {
            if (log.isDebugEnabled())
                log.debug("Recovery reconnect [rmtNode=" + recoveryDesc.node().id() + ']');

            GridCommunicationClient client = clientPool.reserveClient(node, sesInfo.connectionIndex());

            client.release();
        }
        catch (ClusterTopologyCheckedException e) {
            if (log.isDebugEnabled())
                log.debug("Recovery reconnect failed, node stopping [rmtNode=" + recoveryDesc.node().id() + ']');
        }
        catch (IgniteTooManyOpenFilesException e) {
            eRegistrySupplier.get().onException(e.getMessage(), e);

            throw e;
        }
        catch (IgniteCheckedException | IgniteException e) {
            try {
                if (recoveryDesc.nodeAlive(nodeGetter.apply(node.id())) && pingNode.apply(node.id())) {
                    if (log.isDebugEnabled()) {
                        log.debug("Recovery reconnect failed, will retry " +
                            "[rmtNode=" + recoveryDesc.node().id() + ", err=" + e + ']');
                    }

                    addProcessDisconnectRequest(sesInfo);
                }
                else {
                    if (log.isDebugEnabled()) {
                        log.debug("Recovery reconnect failed, " +
                            "node left [rmtNode=" + recoveryDesc.node().id() + ", err=" + e + ']');
                    }

                    eRegistrySupplier.get().onException("Recovery reconnect failed, node left [rmtNode=" + recoveryDesc.node().id() + "]",
                        e);
                }
            }
            catch (IgniteClientDisconnectedException ignored) {
                if (log.isDebugEnabled())
                    log.debug("Failed to ping node, client disconnected.");
            }
        }
    }
}
