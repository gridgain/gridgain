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

import java.net.InetAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.managers.discovery.IgniteDiscoverySpi;
import org.apache.ignite.internal.processors.failure.FailureProcessor;
import org.apache.ignite.internal.processors.tracing.MTC;
import org.apache.ignite.internal.processors.tracing.Span;
import org.apache.ignite.internal.processors.tracing.SpanTags;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.nio.GridCommunicationClient;
import org.apache.ignite.internal.util.nio.GridNioMessageTracker;
import org.apache.ignite.internal.util.nio.GridNioRecoveryDescriptor;
import org.apache.ignite.internal.util.nio.GridNioServerListenerAdapter;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.nio.GridNioSessionMetaKey;
import org.apache.ignite.internal.util.nio.GridShmemCommunicationClient;
import org.apache.ignite.internal.util.nio.GridTcpNioCommunicationClient;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.communication.CommunicationListener;
import org.apache.ignite.spi.communication.CommunicationSpi;
import org.apache.ignite.spi.communication.tcp.AttributeNames;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationMetricsListener;
import org.apache.ignite.spi.communication.tcp.messages.ConnectionCheckMessage;
import org.apache.ignite.spi.communication.tcp.messages.HandshakeMessage;
import org.apache.ignite.spi.communication.tcp.messages.HandshakeWaitMessage;
import org.apache.ignite.spi.communication.tcp.messages.NodeIdMessage;
import org.apache.ignite.spi.communication.tcp.messages.RecoveryLastReceivedMessage;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_CHECK_COMMUNICATION_HANDSHAKE_MESSAGE_SENDER;
import static org.apache.ignite.IgniteSystemProperties.getBoolean;
import static org.apache.ignite.internal.processors.tracing.messages.TraceableMessagesTable.traceName;
import static org.apache.ignite.internal.util.IgniteUtils.spiAttribute;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.ATTR_ADDRS;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.ATTR_EXT_ADDRS;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.ATTR_HOST_NAMES;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.CONN_IDX_META;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.CONSISTENT_ID_META;
import static org.apache.ignite.spi.communication.tcp.internal.CommunicationTcpUtils.NOOP;
import static org.apache.ignite.spi.communication.tcp.internal.CommunicationTcpUtils.usePairedConnections;
import static org.apache.ignite.spi.communication.tcp.internal.TcpCommunicationConnectionCheckFuture.SES_FUT_META;
import static org.apache.ignite.spi.communication.tcp.messages.RecoveryLastReceivedMessage.ALREADY_CONNECTED;
import static org.apache.ignite.spi.communication.tcp.messages.RecoveryLastReceivedMessage.NEED_WAIT;
import static org.apache.ignite.spi.communication.tcp.messages.RecoveryLastReceivedMessage.NODE_STOPPING;
import static org.apache.ignite.spi.communication.tcp.messages.RecoveryLastReceivedMessage.UNKNOWN_NODE;

/**
 * This class implement NioListener which process handshake stage, and transmit messages to session.
 */
public class InboundConnectionHandler extends GridNioServerListenerAdapter<Message> {
    /**
     * Version when client is ready to wait to connect to server (could be needed when client tries to open connection
     * before it starts being visible for server)
     */
    private static final IgniteProductVersion VERSION_SINCE_CLIENT_COULD_WAIT_TO_CONNECT = IgniteProductVersion.fromString("2.1.4");

    /**
     * Enables additional check that sender of communication handshake message corresponds to the node id
     * included in the message. In other words, the remote address of sender is equal to the already known address
     * which corresponds to received node id in the message.
     */
    private final boolean checkCommHandshakeSender = getBoolean(IGNITE_CHECK_COMMUNICATION_HANDSHAKE_MESSAGE_SENDER);

    /** Message tracker meta for session. */
    private static final int TRACKER_META = GridNioSessionMetaKey.nextUniqueKey();

    /** Logger. */
    private final IgniteLogger log;

    /** Config. */
    private final TcpCommunicationConfiguration cfg;

    /** Node getter. */
    private final Function<UUID, ClusterNode> nodeGetter;

    /** Local node supplier. */
    private final Supplier<ClusterNode> locNodeSupplier;

    /** State provider. */
    private final ClusterStateProvider stateProvider;

    /** Client pool. */
    private ConnectionClientPool clientPool;

    /** Connect gate. */
    private final ConnectGateway connectGate;

    /** Failure processor supplier. */
    private final Supplier<FailureProcessor> failureProcessorSupplier;

    /** Attribute names. */
    private final AttributeNames attributeNames;

    /** Metrics listener supplier. */
    private final Supplier<TcpCommunicationMetricsListener> metricsLsnrSupplier;

    /** Context initialize latch. */
    private final CountDownLatch ctxInitLatch;

    /** Ignite ex supplier. */
    private final Supplier<Ignite> igniteExSupplier;

    /** SPI listener. */
    private final CommunicationListener<Message> lsnr;

    /** NIO server. */
    private volatile GridNioServerWrapper nioSrvWrapper;

    /** Communication worker. */
    private volatile CommunicationWorker commWorker;

    /** Client. */
    private final boolean client;

    /** Stopping flag (set to {@code true} when SPI gets stopping signal). */
    private volatile boolean stopping = false;

    /**
     * @param log Logger.
     * @param cfg Config.
     * @param nodeGetter Node getter.
     * @param locNodeSupplier Local node supplier.
     * @param stateProvider State provider.
     * @param clientPool Client pool.
     * @param commWorker Communication worker.
     * @param connectGate Connect gate.
     * @param failureProcessorSupplier Failure processor supplier.
     * @param attributeNames Attribute names.
     * @param metricsLsnrSupplier Metrics listener supplier.
     * @param nioSrvWrapper Nio server wrapper.
     * @param ctxInitLatch Context initialize latch.
     * @param client Client.
     * @param igniteExSupplier Returns already exists instance from spi.
     * @param lsnr Message listener
     */
    public InboundConnectionHandler(
        IgniteLogger log,
        TcpCommunicationConfiguration cfg,
        Function<UUID, ClusterNode> nodeGetter,
        Supplier<ClusterNode> locNodeSupplier, ClusterStateProvider stateProvider,
        ConnectionClientPool clientPool,
        CommunicationWorker commWorker,
        ConnectGateway connectGate,
        Supplier<FailureProcessor> failureProcessorSupplier,
        AttributeNames attributeNames,
        Supplier<TcpCommunicationMetricsListener> metricsLsnrSupplier,
        GridNioServerWrapper nioSrvWrapper,
        CountDownLatch ctxInitLatch,
        boolean client,
        Supplier<Ignite> igniteExSupplier,
        CommunicationListener<Message> lsnr
    ) {
        this.log = log;
        this.cfg = cfg;
        this.nodeGetter = nodeGetter;
        this.locNodeSupplier = locNodeSupplier;
        this.stateProvider = stateProvider;
        this.clientPool = clientPool;
        this.commWorker = commWorker;
        this.connectGate = connectGate;
        this.failureProcessorSupplier = failureProcessorSupplier;
        this.attributeNames = attributeNames;
        this.metricsLsnrSupplier = metricsLsnrSupplier;
        this.nioSrvWrapper = nioSrvWrapper;
        this.ctxInitLatch = ctxInitLatch;
        this.client = client;
        this.igniteExSupplier = igniteExSupplier;
        this.lsnr = lsnr;
    }

    /**
     * @param nioSrvWrapper Nio server wrapper.
     */
    public void setNioSrvWrapper(GridNioServerWrapper nioSrvWrapper) {
        this.nioSrvWrapper = nioSrvWrapper;
    }

    /**
     * @param pool Pool.
     */
    public void setClientPool(ConnectionClientPool pool) {
        this.clientPool = pool;
    }

    /** {@inheritDoc} */
    @Override public void onSessionWriteTimeout(GridNioSession ses) {
        LT.warn(log, "Communication SPI session write timed out (consider increasing " +
            "'socketWriteTimeout' " + "configuration property) [remoteAddr=" + ses.remoteAddress() +
            ", writeTimeout=" + cfg.socketWriteTimeout() + ']');

        if (log.isDebugEnabled()) {
            log.debug("Closing communication SPI session on write timeout [remoteAddr=" + ses.remoteAddress() +
                ", writeTimeout=" + cfg.socketWriteTimeout() + ']');
        }

        ses.close();
    }

    /** {@inheritDoc} */
    @Override public void onConnected(GridNioSession ses) {
        if (ses.accepted()) {
            if (log.isInfoEnabled()) {
                log.info("Accepted incoming communication connection [locAddr=" + ses.localAddress() +
                    ", rmtAddr=" + ses.remoteAddress() + ']');
            }

            try {
                if (client || ctxInitLatch.getCount() == 0 || !stateProvider.isHandshakeWaitSupported()) {
                    if (log.isDebugEnabled())
                        log.debug("Sending local node ID to newly accepted session: " + ses);

                    ses.sendNoFuture(stateProvider.nodeIdMessage(), null);
                }
                else {
                    if (log.isDebugEnabled())
                        log.debug("Sending handshake wait message to newly accepted session: " + ses);

                    ses.sendNoFuture(new HandshakeWaitMessage(), null);
                }
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to send message: " + e, e);
            }
        }
        else {
            if (log.isInfoEnabled())
                log.info("Established outgoing communication connection [locAddr=" + ses.localAddress() +
                    ", rmtAddr=" + ses.remoteAddress() + ']');
        }
    }

    /** {@inheritDoc} */
    @Override public void onMessageSent(GridNioSession ses, Message msg) {
        Object consistentId = ses.meta(CONSISTENT_ID_META);

        if (consistentId != null)
            metricsLsnrSupplier.get().onMessageSent(msg, consistentId);
    }

    /** {@inheritDoc} */
    @Override public void onMessage(final GridNioSession ses, Message msg) {
        Span span = MTC.span();

        span.addLog(() -> "Communication received");
        span.addTag(SpanTags.MESSAGE, () -> traceName(msg));

        ConnectionKey connKey = ses.meta(CONN_IDX_META);

        if (connKey == null) {
            assert ses.accepted() : ses;

            if (!connectGate.tryEnter()) {
                if (log.isDebugEnabled())
                    log.debug("Close incoming connection, failed to enter gateway.");

                ses.send(new RecoveryLastReceivedMessage(NODE_STOPPING)).listen(fut -> ses.close());

                return;
            }

            try {
                onFirstMessage(ses, msg);
            }
            finally {
                connectGate.leave();
            }
        }
        else {
            Object consistentId = ses.meta(CONSISTENT_ID_META);

            assert consistentId != null;

            if (msg instanceof RecoveryLastReceivedMessage) {
                metricsLsnrSupplier.get().onMessageReceived(msg, consistentId);

                GridNioRecoveryDescriptor recovery = ses.outRecoveryDescriptor();

                if (recovery != null) {
                    RecoveryLastReceivedMessage msg0 = (RecoveryLastReceivedMessage)msg;

                    if (log.isDebugEnabled()) {
                        log.debug("Received recovery acknowledgement [rmtNode=" + connKey.nodeId() +
                            ", connIdx=" + connKey.connectionIndex() +
                            ", rcvCnt=" + msg0.received() + ']');
                    }

                    recovery.ackReceived(msg0.received());
                }

                return;
            }
            else {
                GridNioRecoveryDescriptor recovery = ses.inRecoveryDescriptor();

                if (recovery != null) {
                    synchronized (recovery.receiveAndAckMonitor()) {
                        long rcvCnt = recovery.onReceived(ses.bytesReceived());

                        boolean ackByCountThresholdTriggered = rcvCnt % cfg.ackSendThreshold() == 0;
                        if (ackByCountThresholdTriggered || recovery.ackThresholdInBytesExceeded()) {
                            if (log.isDebugEnabled()) {
                                String reason = ackByCountThresholdTriggered ? "count" : "accrued size";

                                log.debug("Send recovery acknowledgement by " + reason + " threshold [rmtNode=" +
                                    connKey.nodeId() + ", connIdx=" + connKey.connectionIndex() +
                                    ", rcvCnt=" + rcvCnt + ']');
                            }

                            ses.systemMessage(new RecoveryLastReceivedMessage(rcvCnt));

                            recovery.lastAcknowledged(rcvCnt);
                        }
                    }
                }
                else if (connKey.dummy()) {
                    assert msg instanceof NodeIdMessage : msg;

                    TcpCommunicationNodeConnectionCheckFuture fut = ses.meta(SES_FUT_META);

                    assert fut != null : msg;

                    fut.onConnected(U.bytesToUuid(((NodeIdMessage)msg).nodeIdBytes(), 0));

                    nioSrvWrapper.nio().closeFromWorkerThread(ses);

                    return;
                }

                if (msg instanceof ConnectionCheckMessage) {
                    if (log.isDebugEnabled())
                        log.debug("Heartbeat message received [rmtNode=" + connKey.nodeId() +
                            ", connIdx=" + connKey.connectionIndex() + "]");

                    return;
                }
            }

            metricsLsnrSupplier.get().onMessageReceived(msg, consistentId);

            IgniteRunnable c;

            if (cfg.messageQueueLimit() > 0) {
                GridNioMessageTracker tracker = ses.meta(TRACKER_META);

                if (tracker == null) {
                    GridNioMessageTracker old = ses.addMeta(TRACKER_META, tracker =
                        new GridNioMessageTracker(ses, cfg.messageQueueLimit()));

                    assert old == null;
                }

                tracker.onMessageReceived();

                c = tracker;
            }
            else
                c = NOOP;

            lsnr.onMessage(connKey.nodeId(), msg, c);
        }
    }

    /** {@inheritDoc} */
    @Override public void onFailure(FailureType failureType, Throwable failure) {
        FailureProcessor failureProcessor = failureProcessorSupplier.get();

        if (failureProcessor != null)
            failureProcessor.process(new FailureContext(failureType, failure));
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(GridNioSession ses, @Nullable Exception e) {
        ConnectionKey connId = ses.meta(CONN_IDX_META);

        if (connId != null) {
            if (connId.dummy())
                return;

            UUID id = connId.nodeId();

            if (log.isDebugEnabled()) {
                String errMsg = e != null ? e.getMessage() : null;
                log.debug("The node was disconnected [nodeId=" + id + ", err=" + errMsg + "]");
            }

            GridCommunicationClient[] nodeClients = clientPool.clientFor(id);

            if (nodeClients != null) {
                for (GridCommunicationClient client : nodeClients) {
                    if (client instanceof GridTcpNioCommunicationClient &&
                        ((GridTcpNioCommunicationClient)client).session() == ses) {
                        client.close();

                        clientPool.removeNodeClient(id, client);
                    }
                }
            }

            if (!stopping) {
                GridNioRecoveryDescriptor outDesc = ses.outRecoveryDescriptor();

                if (outDesc != null) {
                    if (outDesc.nodeAlive(nodeGetter.apply(id))) {
                        if (!outDesc.messagesRequests().isEmpty()) {
                            if (log.isDebugEnabled()) {
                                log.debug("Session was closed but there are unacknowledged messages, " +
                                    "will try to reconnect [rmtNode=" + outDesc.node().id() + ']');
                            }

                            DisconnectedSessionInfo disconnectData =
                                new DisconnectedSessionInfo(outDesc, connId.connectionIndex());

                            commWorker.addProcessDisconnectRequest(disconnectData);
                        }
                    }
                    else
                        outDesc.onNodeLeft();
                }
            }

            CommunicationListener<Message> lsnr0 = lsnr;

            if (lsnr0 != null)
                lsnr0.onDisconnected(id);
        }
    }

    /**
     * Disable processing of incoming messages.
     */
    public void stop() {
        this.stopping = true;
    }

    /**
     * @param ses Session.
     * @param msg Message.
     */
    private void onFirstMessage(final GridNioSession ses, Message msg) {
        UUID sndId;

        ConnectionKey connKey;

        if (msg instanceof NodeIdMessage) {
            sndId = U.bytesToUuid(((NodeIdMessage)msg).nodeIdBytes(), 0);
            connKey = new ConnectionKey(sndId, 0, -1);
        }
        else {
            assert msg instanceof HandshakeMessage : msg;

            HandshakeMessage msg0 = (HandshakeMessage)msg;

            sndId = ((HandshakeMessage)msg).nodeId();
            connKey = new ConnectionKey(sndId, msg0.connectionIndex(), msg0.connectCount());
        }

        if (log.isDebugEnabled())
            log.debug("Remote node ID received: " + sndId);

        final ClusterNode rmtNode = nodeGetter.apply(sndId);

        if (rmtNode == null) {
            DiscoverySpi discoverySpi = igniteExSupplier.get().configuration().getDiscoverySpi();

            boolean unknownNode = true;

            if (discoverySpi instanceof TcpDiscoverySpi) {
                TcpDiscoverySpi tcpDiscoverySpi = (TcpDiscoverySpi)discoverySpi;

                ClusterNode node0 = tcpDiscoverySpi.getNode0(sndId);

                if (node0 != null) {
                    assert node0.isClient() : node0;

                    if (node0.version().compareTo(VERSION_SINCE_CLIENT_COULD_WAIT_TO_CONNECT) >= 0)
                        unknownNode = false;
                }
            }
            else if (discoverySpi instanceof IgniteDiscoverySpi)
                unknownNode = !((IgniteDiscoverySpi)discoverySpi).knownNode(sndId);

            if (unknownNode) {
                U.warn(log, "Close incoming connection, unknown node [nodeId=" + sndId + ", ses=" + ses + ']');

                ses.send(new RecoveryLastReceivedMessage(UNKNOWN_NODE)).listen(fut -> ses.close());
            }
            else
                ses.send(new RecoveryLastReceivedMessage(NEED_WAIT)).listen(fut -> ses.close());

            return;
        }
        else {
            if (checkCommHandshakeSender) {
                CommunicationSpi commSpi = igniteExSupplier.get().configuration().getCommunicationSpi();

                IgniteClosure<String, Collection<String>> notNullAttrs = (attr) -> {
                    Collection<String> r = rmtNode.attribute(spiAttribute(commSpi, attr));
                    if (F.isEmpty(r))
                        return Collections.emptyList();

                    return r;
                };

                Collection<String> rmtAddrs = notNullAttrs.apply(ATTR_ADDRS);
                Collection<String> rmtExtAddrs = notNullAttrs.apply(ATTR_EXT_ADDRS);
                Collection<String> rmtHosts = notNullAttrs.apply(ATTR_HOST_NAMES);

                InetAddress rmtInetAddr = ses.remoteAddress().getAddress();

                if (!rmtAddrs.contains(rmtInetAddr.getHostAddress()) && !rmtHosts.contains(rmtInetAddr.getHostName()) &&
                    !rmtExtAddrs.contains(rmtInetAddr.getHostAddress())) {

                    String knownAddrs = String.join(",", rmtAddrs);
                    String knownExtAddrs = String.join(",", rmtExtAddrs);
                    String knownHosts = String.join(",", rmtHosts);

                    U.warn(log, "Closing incoming connection, unexpected remote address " +
                        "[nodeId=" + sndId + ", addrs=[" + knownAddrs + ']' + ", extAddrs=[" + knownExtAddrs + ']' +
                        ", hosts=[" + knownHosts + ']' + ", ses=" + ses + ']');

                    ses.send(new RecoveryLastReceivedMessage(UNKNOWN_NODE)).listen(fut -> ses.close());

                    return;
                }
            }
        }

        ses.addMeta(CONSISTENT_ID_META, rmtNode.consistentId());

        final ConnectionKey old = ses.addMeta(CONN_IDX_META, connKey);

        assert old == null;

        ClusterNode locNode = locNodeSupplier.get();

        if (ses.remoteAddress() == null)
            return;

        assert msg instanceof HandshakeMessage : msg;

        HandshakeMessage msg0 = (HandshakeMessage)msg;

        if (log.isDebugEnabled()) {
            log.debug("Received handshake message [locNodeId=" + locNode.id() + ", rmtNodeId=" + sndId +
                ", msg=" + msg0 + ']');
        }

        if (cfg.usePairedConnections() && usePairedConnections(rmtNode, attributeNames.pairedConnection())) {
            final GridNioRecoveryDescriptor recoveryDesc = nioSrvWrapper.inRecoveryDescriptor(rmtNode, connKey);

            boolean reserve = recoveryDesc.tryReserve();

            if (reserve)
                connectedNew(recoveryDesc, ses, true);
            else {
                ses.send(new RecoveryLastReceivedMessage(ALREADY_CONNECTED));

                closeStaleConnections(connKey);
            }
        }
        else {
            assert connKey.connectionIndex() >= 0 : connKey;

            GridCommunicationClient[] curClients = clientPool.clientFor(sndId);

            GridCommunicationClient oldClient =
                curClients != null && connKey.connectionIndex() < curClients.length ?
                    curClients[connKey.connectionIndex()] :
                    null;

            boolean hasShmemClient = false;

            if (oldClient != null) {
                if (oldClient instanceof GridTcpNioCommunicationClient) {
                    if (log.isInfoEnabled())
                        log.info("Received incoming connection when already connected " +
                            "to this node, rejecting [locNode=" + locNode.id() +
                            ", rmtNode=" + sndId + ']');

                    ses.send(new RecoveryLastReceivedMessage(ALREADY_CONNECTED));

                    // We are sending ConectionCheckMessage through old session to validate if session is alive.
                    ((GridTcpNioCommunicationClient) oldClient).checkConnectionIfEnabled();

                    closeStaleConnections(connKey);

                    return;
                }
                else {
                    assert oldClient instanceof GridShmemCommunicationClient;

                    hasShmemClient = true;
                }
            }

            GridFutureAdapter<GridCommunicationClient> fut = new GridFutureAdapter<>();

            GridFutureAdapter<GridCommunicationClient> oldFut = clientPool.putIfAbsentFut(connKey, fut);

            final GridNioRecoveryDescriptor recoveryDesc = nioSrvWrapper.inRecoveryDescriptor(rmtNode, connKey);

            if (oldFut == null) {
                try {
                    curClients = clientPool.clientFor(sndId);

                    oldClient = curClients != null && connKey.connectionIndex() < curClients.length ?
                        curClients[connKey.connectionIndex()] : null;

                    if (oldClient != null) {
                        if (oldClient instanceof GridTcpNioCommunicationClient) {
                            assert oldClient.connectionIndex() == connKey.connectionIndex() : oldClient;

                            if (log.isInfoEnabled())
                                log.info("Received incoming connection when already connected " +
                                    "to this node, rejecting [locNode=" + locNode.id() +
                                    ", rmtNode=" + sndId + ']');

                            ses.send(new RecoveryLastReceivedMessage(ALREADY_CONNECTED));

                            closeStaleConnections(connKey);

                            fut.onDone(oldClient);

                            return;
                        }
                        else {
                            assert oldClient instanceof GridShmemCommunicationClient;

                            hasShmemClient = true;
                        }
                    }

                    boolean reserved = recoveryDesc.tryReserve();

                    if (log.isDebugEnabled()) {
                        log.debug("Received incoming connection from remote node " +
                            "[rmtNode=" + rmtNode.id() + ", reserved=" + reserved +
                            ", recovery=" + recoveryDesc + ']');
                    }

                    if (reserved) {
                        GridTcpNioCommunicationClient client =
                            connected(recoveryDesc, ses, rmtNode, msg0.received(), true, !hasShmemClient);

                        fut.onDone(client);
                    }
                    else {
                        ses.send(new RecoveryLastReceivedMessage(ALREADY_CONNECTED));

                        fut.onDone();
                    }
                }
                catch (Throwable e) {
                    log.warning("Communication connection isn't established due to exception [rmtNode=" + rmtNode.id()
                        + ", err=" + e.getMessage() + ']');

                    fut.onDone(e);
                }
                finally {
                    clientPool.removeFut(connKey, fut);
                }
            }
            else {
                if (oldFut instanceof ConnectFuture && locNode.order() < rmtNode.order()) {
                    ConnectFuture conFut = (ConnectFuture) oldFut;

                    int attemptNo = conFut.registerIncomingConnectionAttempt();

                    if (log.isInfoEnabled()) {
                        log.info("Received incoming connection from remote node while " +
                                "connecting to this node, rejecting [locNode=" + locNode.id() +
                                ", locNodeOrder=" + locNode.order() + ", rmtNode=" + rmtNode.id() +
                                ", rmtNodeOrder=" + rmtNode.order() + ", attemptNo=" + attemptNo + ']');
                    }

                    ses.send(new RecoveryLastReceivedMessage(ALREADY_CONNECTED));
                }
                else {
                    // The code below causes a race condition between shmem and TCP (see IGNITE-1294)
                    boolean reserved = recoveryDesc.tryReserve();

                    if (reserved) {
                        GridTcpNioCommunicationClient client =
                            connected(recoveryDesc, ses, rmtNode, msg0.received(), true, !hasShmemClient);

                        oldFut.onDone(client);

                        clientPool.removeFut(connKey, oldFut);
                    }
                    else
                        ses.send(new RecoveryLastReceivedMessage(ALREADY_CONNECTED));
                }
            }
        }
    }

    /**
     * @param connKey Connection key.
     */
    private void closeStaleConnections(ConnectionKey connKey) {
        for (GridNioSession ses0 : nioSrvWrapper.nio().sessions()) {
            ConnectionKey key0 = ses0.meta(CONN_IDX_META);

            if (key0 != null &&
                key0.nodeId().equals(connKey.nodeId()) &&
                key0.connectionIndex() == connKey.connectionIndex() &&
                key0.connectCount() < connKey.connectCount())
                ses0.close();
        }
    }

    /**
     * @param recovery Recovery descriptor.
     * @param ses Session.
     * @param node Node.
     * @param rcvCnt Number of received messages.
     * @param sndRes If {@code true} sends response for recovery handshake.
     * @param createClient If {@code true} creates NIO communication client.
     * @return Client.
     */
    @Nullable
    private GridTcpNioCommunicationClient connected(
        GridNioRecoveryDescriptor recovery,
        GridNioSession ses,
        ClusterNode node,
        long rcvCnt,
        boolean sndRes,
        boolean createClient) {
        ConnectionKey connKey = ses.meta(CONN_IDX_META);

        assert connKey != null && connKey.connectionIndex() >= 0 : connKey;
        assert !(cfg.usePairedConnections() && usePairedConnections(node, attributeNames.pairedConnection()));

        recovery.onHandshake(rcvCnt);

        ses.inRecoveryDescriptor(recovery);
        ses.outRecoveryDescriptor(recovery);

        nioSrvWrapper.nio().resend(ses);

        try {
            if (sndRes)
                nioSrvWrapper.nio().sendSystem(ses, new RecoveryLastReceivedMessage(recovery.received()));
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to send message: " + e, e);
        }

        recovery.onConnected();

        GridTcpNioCommunicationClient client = null;

        if (createClient) {
            boolean enableConnectionCheck = cfg.enableConnectionCheck()
                    && stateProvider.isTcpCommunicationConnectionCheckSupported(node);

            client = new GridTcpNioCommunicationClient(connKey.connectionIndex(), ses, log, enableConnectionCheck);

            clientPool.addNodeClient(node, connKey.connectionIndex(), client);
        }

        return client;
    }

    /**
     * @param recovery Recovery descriptor.
     * @param ses Session.
     * @param sndRes If {@code true} sends response for recovery handshake.
     */
    private void connectedNew(
        GridNioRecoveryDescriptor recovery,
        GridNioSession ses,
        boolean sndRes) {
        try {
            ses.inRecoveryDescriptor(recovery);

            if (sndRes)
                nioSrvWrapper.nio().sendSystem(ses, new RecoveryLastReceivedMessage(recovery.received()));

            recovery.onConnected();
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to send message: " + e, e);
        }
    }

    /**
     * @param commWorker New communication worker.
     */
    public void communicationWorker(CommunicationWorker commWorker) {
        this.commWorker = commWorker;
    }
}
