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

package org.apache.ignite.internal.processors.odbc;

import java.io.Closeable;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.ThinClientConfiguration;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.MarshallerContextImpl;
import org.apache.ignite.internal.binary.BinaryCachingMetadataHandler;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.binary.streams.BinaryHeapInputStream;
import org.apache.ignite.internal.binary.streams.BinaryHeapOutputStream;
import org.apache.ignite.internal.processors.authentication.AuthorizationContext;
import org.apache.ignite.internal.processors.authentication.IgniteAccessControlException;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedThinClientConfiguration;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcConnectionContext;
import org.apache.ignite.internal.processors.odbc.odbc.OdbcConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientStatus;
import org.apache.ignite.internal.processors.security.OperationSecurityContext;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.nio.GridNioFuture;
import org.apache.ignite.internal.util.nio.GridNioServerListenerAdapter;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.nio.GridNioSessionMetaKey;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.security.SecurityException;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.odbc.ClientListenerMetrics.clientTypeLabel;

/**
 * Client message listener.
 */
public class ClientListenerNioListener extends GridNioServerListenerAdapter<ClientMessage> {
    /** ODBC driver handshake code. */
    public static final byte ODBC_CLIENT = 0;

    /** JDBC driver handshake code. */
    public static final byte JDBC_CLIENT = 1;

    /** Thin client handshake code. */
    public static final byte THIN_CLIENT = 2;

    /** Connection handshake timeout task. */
    public static final int CONN_CTX_HANDSHAKE_TIMEOUT_TASK = GridNioSessionMetaKey.nextUniqueKey();

    /** Connection-related metadata key. */
    public static final int CONN_CTX_META_KEY = GridNioSessionMetaKey.nextUniqueKey();

    /** Connection-related metadata key. */
    public static final int CONN_STATE_META_KEY = GridNioSessionMetaKey.nextUniqueKey();

    /** Connection is not established. */
    public static final Integer CONN_STATE_DISCONNECTED = 0;

    /** Physical level connection is established. */
    public static final Integer CONN_STATE_PHYSICAL_CONNECTED = 1;

    /** Logical level connection is established. */
    public static final Integer CONN_STATE_HANDSHAKE_ACCEPTED = 2;

    /** Next connection id. */
    private static final AtomicInteger nextConnId = new AtomicInteger(1);

    /** Current count of active connections. */
    private static final AtomicInteger connectionsCnt = new AtomicInteger(0);

    /** Busy lock. */
    private final GridSpinBusyLock busyLock;

    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Maximum allowed cursors. */
    private final int maxCursors;

    /** Logger. */
    private final IgniteLogger log;

    /** Client connection config. */
    private final ClientConnectorConfiguration cliConnCfg;

    /** Distributed Thin Client config. */
    private final DistributedThinClientConfiguration distrThinCfg;

    /** Thin client configuration. */
    private final ThinClientConfiguration thinCfg;

    /** Metrics. */
    private final ClientListenerMetrics metrics;

    /**
     * Constructor.
     *
     * @param ctx          Context.
     * @param busyLock     Shutdown busy lock.
     * @param cliConnCfg   Client connector configuration.
     * @param distrThinCfg Distributed thin client configuration.
     */
    public ClientListenerNioListener(
        GridKernalContext ctx,
        GridSpinBusyLock busyLock,
        ClientConnectorConfiguration cliConnCfg,
        DistributedThinClientConfiguration distrThinCfg
    ) {
        assert cliConnCfg != null;
        assert distrThinCfg != null;

        this.ctx = ctx;
        this.busyLock = busyLock;
        this.cliConnCfg = cliConnCfg;
        this.distrThinCfg = distrThinCfg;

        maxCursors = cliConnCfg.getMaxOpenCursorsPerConnection();
        log = ctx.log(getClass());

        thinCfg = cliConnCfg.getThinClientConfiguration() == null ? new ThinClientConfiguration()
            : new ThinClientConfiguration(cliConnCfg.getThinClientConfiguration());

        metrics = new ClientListenerMetrics(ctx);
    }

    /** {@inheritDoc} */
    @Override public void onConnected(GridNioSession ses) {
        Integer connState = ses.meta(CONN_STATE_META_KEY);

        // It means connection was already processed.
        if (connState != null)
            return;

        ses.addMeta(CONN_STATE_META_KEY, CONN_STATE_PHYSICAL_CONNECTED);

        if (log.isDebugEnabled())
            log.debug("Client connected: " + ses.remoteAddress());

        long handshakeTimeout = cliConnCfg.getHandshakeTimeout();

        if (handshakeTimeout > 0)
            scheduleHandshakeTimeout(ses, handshakeTimeout);
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(GridNioSession ses, @Nullable Exception e) {
        Integer connState = ses.meta(CONN_STATE_META_KEY);

        // It means connection was never properly established or already processed.
        if (connState == null || connState.equals(CONN_STATE_DISCONNECTED))
            return;

        ses.addMeta(CONN_STATE_META_KEY, CONN_STATE_DISCONNECTED);
        if (connState.equals(CONN_STATE_HANDSHAKE_ACCEPTED))
            connectionsCnt.decrementAndGet();

        ClientListenerConnectionContext connCtx = ses.meta(CONN_CTX_META_KEY);

        if (connCtx != null) {
            connCtx.onDisconnected();

            metrics.onDisconnect(connCtx.clientType());
        }

        if (log.isDebugEnabled()) {
            if (e == null)
                log.debug("Client disconnected: " + ses.remoteAddress());
            else
                log.debug("Client disconnected due to an error [addr=" + ses.remoteAddress() + ", err=" + e + ']');
        }
    }

    /** {@inheritDoc} */
    @Override public void onMessage(GridNioSession ses, ClientMessage msg) {
        assert msg != null;

        ClientListenerConnectionContext connCtx = ses.meta(CONN_CTX_META_KEY);

        if (connCtx == null) {
            try {
                onHandshake(ses, msg);
            }
            catch (Exception e) {
                U.error(log, "Failed to handle handshake request " +
                    "(probably, connection has already been closed).", e);
            }

            return;
        }

        ClientListenerMessageParser parser = connCtx.parser();
        ClientListenerRequestHandler handler = connCtx.handler();

        ClientListenerRequest req;

        try {
            req = parser.decode(msg);
        }
        catch (Exception e) {
            try {
                handler.unregisterRequest(parser.decodeRequestId(msg));
            }
            catch (Exception e1) {
                U.error(log, "Failed to unregister request.", e1);
            }

            U.error(log, "Failed to parse client request.", e);

            ses.close();

            return;
        }

        assert req != null;

        try {
            long startTime;

            if (log.isTraceEnabled()) {
                startTime = System.nanoTime();

                log.trace("Client request received [reqId=" + req.requestId() + ", addr=" +
                    ses.remoteAddress() + ", req=" + req + ']');
            }
            else
                startTime = 0;

            AuthorizationContext authCtx = connCtx.authorizationContext();

            if (authCtx != null)
                AuthorizationContext.context(authCtx);

            ClientListenerResponse resp;

            try (OperationSecurityContext ignored = ctx.security().withContext(connCtx.securityContext())) {
                resp = handler.handle(req);

                if (resp != null) {
                    if (resp instanceof ClientListenerAsyncResponse) {
                        ((ClientListenerAsyncResponse)resp).future().listen(fut -> {
                            try {
                                handleResponse(req, fut.get(), startTime, ses, parser);
                            }
                            catch (Throwable e) {
                                handleError(req, e, ses, parser, handler);
                            }
                        });
                    }
                    else
                        handleResponse(req, resp, startTime, ses, parser);
                }
            }
            finally {
                if (authCtx != null)
                    AuthorizationContext.clear();
            }
        }
        catch (Throwable e) {
            handleError(req, e, ses, parser, handler);
        }
    }

    /** */
    private void handleResponse(
        ClientListenerRequest req,
        ClientListenerResponse resp,
        long startTime,
        GridNioSession ses,
        ClientListenerMessageParser parser
    ) {
        if (log.isTraceEnabled()) {
            long dur = (System.nanoTime() - startTime) / 1000;

            log.trace("Client request processed [reqId=" + req.requestId() + ", dur(mcs)=" + dur +
                ", resp=" + resp.status() + ']');
        }

        GridNioFuture<?> fut = ses.send(parser.encode(resp));

        fut.listen(f -> {
            if (f.error() == null)
                resp.onSent();
        });
    }

    /** */
    private void handleError(
        ClientListenerRequest req,
        Throwable e,
        GridNioSession ses,
        ClientListenerMessageParser parser,
        ClientListenerRequestHandler hnd
    ) {
        hnd.unregisterRequest(req.requestId());

        if (e instanceof Error)
            U.error(log, "Failed to process client request [req=" + req + ']', e);
        else
            U.warn(log, "Failed to process client request [req=" + req + ']', e);

        ses.send(parser.encode(hnd.handleException(e, req)));

        if (e instanceof Error)
            throw (Error)e;
    }

    /** {@inheritDoc} */
    @Override public void onSessionIdleTimeout(GridNioSession ses) {
        ses.close();
    }

    /** {@inheritDoc} */
    @Override public void onFailure(FailureType failureType, Throwable failure) {
        if (failure instanceof OutOfMemoryError)
            ctx.failure().process(new FailureContext(failureType, failure));
    }

    /**
     * Schedule handshake timeout.
     * @param ses Connection session.
     * @param handshakeTimeout Handshake timeout.
     */
    private void scheduleHandshakeTimeout(GridNioSession ses, long handshakeTimeout) {
        assert handshakeTimeout > 0;

        Closeable timeoutTask = ctx.timeout().schedule(new Runnable() {
            @Override public void run() {
                ses.close();

                metrics.onHandshakeTimeout();

                U.warn(log, "Unable to perform handshake within timeout " +
                    "[timeout=" + handshakeTimeout + ", remoteAddr=" + ses.remoteAddress() + ']');
            }
        }, handshakeTimeout, -1);

        ses.addMeta(CONN_CTX_HANDSHAKE_TIMEOUT_TASK, timeoutTask);
    }

    /**
     * Cancel handshake timeout task execution.
     * @param ses Connection session.
     */
    private void cancelHandshakeTimeout(GridNioSession ses) {
        Closeable timeoutTask = ses.removeMeta(CONN_CTX_HANDSHAKE_TIMEOUT_TASK);

        try {
            if (timeoutTask != null)
                timeoutTask.close();
        } catch (Exception e) {
            U.warn(log, "Failed to cancel handshake timeout task " +
                "[remoteAddr=" + ses.remoteAddress() + ", err=" + e + ']');
        }
    }

    /**
     * Perform handshake.
     *
     * @param ses Session.
     * @param msg Message bytes.
     */
    private void onHandshake(GridNioSession ses, ClientMessage msg) {
        BinaryContext ctx = new BinaryContext(BinaryCachingMetadataHandler.create(), new IgniteConfiguration(), null);

        BinaryMarshaller marsh = new BinaryMarshaller();

        marsh.setContext(new MarshallerContextImpl(null, null));

        ctx.configure(marsh, new BinaryConfiguration());

        BinaryReaderExImpl reader = new BinaryReaderExImpl(ctx, new BinaryHeapInputStream(msg.payload()), null, true);

        byte cmd = reader.readByte();

        if (cmd != ClientListenerRequest.HANDSHAKE) {
            U.warn(log, "Unexpected client request (will close session): " + ses.remoteAddress());

            ses.close();

            return;
        }

        short verMajor = reader.readShort();
        short verMinor = reader.readShort();
        short verMaintenance = reader.readShort();

        ClientListenerProtocolVersion ver = ClientListenerProtocolVersion.create(verMajor, verMinor, verMaintenance);

        BinaryWriterExImpl writer = new BinaryWriterExImpl(null, new BinaryHeapOutputStream(8), null, null);

        byte clientType = reader.readByte();

        ClientListenerConnectionContext connCtx = null;

        try {
            int maxConn = distrThinCfg.maxConnectionsPerNode();
            if (maxConn > 0 && connectionsCnt.get() >= maxConn)
                throw new IgniteCheckedException("Connection limit reached: " + maxConn);

            connCtx = prepareContext(clientType, ses);

            ensureClientPermissions(clientType);

            if (connCtx.isVersionSupported(ver)) {
                connCtx.initializeFromHandshake(ses, ver, reader);

                ses.addMeta(CONN_CTX_META_KEY, connCtx);
            }
            else
                throw new IgniteCheckedException("Unsupported version: " + ver.asString());

            cancelHandshakeTimeout(ses);

            connCtx.handler().writeHandshake(writer);

            ses.addMeta(CONN_STATE_META_KEY, CONN_STATE_HANDSHAKE_ACCEPTED);
            connectionsCnt.incrementAndGet();

            metrics.onHandshakeAccept(clientType);

            if (log.isDebugEnabled()) {
                String login = connCtx.securityContext() == null ? null :
                    connCtx.securityContext().subject().login().toString();

                log.debug("Client handshake accepted [rmtAddr=" + ses.remoteAddress() +
                    ", type=" + clientTypeLabel(clientType) + ", ver=" + ver.asString() +
                    ", login=" + login + ", connId=" + connCtx.connectionId() + ']');
            }
        }
        catch (IgniteAccessControlException | SecurityException authEx) {
            cancelHandshakeTimeout(ses);
            metrics.onFailedAuth();

            if (log.isDebugEnabled()) {
                log.debug("Client authentication failed [rmtAddr=" + ses.remoteAddress() +
                    ", type=" + clientTypeLabel(clientType) + ", ver=" + ver.asString() +
                    ", err=" + authEx.getMessage() + ']');
            }

            writer.writeBoolean(false);

            writer.writeShort((short)0);
            writer.writeShort((short)0);
            writer.writeShort((short)0);

            writer.doWriteString(authEx.getMessage());

            if (ver.compareTo(ClientConnectionContext.VER_1_1_0) >= 0)
                writer.writeInt(
                    authEx instanceof IgniteAccessControlException ? ClientStatus.AUTH_FAILED : ClientStatus.SECURITY_VIOLATION
                );
        }
        catch (IgniteCheckedException e) {
            U.warn(log, "Error during handshake [rmtAddr=" + ses.remoteAddress() +
                ", type=" + clientTypeLabel(clientType) + ", ver=" + ver.asString() + ", msg=" + e.getMessage() + ']');

            metrics.onGeneralReject();

            ClientListenerProtocolVersion currVer;

            if (connCtx == null)
                currVer = ClientListenerProtocolVersion.VER_UNKNOWN;
            else
                currVer = connCtx.defaultVersion();

            writer.writeBoolean(false);

            writer.writeShort(currVer.major());
            writer.writeShort(currVer.minor());
            writer.writeShort(currVer.maintenance());

            writer.doWriteString(e.getMessage());

            if (ver.compareTo(ClientConnectionContext.VER_1_1_0) >= 0)
                writer.writeInt(ClientStatus.FAILED);
        }

        ses.send(new ClientMessage(writer.array()));
    }

    /**
     * Prepare context.
     *
     * @param ses Client's NIO session.
     * @param clientType Client type.
     * @return Context.
     * @throws IgniteCheckedException If failed.
     */
    private ClientListenerConnectionContext prepareContext(byte clientType, GridNioSession ses)
        throws IgniteCheckedException {
        long connId = nextConnectionId();

        TimeZone timeZone = nodeTimeZoneId();

        switch (clientType) {
            case ODBC_CLIENT:
                return new OdbcConnectionContext(ctx, ses, busyLock, connId, maxCursors);

            case JDBC_CLIENT:
                return new JdbcConnectionContext(ctx, ses, busyLock, connId, maxCursors);

            case THIN_CLIENT:
                return new ClientConnectionContext(ctx, ses, connId, maxCursors, thinCfg, timeZone);
        }

        throw new IgniteCheckedException("Unknown client type: " + clientType);
    }

    /**
     * @return Node time zome identifier.
     */
    private TimeZone nodeTimeZoneId() {
        if (ctx.query().moduleEnabled())
            return ctx.query().getIndexing().clusterTimezone();
        else
            return TimeZone.getDefault();
    }

    /**
     * Generate unique connection id.
     * @return connection id.
     */
    private long nextConnectionId() {
        return (ctx.discovery().localNode().order() << 32) + nextConnId.getAndIncrement();
    }

    /**
     * Ensures if the given type of client is enabled by config.
     *
     * @param clientType Client type.
     * @throws IgniteCheckedException If failed.
     */
    private void ensureClientPermissions(byte clientType) throws IgniteCheckedException {
        switch (clientType) {
            case ODBC_CLIENT: {
                if (!cliConnCfg.isOdbcEnabled())
                    throw new IgniteCheckedException("ODBC connection is not allowed, " +
                        "see ClientConnectorConfiguration.odbcEnabled.");
                break;
            }

            case JDBC_CLIENT: {
                if (!cliConnCfg.isJdbcEnabled())
                    throw new IgniteCheckedException("JDBC connection is not allowed, " +
                        "see ClientConnectorConfiguration.jdbcEnabled.");

                break;
            }

            case THIN_CLIENT: {
                if (!cliConnCfg.isThinClientEnabled())
                    throw new IgniteCheckedException("Thin client connection is not allowed, " +
                        "see ClientConnectorConfiguration.thinClientEnabled.");

                break;
            }

            default:
                throw new IgniteCheckedException("Unknown client type: " + clientType);
        }
    }
}
