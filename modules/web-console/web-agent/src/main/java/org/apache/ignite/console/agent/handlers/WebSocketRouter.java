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

package org.apache.ignite.console.agent.handlers;

import java.net.ConnectException;
import java.net.URI;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.console.agent.AgentConfiguration;
import org.apache.ignite.console.websocket.AgentHandshakeRequest;
import org.apache.ignite.console.websocket.AgentHandshakeResponse;
import org.apache.ignite.console.websocket.WebSocketEvent;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.logger.slf4j.Slf4jLogger;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.StatusCode;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketError;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketFrame;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.eclipse.jetty.websocket.api.extensions.Frame;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.slf4j.LoggerFactory;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.ignite.console.agent.AgentUtils.configureProxy;
import static org.apache.ignite.console.agent.AgentUtils.secured;
import static org.apache.ignite.console.agent.AgentUtils.sslContextFactory;
import static org.apache.ignite.console.json.JsonUtils.fromJson;
import static org.apache.ignite.console.websocket.WebSocketConsts.AGENTS_PATH;
import static org.apache.ignite.console.websocket.WebSocketConsts.AGENT_HANDSHAKE;
import static org.apache.ignite.console.websocket.WebSocketConsts.AGENT_REVOKE_TOKEN;
import static org.apache.ignite.console.websocket.WebSocketConsts.NODE_REST;
import static org.apache.ignite.console.websocket.WebSocketConsts.NODE_VISOR;
import static org.apache.ignite.console.websocket.WebSocketConsts.SCHEMA_IMPORT_DRIVERS;
import static org.apache.ignite.console.websocket.WebSocketConsts.SCHEMA_IMPORT_METADATA;
import static org.apache.ignite.console.websocket.WebSocketConsts.SCHEMA_IMPORT_SCHEMAS;

/**
 * Router that listen for web socket and redirect messages to event bus.
 */
@WebSocket(maxTextMessageSize = 10 * 1024 * 1024, maxBinaryMessageSize = 10 * 1024 * 1024)
public class WebSocketRouter implements AutoCloseable {
    /** */
    private static final IgniteLogger log = new Slf4jLogger(LoggerFactory.getLogger(WebSocketRouter.class));

    /** */
    private static final ByteBuffer PONG_MSG = UTF_8.encode("PONG");

    /** */
    private final CountDownLatch closeLatch = new CountDownLatch(1);

    /** */
    private final WebSocketSession wss = new WebSocketSession();

    /** Agent configuration. */
    private final AgentConfiguration cfg;

    /** Http client. */
    private final HttpClient httpClient;

    /** Web Socket Client. */
    private WebSocketClient client;

    /** */
    private final ClusterHandler clusterHnd;

    /** */
    private final DatabaseHandler dbHnd;

    /** */
    private int reconnectCnt;

    /**
     * @param cfg Configuration.
     */
    public WebSocketRouter(AgentConfiguration cfg) {
        this.cfg = cfg;

        httpClient = new HttpClient(createSslFactory(cfg));
        
        configureProxy(httpClient, cfg.serverUri());

        clusterHnd = new ClusterHandler(cfg, wss);
        dbHnd = new DatabaseHandler(cfg, wss);
    }

    /**
     * @param cfg Config.
     */
    private static SslContextFactory createSslFactory(AgentConfiguration cfg) {
        boolean trustAll = Boolean.getBoolean("trust.all");

        if (trustAll && !F.isEmpty(cfg.serverTrustStore())) {
            log.warning("Options contains both '--server-trust-store' and '-Dtrust.all=true'. " +
                "Option '-Dtrust.all=true' will be ignored on connect to Web server.");

            trustAll = false;
        }

        boolean ssl = trustAll || !F.isEmpty(cfg.serverTrustStore()) || !F.isEmpty(cfg.serverKeyStore());

        if (!ssl)
            return null;

        return sslContextFactory(
            cfg.serverKeyStore(),
            cfg.serverKeyStorePassword(),
            trustAll,
            cfg.serverTrustStore(),
            cfg.serverTrustStorePassword(),
            cfg.cipherSuites()
        );
    }

    /**
     * Start websocket client.
     */
    public void start() throws Exception {
        log.info("Starting Web Console Agent...");
        log.info("Connecting to: " + cfg.serverUri());

        httpClient.start();

        try {
            reconnect();
        }
        catch (Throwable e) {
            log.error("Failed to connect to the server", e);
        }
    }

    /** {@inheritDoc} */
    @Override public void close() {
        log.info("Stopping Web Console Agent...");

        try {
            client.stop();
        }
        catch (Throwable e) {
            log.error("Failed to close websocket", e);
        }

        try {
            clusterHnd.stop();
        }
        catch (Throwable e) {
            log.error("Failed to stop cluster handler", e);
        }
    }

    /**
     * Reconnect to backend.
     * @throws Exception If failed to connect to server.
     */
    private void reconnect() throws Exception {
        if (!isRunning())
            return;

        if (client != null) {
            client.destroy();

            wss.close(StatusCode.NORMAL, null);
        }

        client = new WebSocketClient(httpClient);

        try {
            client.start();
            client.connect(this, new URI(cfg.serverUri()).resolve(AGENTS_PATH)).get(10L, TimeUnit.SECONDS);

            reconnectCnt = 0;

            clusterHnd.start();
        }
        catch (ConnectException | TimeoutException ignored) {
            // No-op.
        }
    }

    /**
     * @throws InterruptedException If await failed.
     */
    public void awaitClose() throws InterruptedException {
        closeLatch.await();
    }

    /**
     * @return {@code true} If web agent is running.
     */
    private boolean isRunning() {
        return closeLatch.getCount() > 0;
    }

    /**
     * @param ses Session.
     */
    @OnWebSocketConnect
    public void onConnect(Session ses) {
        log.info("Connected to server: " + ses.getRemoteAddress());

        wss.open(ses);

        try {
            String ver = "";
            String buildTime = "";

            String clsName = WebSocketRouter.class.getSimpleName() + ".class";
            String clsPath = WebSocketRouter.class.getResource(clsName).toString();

            if (clsPath.startsWith("jar")) {
                String manifestPath = clsPath.substring(0, clsPath.lastIndexOf('!') + 1) + "/META-INF/MANIFEST.MF";

                Manifest manifest = new Manifest(new URL(manifestPath).openStream());

                Attributes attr = manifest.getMainAttributes();

                ver = attr.getValue("Implementation-Version");
                buildTime = attr.getValue("Build-Time");
            }

            AgentHandshakeRequest req = new AgentHandshakeRequest(
                cfg.disableDemo(),
                ver,
                buildTime,
                cfg.tokens()
            );

            wss.send(AGENT_HANDSHAKE, req);
        }
        catch (Throwable e) {
            log.error("Failed to send handshake to server", e);
        }
    }

    /**
     * @param json Response from server in JSON format.
     */
    private void handshake(String json) {
        try {
            AgentHandshakeResponse res = fromJson(json, AgentHandshakeResponse.class);

            if (F.isEmpty(res.getError())) {
                Set<String> validTokens = res.getTokens();
                List<String> missedTokens = cfg.tokens();

                cfg.tokens(new ArrayList<>(validTokens));

                missedTokens.removeAll(validTokens);

                if (!F.isEmpty(missedTokens)) {
                    log.warning("Failed to validate token(s): " + secured(missedTokens) + "." +
                        " Please reload agent archive or check settings.");
                }

                log.info("Successful handshake with server.");
            }
            else {
                log.error(res.getError());

                closeLatch.countDown();
            }
        }
        catch (Throwable e) {
            log.error("Failed to process handshake response from server", e);

            closeLatch.countDown();
        }
    }

    /**
     * @param tok Token to revoke.
     */
    private void revokeToken(String tok) {
        log.warning("Security token has been revoked: " + tok);

        cfg.tokens().remove(tok);

        if (F.isEmpty(cfg.tokens())) {
            log.warning("Web Console Agent will be stopped because no more valid tokens available");

            wss.close(StatusCode.SHUTDOWN, "No more valid tokens available");

            closeLatch.countDown();
        }
    }

    /**
     * @param msg Message.
     */
    @OnWebSocketMessage
    public void onMessage(String msg) {
        try {
            WebSocketEvent evt = fromJson(msg, WebSocketEvent.class);

            String evtType = evt.getEventType();

            switch (evtType) {
                case AGENT_HANDSHAKE:
                    handshake(evt.getPayload());

                    break;

                case AGENT_REVOKE_TOKEN:
                    revokeToken(evt.getPayload());

                    break;

                case SCHEMA_IMPORT_DRIVERS:
                    dbHnd.collectJdbcDrivers(evt);

                    break;

                case SCHEMA_IMPORT_SCHEMAS:
                    dbHnd.collectDbSchemas(evt);

                    break;

                case SCHEMA_IMPORT_METADATA:
                    dbHnd.collectDbMetadata(evt);

                    break;

                case NODE_REST:
                case NODE_VISOR:
                    clusterHnd.restRequest(evt);

                    break;

                default:
                    log.warning("Unknown event: " + evt);
            }
        }
        catch (Throwable e) {
            log.error("Failed to process message: " + msg, e);
        }
    }

    /**
     * @param ses Session.
     * @param frame Frame.
     */
    @OnWebSocketFrame
    public void onFrame(Session ses, Frame frame) {
        if (isRunning() && frame.getType() == Frame.Type.PING) {
            if (log.isTraceEnabled())
                log.trace("Received ping message [socket=" + ses + ", msg=" + frame + "]");

            try {
                ses.getRemote().sendPong(PONG_MSG);
            }
            catch (Throwable e) {
                log.error("Failed to send pong to: " + ses, e);
            }
        }
    }

    /**
     * @param e Error.
     */
    @OnWebSocketError
    public void onError(Throwable e) {
        // Reconnect only in case of ConnectException.
        if (e instanceof ConnectException) {
            LT.error(log, e.getCause(), "Failed to connect to the server");

            if (reconnectCnt < 10)
                reconnectCnt++;

            try {
                Thread.sleep(reconnectCnt * 1000);

                reconnect();
            }
            catch (Throwable ignore) {
                // No-op.
            }
        }
    }

    /**
     *
     * @param statusCode Close status code.
     * @param reason Close reason.
     */
    @OnWebSocketClose
    public void onClose(int statusCode, String reason) {
        log.info("Connection closed [code=" + statusCode + ", reason=" + reason + "]");

        try {
            reconnect();
        }
        catch (Throwable ignore) {
            // No-op.
        }
    }
}
