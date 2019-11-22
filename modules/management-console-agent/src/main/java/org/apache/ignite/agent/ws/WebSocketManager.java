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

package org.apache.ignite.agent.ws;

import java.net.ProxySelector;
import java.net.URI;
import java.util.Optional;
import java.util.UUID;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.management.ManagementConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.glassfish.tyrus.client.ClientManager;
import org.glassfish.tyrus.client.ClientProperties;
import org.glassfish.tyrus.client.SslContextConfigurator;
import org.glassfish.tyrus.client.SslEngineConfigurator;
import org.glassfish.tyrus.client.auth.Credentials;
import org.springframework.messaging.converter.CompositeMessageConverter;
import org.springframework.messaging.converter.MappingJackson2MessageConverter;
import org.springframework.messaging.converter.StringMessageConverter;
import org.springframework.messaging.simp.stomp.StompHeaders;
import org.springframework.messaging.simp.stomp.StompSession;
import org.springframework.messaging.simp.stomp.StompSessionHandler;
import org.springframework.util.MimeTypeUtils;
import org.springframework.web.socket.WebSocketHttpHeaders;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.messaging.WebSocketStompClient;

import static java.net.Proxy.NO_PROXY;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.agent.utils.AgentObjectMapperFactory.binaryMapper;
import static org.glassfish.tyrus.client.ClientManager.createClient;
import static org.glassfish.tyrus.client.ClientProperties.PROXY_URI;
import static org.glassfish.tyrus.client.ClientProperties.SSL_ENGINE_CONFIGURATOR;
import static org.glassfish.tyrus.client.ThreadPoolConfig.defaultConfig;
import static org.glassfish.tyrus.container.grizzly.client.GrizzlyClientProperties.SELECTOR_THREAD_POOL_CONFIG;

/**
 * Web socket manager.
 */
public class WebSocketManager extends GridProcessorAdapter {
    /** Empty string array. */
    private static final String[] EMPTY = new String[0];

    /** Mapper. */
    private final ObjectMapper mapper = binaryMapper();

    /** Ws max buffer size. */
    private static final int WS_MAX_BUFFER_SIZE =  10 * 1024 * 1024;

    /** Agent version header. */
    private static final String AGENT_VERSION_HDR = "Agent-Version";

    /** Cluster id header. */
    private static final String CLUSTER_ID_HDR = "Cluster-Id";

    /** Current version. */
    private static final String CURR_VER = "1.0.0";

    /** Max sleep time seconds between reconnects. */
    private static final int MAX_SLEEP_TIME_SECONDS = 10;

    /** Client. */
    private WebSocketStompClient client;

    /** Session. */
    private StompSession ses;

    /** Reconnect count. */
    private int reconnectCnt;

    /**
     * @param ctx Kernal context.
     */
    public WebSocketManager(GridKernalContext ctx) {
        super(ctx);
    }

    /**
     * @param cfg Url.
     * @param sesHnd Session handler.
     */
    public void connect(URI uri, ManagementConfiguration cfg, StompSessionHandler sesHnd) throws Exception {
        if (reconnectCnt == -1)
            log.info("Connecting to server: " + uri);

        if (reconnectCnt < MAX_SLEEP_TIME_SECONDS)
            reconnectCnt++;

        Thread.sleep(reconnectCnt * 1000);

        client = new WebSocketStompClient(new StandardWebSocketClient(createWebSocketClient(uri, cfg)));

        client.setMessageConverter(getMessageConverter());

        client.start();

        ses = client.connect(uri, handshakeHeaders(), connectHeaders(), sesHnd).get(10L, SECONDS);

        reconnectCnt = -1;
    }

    /**
     * TODO GG-24630: Remove synchronized and make the send method non-blocking.
     *
     * @param dest Destination.
     * @param payload Payload.
     */
    public synchronized boolean send(String dest, byte[] payload) {
        boolean connected = ses != null && ses.isConnected();

        // TODO: workaround of spring-messaging bug with send byte array data.
        // https://github.com/spring-projects/spring-framework/issues/23358
        StompHeaders headers = new StompHeaders();

        headers.setContentType(MimeTypeUtils.APPLICATION_OCTET_STREAM);
        headers.setDestination(dest);

        if (connected)
            ses.send(headers, payload);

        return connected;
    }

    /**
     * TODO GG-24630: Remove synchronized and make the send method non-blocking.
     *
     * @param dest Destination.
     * @param payload Payload.
     */
    public synchronized boolean send(String dest, Object payload) {
        boolean connected = connected();

        if (connected)
            ses.send(dest, payload);

        return connected;
    }

    /**
     * @return {@code True} if agent connected to backend.
     */
    public boolean connected() {
        return ses != null && ses.isConnected();
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) {
        if (client != null)
            client.stop();
    }

    /**
     * @return Composite message converter.
     */
    private CompositeMessageConverter getMessageConverter() {
        MappingJackson2MessageConverter mapper =
            new MappingJackson2MessageConverter(MimeTypeUtils.APPLICATION_OCTET_STREAM);

        mapper.setObjectMapper(this.mapper);

        return new CompositeMessageConverter(
            U.sealList(new StringMessageConverter(), mapper)
        );
    }

    /**
     * @return Handshake headers.
     */
    private WebSocketHttpHeaders handshakeHeaders() {
        UUID clusterId = ctx.cluster().get().id();

        WebSocketHttpHeaders handshakeHeaders = new WebSocketHttpHeaders();

        handshakeHeaders.add(AGENT_VERSION_HDR, CURR_VER);
        handshakeHeaders.add(CLUSTER_ID_HDR, clusterId.toString());

        return handshakeHeaders;
    }

    /**
     * @return Connection headers.
     */
    private StompHeaders connectHeaders() {
        UUID clusterId = ctx.cluster().get().id();

        StompHeaders connectHeaders = new StompHeaders();

        connectHeaders.add(CLUSTER_ID_HDR, clusterId.toString());

        return connectHeaders;
    }

    /**
     * @param uri Uri.
     * @param cfg Config.
     * @return Tyrus client.
     */
    private ClientManager createWebSocketClient(URI uri, ManagementConfiguration cfg) {
        ClientManager client = createClient();

        client.getProperties().put(SELECTOR_THREAD_POOL_CONFIG, defaultConfig().setPoolName("mgmt-console-ws-client"));

        if ("wss".equals(uri.getScheme()))
            client.getProperties().put(SSL_ENGINE_CONFIGURATOR, createSslEngineConfigurator(log, cfg));

        configureProxy(client, uri);

        return client;
    }

    /**
     * @param log Logger.
     * @param cfg Config.
     * @return SSL engine configurator.
     */
    private SslEngineConfigurator createSslEngineConfigurator(IgniteLogger log, ManagementConfiguration cfg) {
        boolean trustAll = Boolean.getBoolean("trust.all");

        if (trustAll && !F.isEmpty(cfg.getConsoleTrustStore())) {
            log.warning("Management configuration contains 'server-trust-store' property and node has system" +
                    " property '-Dtrust.all=true'. Option '-Dtrust.all=true' will be ignored.");

            trustAll = false;
        }

        boolean isNeedClientAuth = !F.isEmpty(cfg.getConsoleTrustStore()) || !F.isEmpty(cfg.getConsoleKeyStore());

        SslContextConfigurator sslCtxConfigurator = new SslContextConfigurator();

        if (!F.isEmpty(cfg.getConsoleTrustStore()))
            sslCtxConfigurator.setTrustStoreFile(cfg.getConsoleTrustStore());

        if (!F.isEmpty(cfg.getConsoleTrustStorePassword()))
            sslCtxConfigurator.setTrustStorePassword(cfg.getConsoleTrustStorePassword());

        if (!F.isEmpty(cfg.getConsoleKeyStore()))
            sslCtxConfigurator.setKeyStoreFile(cfg.getConsoleKeyStore());

        if (!F.isEmpty(cfg.getConsoleKeyStorePassword()))
            sslCtxConfigurator.setKeyStorePassword(cfg.getConsoleKeyStorePassword());

        SslEngineConfigurator sslEngineConfigurator = new SslEngineConfigurator(sslCtxConfigurator, true, isNeedClientAuth, false);

        if (!F.isEmpty(cfg.getCipherSuites()))
            sslEngineConfigurator.setEnabledCipherSuites(cfg.getCipherSuites().toArray(EMPTY));

        if (trustAll)
            sslEngineConfigurator.setHostnameVerifier((hostname, session) -> true);

        return sslEngineConfigurator;
    }

    /**
     * @param mgr Manager.
     * @param srvUri Server uri.
     */
    private void configureProxy(ClientManager mgr, URI srvUri) {
        Optional<String> proxyAddress = ProxySelector.getDefault().select(srvUri).stream()
            .filter(p -> !p.equals(NO_PROXY))
            .map(p -> p.address().toString()).findFirst();

        if (proxyAddress.isPresent()) {
            mgr.getProperties().put(PROXY_URI, proxyAddress.get());

            addAuthentication(mgr, proxyAddress.get());
        }
    }

    /**
     * @param mgr Manager.
     * @param proxyAddr Proxy address.
     */
    private void addAuthentication(ClientManager mgr, String proxyAddr) {
        String user, pwd;

        if (proxyAddr.startsWith("http")) {
            user = System.getProperty("http.proxyUsername");

            pwd = System.getProperty("http.proxyPassword");
        }
        else if (proxyAddr.startsWith("https")) {
            user = System.getProperty("https.proxyUsername");

            pwd = System.getProperty("https.proxyPassword");
        }
        else {
            user = System.getProperty("java.net.socks.username");

            pwd = System.getProperty("java.net.socks.password");
        }

        mgr.getProperties().put(ClientProperties.CREDENTIALS, new Credentials(user, pwd));
    }
}
