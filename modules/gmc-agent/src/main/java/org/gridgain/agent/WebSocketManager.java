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

package org.gridgain.agent;

import java.io.ByteArrayInputStream;
import java.net.InetSocketAddress;
import java.net.ProxySelector;
import java.net.URI;
import java.security.KeyStore;
import java.util.List;
import java.util.UUID;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.gmc.ManagementConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.HttpProxy;
import org.eclipse.jetty.client.Origin;
import org.eclipse.jetty.client.ProxyConfiguration;
import org.eclipse.jetty.client.Socks4Proxy;
import org.eclipse.jetty.client.util.BasicAuthentication;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.springframework.messaging.converter.CompositeMessageConverter;
import org.springframework.messaging.converter.MappingJackson2MessageConverter;
import org.springframework.messaging.converter.StringMessageConverter;
import org.springframework.messaging.simp.stomp.StompHeaders;
import org.springframework.messaging.simp.stomp.StompSession;
import org.springframework.messaging.simp.stomp.StompSessionHandler;
import org.springframework.util.MimeTypeUtils;
import org.springframework.web.socket.WebSocketHttpHeaders;
import org.springframework.web.socket.client.jetty.JettyWebSocketClient;
import org.springframework.web.socket.messaging.WebSocketStompClient;

import static java.net.Proxy.NO_PROXY;
import static java.net.Proxy.Type.SOCKS;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static org.eclipse.jetty.client.api.Authentication.ANY_REALM;
import static org.gridgain.agent.AgentUtils.EMPTY;

/**
 * Web socket manager.
 */
public class WebSocketManager implements AutoCloseable {
    /** Mapper. */
    private final ObjectMapper mapper = new ObjectMapper(new SmileFactory())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    /** Ws max buffer size. */
    private static final int WS_MAX_BUFFER_SIZE =  10 * 1024 * 1024;

    /** Agent version header. */
    private static final String AGENT_VERSION_HDR = "agent-version";

    /** Cluster id header. */
    private static final String CLUSTER_ID_HDR = "cluster-id";

    /** Current version. */
    private static final String CURR_VER = "9.0.0";

    /** Max sleep time seconds. */
    private static final int MAX_SLEEP_TIME_SECONDS = 10;

    /** Context. */
    private GridKernalContext ctx;

    /** Logger. */
    private IgniteLogger log;

    /** Client. */
    private WebSocketStompClient client;

    /** Session. */
    private StompSession ses;

    /** Reconnect count. */
    private int reconnectCnt;

    /** Is stopped. */
    private volatile boolean isStopped;

    /**
     * @param ctx Context.
     */
    public WebSocketManager(GridKernalContext ctx) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> isStopped = true));

        this.ctx = ctx;
        this.log = ctx.log(WebSocketManager.class);
    }

    /**
     * @param cfg Url.
     * @param sesHnd Session handler.
     */
    public void connect(URI uri, ManagementConfiguration cfg, StompSessionHandler sesHnd) throws Exception {
        if (isStopped)
            throw new IgniteCheckedException("Web socket manager was stopped.");

        if (client != null)
            client.stop();

        if (reconnectCnt == -1)
            log.info("Connecting to server: " + uri);

        if (reconnectCnt < MAX_SLEEP_TIME_SECONDS)
            reconnectCnt++;

        Thread.sleep(reconnectCnt * 1000);

        WebSocketClient webSockClient = new WebSocketClient(createHttpClient(uri, cfg));
        webSockClient.setMaxTextMessageBufferSize(WS_MAX_BUFFER_SIZE);
        webSockClient.setMaxBinaryMessageBufferSize(WS_MAX_BUFFER_SIZE);

        client = new WebSocketStompClient(new JettyWebSocketClient(webSockClient));
        client.setMessageConverter(getMessageConverter());
        client.start();

        ses = client.connect(uri, getHandshakeHeaders(), getConnectHeaders(), sesHnd).get(10L, SECONDS);
        reconnectCnt = -1;
    }

    /**
     * @param dest Destination.
     * @param payload Payload.
     */
    public boolean send(String dest, byte[] payload) {
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
     * @param dest Destination.
     * @param payload Payload.
     */
    public boolean send(String dest, Object payload) {
        boolean connected = ses != null && ses.isConnected();

        if (connected)
            ses.send(dest, payload);

        return connected;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        isStopped = true;
        client.stop();
    }

    /**
     * @return Composite message converter.
     */
    private CompositeMessageConverter getMessageConverter() {
        MappingJackson2MessageConverter mapper = new MappingJackson2MessageConverter(MimeTypeUtils.APPLICATION_OCTET_STREAM);
        mapper.setObjectMapper(this.mapper);

        return new CompositeMessageConverter(
            U.sealList(new StringMessageConverter(), mapper)
        );
    }

    /**
     * @return Handshake headers.
     */
    private WebSocketHttpHeaders getHandshakeHeaders() {
        UUID clusterId = ctx.cluster().get().id();

        WebSocketHttpHeaders handshakeHeaders = new WebSocketHttpHeaders();
        handshakeHeaders.add(AGENT_VERSION_HDR, CURR_VER);
        handshakeHeaders.add(CLUSTER_ID_HDR, clusterId.toString());

        return handshakeHeaders;
    }

    /**
     * @return Connection headers.
     */
    private StompHeaders getConnectHeaders() {
        UUID clusterId = ctx.cluster().get().id();

        StompHeaders connectHeaders = new StompHeaders();
        connectHeaders.add(CLUSTER_ID_HDR, clusterId.toString());

        return connectHeaders;
    }

    /**
     * @return Jetty http client.
     */
    private HttpClient createHttpClient(URI uri, ManagementConfiguration cfg) throws IgniteCheckedException {
        HttpClient httpClient = new HttpClient(createServerSslFactory(log, cfg));
        // TODO GG-18379 Investigate how to establish native websocket connection with proxy.
        configureProxy(log, httpClient, uri);

        try {
            httpClient.start();
        }
        catch (Exception e) {
            throw new IgniteCheckedException(e);
        }

        return httpClient;
    }

    /**
     * @param log Logger.
     * @param cfg Config.
     */
    @SuppressWarnings("deprecation")
    private SslContextFactory createServerSslFactory(IgniteLogger log, ManagementConfiguration cfg) {
        boolean trustAll = Boolean.getBoolean("trust.all");

        if (trustAll && !F.isEmpty(cfg.getServerTrustStore())) {
            log.warning("Options contains both '--server-trust-store' and '-Dtrust.all=true'. " +
                    "Option '-Dtrust.all=true' will be ignored on connect to Web server.");

            trustAll = false;
        }

        boolean ssl = trustAll || !F.isEmpty(cfg.getServerTrustStore()) || !F.isEmpty(cfg.getServerKeyStore());

        if (!ssl)
            return new SslContextFactory();

        return sslContextFactory(
                cfg.getServerKeyStore(),
                cfg.getServerKeyStorePassword(),
                trustAll,
                cfg.getServerTrustStore(),
                cfg.getServerTrustStorePassword(),
                cfg.getCipherSuites()
        );
    }

    /**
     * @param content Key store content.
     * @param pwd Password.
     */
    private KeyStore keyStore(String content, String pwd) throws Exception {
        KeyStore keyStore = KeyStore.getInstance("JKS");

        keyStore.load(new ByteArrayInputStream(content.getBytes(UTF_8)), pwd != null ? pwd.toCharArray() : null);

        return keyStore;
    }

    /**
     *
     * @param keyStore Path to key store.
     * @param keyStorePwd Optional key store password.
     * @param trustAll Whether we should trust for self-signed certificate.
     * @param trustStore Path to trust store.
     * @param trustStorePwd Optional trust store password.
     * @param ciphers Optional list of enabled cipher suites.
     * @return SSL context factory.
     */
    @SuppressWarnings("deprecation")
    private SslContextFactory sslContextFactory(
            String keyStore,
            String keyStorePwd,
            boolean trustAll,
            String trustStore,
            String trustStorePwd,
            List<String> ciphers
    ) {
        SslContextFactory sslCtxFactory = new SslContextFactory();

        if (!F.isEmpty(keyStore)) {
            try {
                sslCtxFactory.setKeyStore(keyStore(keyStore, keyStorePwd));
            }
            catch (Exception e) {
                log.warning("Failed to load server keyStore", e);
            }
        }

        if (trustAll) {
            sslCtxFactory.setTrustAll(true);
            // Available in Jetty >= 9.4.15.x sslCtxFactory.setHostnameVerifier((hostname, session) -> true);
        }
        else if (!F.isEmpty(trustStore)) {
            try {
                sslCtxFactory.setKeyStore(keyStore(trustStore, trustStorePwd));
            }
            catch (Exception e) {
                log.warning("Failed to load server keyStore", e);
            }
        }

        if (!F.isEmpty(ciphers))
            sslCtxFactory.setIncludeCipherSuites(ciphers.toArray(EMPTY));

        return sslCtxFactory;
    }

    /**
     * @param srvUri Server uri.
     */
    private void configureProxy(IgniteLogger log, HttpClient httpClient, URI srvUri) {
        try {
            boolean secure = "https".equalsIgnoreCase(srvUri.getScheme());

            List<ProxyConfiguration.Proxy> proxies = ProxySelector.getDefault().select(srvUri).stream()
                .filter(p -> !p.equals(NO_PROXY))
                .map(p -> {
                    InetSocketAddress inetAddr = (InetSocketAddress)p.address();

                    Origin.Address addr = new Origin.Address(inetAddr.getHostName(), inetAddr.getPort());

                    if (p.type() == SOCKS)
                        return new Socks4Proxy(addr, secure);

                    return new HttpProxy(addr, secure);
                })
                .collect(toList());

            httpClient.getProxyConfiguration().getProxies().addAll(proxies);

            addAuthentication(httpClient, proxies);
        }
        catch (Exception e) {
            log.warning("Failed to configure proxy.", e);
        }
    }

    /**
     * @param httpClient Http client.
     * @param proxies Proxies.
     */
    private void addAuthentication(HttpClient httpClient, List<ProxyConfiguration.Proxy> proxies) {
        proxies.forEach(p -> {
            String user, pwd;

            if (p instanceof HttpProxy) {
                String scheme = p.getURI().getScheme();

                user = System.getProperty(scheme + ".proxyUsername");
                pwd = System.getProperty(scheme + ".proxyPassword");
            }
            else {
                user = System.getProperty("java.net.socks.username");
                pwd = System.getProperty("java.net.socks.password");
            }

            httpClient.getAuthenticationStore().addAuthentication(
                new BasicAuthentication(p.getURI(), ANY_REALM, user, pwd)
            );
        });
    }
}
