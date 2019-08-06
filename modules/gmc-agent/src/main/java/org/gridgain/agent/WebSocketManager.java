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

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.ProxySelector;
import java.net.URI;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.HttpProxy;
import org.eclipse.jetty.client.Origin;
import org.eclipse.jetty.client.ProxyConfiguration;
import org.eclipse.jetty.client.Socks4Proxy;
import org.eclipse.jetty.client.util.BasicAuthentication;
import org.eclipse.jetty.io.EofException;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.websocket.api.UpgradeException;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.springframework.messaging.converter.CompositeMessageConverter;
import org.springframework.messaging.converter.MappingJackson2MessageConverter;
import org.springframework.messaging.converter.StringMessageConverter;
import org.springframework.messaging.simp.stomp.StompHeaders;
import org.springframework.messaging.simp.stomp.StompSession;
import org.springframework.messaging.simp.stomp.StompSessionHandler;
import org.springframework.web.socket.WebSocketHttpHeaders;
import org.springframework.web.socket.client.jetty.JettyWebSocketClient;
import org.springframework.web.socket.messaging.WebSocketStompClient;

import static java.net.Proxy.NO_PROXY;
import static java.net.Proxy.Type.SOCKS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static org.eclipse.jetty.client.api.Authentication.ANY_REALM;
import static org.gridgain.agent.AgentUtils.EMPTY;

/**
 * Web socket manager.
 */
public class WebSocketManager implements AutoCloseable {
    /** Ws max buffer size. */
    private static final int WS_MAX_BUFFER_SIZE =  10 * 1024 * 1024;

    /** Agent version header. */
    private static final String AGENT_VERSION_HDR = "AGENT_VERSION";

    /** Cluster id header. */
    private static final String CLUSTER_ID_HDR = "CLUSTER_ID";

    /** Current version. */
    private static final String CURR_VER = "9.0.0";

    /** Max sleep time seconds. */
    private static final int MAX_SLEEP_TIME_SECONDS = 10;

    /** Context. */
    private GridKernalContext ctx;

    /** Config. */
    private AgentConfiguration cfg;

    /** Logger. */
    private IgniteLogger log;

    /** Url. */
    private URI url;

    /** Session handler. */
    private StompSessionHandler sesHnd;

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
     * @param cfg Config.
     */
    public WebSocketManager(GridKernalContext ctx, AgentConfiguration cfg) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> isStopped = true));

        this.ctx = ctx;
        this.cfg = cfg;
        this.log = ctx.log(WebSocketManager.class);
    }

    /**
     * @param url Url.
     * @param sesHnd Session handler.
     */
    public URI connect(URI url, StompSessionHandler sesHnd) throws IgniteCheckedException, InterruptedException {
        if (isStopped)
            throw new IgniteCheckedException("Web socket manager was stopped.");

        this.url = url;
        this.sesHnd = sesHnd;

        WebSocketClient webSockClient = new WebSocketClient(createHttpClient());
        webSockClient.setMaxTextMessageBufferSize(WS_MAX_BUFFER_SIZE);
        webSockClient.setMaxBinaryMessageBufferSize(WS_MAX_BUFFER_SIZE);

        client = new WebSocketStompClient(new JettyWebSocketClient(webSockClient));
        client.setMessageConverter(getMessageConverter());
        client.start();

        try {
            ses = client.connect(url, getHandshakeHeaders(), getConnectHeaders(), sesHnd).get(10L, SECONDS);
            reconnectCnt = -1;

            return url;
        }
        catch (TimeoutException ex) {
            return reconnect();
        } catch (ExecutionException ex) {
            if (ifReconnectException(ex.getCause())) {
                if (reconnectCnt == 0)
                    log.error("Failed to establish websocket connection with server: " + this.url);

                return reconnect();
            }
            else {
                log.error("Failed to establish websocket connection with server: " + this.url, ex);

                throw new IgniteCheckedException(ex.getCause());
            }
        }
    }

    /**
     * Reconnect.
     */
    public URI reconnect() throws IgniteCheckedException, InterruptedException {
        if (isStopped)
            throw new IgniteCheckedException("Web socket manager was stopped.");

        client.stop();

        if (reconnectCnt == -1)
            log.info("Connecting to server: " + url);

        if (reconnectCnt < MAX_SLEEP_TIME_SECONDS)
            reconnectCnt++;

        Thread.sleep(reconnectCnt * 1000);

        return connect(url, sesHnd);
    }

    /**
     * @return Stomp session.
     */
    public StompSession getSession() {
        return ses;
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
        return new CompositeMessageConverter(
            U.sealList(new StringMessageConverter(), new MappingJackson2MessageConverter())
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
     * @param ex Ex.
     */
    private boolean ifReconnectException(Throwable ex) {
        return ex instanceof ConnectException || ex instanceof UpgradeException || ex instanceof EofException;
    }

    /**
     * @return Jetty http client.
     */
    private HttpClient createHttpClient() throws IgniteCheckedException {
        HttpClient httpClient = new HttpClient(createServerSslFactory(log, cfg));
        // TODO GG-18379 Investigate how to establish native websocket connection with proxy.
        configureProxy(log, httpClient, url);

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
    private SslContextFactory createServerSslFactory(IgniteLogger log, AgentConfiguration cfg) {
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
            sslCtxFactory.setKeyStorePath(keyStore);

            if (!F.isEmpty(keyStorePwd))
                sslCtxFactory.setKeyStorePassword(keyStorePwd);
        }

        if (trustAll) {
            sslCtxFactory.setTrustAll(true);
            // Available in Jetty >= 9.4.15.x sslCtxFactory.setHostnameVerifier((hostname, session) -> true);
        }
        else if (!F.isEmpty(trustStore)) {
            sslCtxFactory.setTrustStorePath(trustStore);

            if (!F.isEmpty(trustStorePwd))
                sslCtxFactory.setTrustStorePassword(trustStorePwd);
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
