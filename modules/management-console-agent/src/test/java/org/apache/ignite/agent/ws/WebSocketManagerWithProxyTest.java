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

import org.apache.ignite.agent.AgentWithProxyAbstractTest;
import org.apache.ignite.agent.config.WebSocketConfig;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.testcontainers.containers.GenericContainer;

import static java.lang.String.valueOf;

/**
 * Websocket manager tests with proxy.
 */
public class WebSocketManagerWithProxyTest extends AgentWithProxyAbstractTest {
    /**
     * Should connect with HTTP proxy.
     */
    @Test
    @WithSystemProperty(key = "test.withProxy", value = "true")
    public void shouldConnectWithProxy() throws Exception {
        try (GenericContainer proxy = startProxy()) {
            System.setProperty("http.proxyHost", proxy.getContainerIpAddress());
            System.setProperty("http.proxyPort", valueOf(proxy.getFirstMappedPort()));

            IgniteEx ignite = startGrid(0);

            changeManagementConsoleConfig(ignite);
        }
    }

    /**
     * Should connect with HTTPS proxy.
     */
    @Test
    @WithSystemProperty(key = "test.withProxy", value = "true")
    public void shouldConnectWithHttpsProxy() throws Exception {
        try (GenericContainer httpsProxy = startHttpsProxy()) {
            System.setProperty("http.proxyHost", httpsProxy.getContainerIpAddress());
            System.setProperty("http.proxyPort", valueOf(httpsProxy.getFirstMappedPort()));

            IgniteEx ignite = startGrid(0);

            changeManagementConsoleConfig(ignite);
        }
    }

    /**
     * Should connect with HTTP proxy with authorization.
     */
    @Test
    @WithSystemProperty(key = "test.withProxy", value = "true")
    public void shouldConnectWithAuthProxy() throws Exception {
        try (GenericContainer authProxy = startProxyWithCreds()) {
            System.setProperty("http.proxyHost", authProxy.getContainerIpAddress());
            System.setProperty("http.proxyPort", valueOf(authProxy.getFirstMappedPort()));

            System.setProperty("http.proxyUsername", "user");
            System.setProperty("http.proxyPassword", "123456");

            IgniteEx ignite = startGrid(0);

            changeManagementConsoleConfig(ignite);
        }
    }

    /**
     * Should connect with HTTP proxy.
     */
    @Test
    @WithSystemProperty(key = "test.withProxy", value = "true")
    public void shouldConnectWithProxyFromHttpsProperty() throws Exception {
        try (GenericContainer proxy = startProxy()) {
            System.setProperty("https.proxyHost", proxy.getContainerIpAddress());
            System.setProperty("https.proxyPort", valueOf(proxy.getFirstMappedPort()));

            IgniteEx ignite = startGrid(0);

            changeManagementConsoleConfig(ignite);
        }
    }

    /**
     * Should connect with HTTPS proxy.
     */
    @Test
    @WithSystemProperty(key = "test.withProxy", value = "true")
    public void shouldConnectWithHttpsProxyFromHttpsProperty() throws Exception {
        try (GenericContainer httpsProxy = startHttpsProxy()) {
            System.setProperty("https.proxyHost", httpsProxy.getContainerIpAddress());
            System.setProperty("https.proxyPort", valueOf(httpsProxy.getFirstMappedPort()));

            IgniteEx ignite = startGrid(0);

            changeManagementConsoleConfig(ignite);
        }
    }

    /**
     * Should connect with HTTP proxy with authorization.
     */
    @Test
    @WithSystemProperty(key = "test.withProxy", value = "true")
    public void shouldConnectWithAuthProxyFromHttpsProperty() throws Exception {
        try (GenericContainer authProxy = startProxyWithCreds()) {
            System.setProperty("https.proxyHost", authProxy.getContainerIpAddress());
            System.setProperty("https.proxyPort", valueOf(authProxy.getFirstMappedPort()));

            System.setProperty("https.proxyUsername", "user");
            System.setProperty("https.proxyPassword", "123456");

            IgniteEx ignite = startGrid(0);

            changeManagementConsoleConfig(ignite);
        }
    }

    /**
     * Weboscket manager test with proxy and secured backend.
     */
    @ActiveProfiles("ssl")
    public static class WebSocketManagerWithSslAndProxyTest extends AgentWithProxyAbstractTest {
        /**
         * Should connect to secured backend with HTTP proxy.
         */
        @Test
        @WithSystemProperty(key = "test.withProxy", value = "true")
        @WithSystemProperty(key = "test.withTrustStore", value = "true")
        public void shouldConnectWithProxy() throws Exception {
            try (GenericContainer proxy = startProxy()) {
                System.setProperty("http.proxyHost", proxy.getContainerIpAddress());
                System.setProperty("http.proxyPort", valueOf(proxy.getFirstMappedPort()));

                IgniteEx ignite = startGrid(0);

                changeManagementConsoleConfig(ignite);
            }
        }

        /**
         * Should connect to secured backend with HTTPS proxy.
         */
        @Test
        @WithSystemProperty(key = "test.withProxy", value = "true")
        @WithSystemProperty(key = "test.withTrustStore", value = "true")
        public void shouldConnectWithHttpsProxy() throws Exception {
            try (GenericContainer httpsProxy = startHttpsProxy()) {
                System.setProperty("http.proxyHost", httpsProxy.getContainerIpAddress());
                System.setProperty("http.proxyPort", valueOf(httpsProxy.getFirstMappedPort()));

                IgniteEx ignite = startGrid(0);

                changeManagementConsoleConfig(ignite);
            }
        }

        /**
         * Should connect to secured backend with HTTP proxy and authorization.
         */
        @Test
        @WithSystemProperty(key = "test.withProxy", value = "true")
        @WithSystemProperty(key = "test.withTrustStore", value = "true")
        public void shouldConnectWithAuthProxy() throws Exception {
            try (GenericContainer authProxy = startProxyWithCreds()) {
                System.setProperty("http.proxyHost", authProxy.getContainerIpAddress());
                System.setProperty("http.proxyPort", valueOf(authProxy.getFirstMappedPort()));

                System.setProperty("http.proxyUsername", "user");
                System.setProperty("http.proxyPassword", "123456");

                IgniteEx ignite = startGrid(0);

                changeManagementConsoleConfig(ignite);
            }
        }
    }

    /**
     * Weboscket manager test with proxy and secured backend with client authentication.
     */
    @ActiveProfiles("ssl")
    @SpringBootTest(classes = {WebSocketConfig.class}, properties = {"server.ssl.client-auth=need"}, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
    public static class WebSocketManagerWithTwoWaySslAndProxyTest extends AgentWithProxyAbstractTest {
        /**
         * Should connect to secured backend with client authorization with HTTP.
         */
        @Test
        @WithSystemProperty(key = "test.withProxy", value = "true")
        @WithSystemProperty(key = "test.withTrustStore", value = "true")
        @WithSystemProperty(key = "test.withKeyStore", value = "true")
        public void shouldConnectWithProxy() throws Exception {
            try (GenericContainer proxy = startProxy()) {
                System.setProperty("http.proxyHost", proxy.getContainerIpAddress());
                System.setProperty("http.proxyPort", valueOf(proxy.getFirstMappedPort()));

                IgniteEx ignite = startGrid(0);

                changeManagementConsoleConfig(ignite);
            }
        }

        /**
         * Should connect to secured backend with client authorization with HTTPS.
         */
        @Test
        @WithSystemProperty(key = "test.withProxy", value = "true")
        @WithSystemProperty(key = "test.withTrustStore", value = "true")
        @WithSystemProperty(key = "test.withKeyStore", value = "true")
        public void shouldConnectWithHttpsProxy() throws Exception {
            try (GenericContainer httpsProxy = startHttpsProxy()) {
                System.setProperty("http.proxyHost", httpsProxy.getContainerIpAddress());
                System.setProperty("http.proxyPort", valueOf(httpsProxy.getFirstMappedPort()));

                IgniteEx ignite = startGrid(0);

                changeManagementConsoleConfig(ignite);
            }
        }

        /**
         * Should connect to secured backend with client authorization with HTTP and authorization.
         */
        @Test
        @WithSystemProperty(key = "test.withProxy", value = "true")
        @WithSystemProperty(key = "test.withTrustStore", value = "true")
        @WithSystemProperty(key = "test.withKeyStore", value = "true")
        public void shouldConnectWithAuthProxy() throws Exception {
            try (GenericContainer authProxy = startProxyWithCreds()) {
                System.setProperty("http.proxyHost", authProxy.getContainerIpAddress());
                System.setProperty("http.proxyPort", valueOf(authProxy.getFirstMappedPort()));

                System.setProperty("http.proxyUsername", "user");
                System.setProperty("http.proxyPassword", "123456");

                IgniteEx ignite = startGrid(0);

                changeManagementConsoleConfig(ignite);
            }
        }
    }
}
