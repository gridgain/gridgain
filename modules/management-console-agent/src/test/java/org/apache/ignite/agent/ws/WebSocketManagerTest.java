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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.agent.AgentCommonAbstractTest;
import org.apache.ignite.agent.ManagementConsoleAgent;
import org.apache.ignite.agent.config.WebSocketConfig;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.management.ManagementConfiguration;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import static com.google.common.collect.Lists.newArrayList;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.agent.StompDestinationsUtils.buildActionRequestTopic;
import static org.awaitility.Awaitility.with;

/**
 * Websocket manager test.
 */
public class WebSocketManagerTest extends AgentCommonAbstractTest {
    /**
     * Should send messages in two threads.
     */
    @Test
    public void shouldSendParallelMessages() throws Exception {
        IgniteEx ignite = startGrid(0);
        changeManagementConsoleConfig(ignite);

        WebSocketManager mgr = ((ManagementConsoleAgent)ignite.context().managementConsole()).webSocketManager();
        ExecutorService srv = Executors.newFixedThreadPool(2);

        List<CompletableFuture> list = new ArrayList<>();

        for(int i = 0; i < 500; i++) {
            list.add(runAsync(() -> mgr.send("/topic/first", 1), srv));
            list.add(runAsync(() -> mgr.send("/topic/second", 2), srv));
        }

        CompletableFuture.allOf(list.toArray(new CompletableFuture[0])).get(10, TimeUnit.SECONDS);

        assertWithPoll(() -> interceptor.getAllPayloads("/topic/first", String.class).size() == 500);
        assertWithPoll(() -> interceptor.getAllPayloads("/topic/second", String.class).size() == 500);
    }

    /**
     * Should reconnect.
     */
    @Test
    public void shouldReconnect() throws Exception {
        IgniteEx ignite = startGrid(0);
        changeManagementConsoleConfig(ignite);

        websocketDecoratedFactory.disconnectAllClients();

        assertWithPoll(() -> websocketDecoratedFactory.getConnectedSessionsCount() == 1);
    }

    /**
     * Should connect to third url from url list.
     */
    @Test
    public void shouldConnectToThirdUrlFromList() throws Exception {
        IgniteEx ignite = startGrid(0);

        ManagementConfiguration cfg = ignite.context().managementConsole().configuration();

        cfg.setConsoleUris(newArrayList("http://localhost:3000", "http://localhost:3010", "http://localhost:" + port));

        ignite.context().managementConsole().configuration(cfg);

        assertWithPoll(
            () -> interceptor.isSubscribedOn(buildActionRequestTopic(ignite.context().cluster().get().id()))
        );
    }

    /**
     * Websocket manager test with SSL backend.
     */
    @ActiveProfiles("ssl")
    public static class WebSocketManagerSSLTest extends AgentCommonAbstractTest {
        /**
         * Should connect to secured backend.
         */
        @Test
        @WithSystemProperty(key = "test.withTrustStore", value = "true")
        public void shouldConnectToSecuredBackend() throws Exception {
            IgniteEx ignite = startGrid(0);

            changeManagementConsoleConfig(ignite);
        }

        /**
         * Should not connect to secured backend without trustore.
         */
        @Test
        public void shouldNotConnectToSecuredBackendWithoutTrustStore() throws Exception {
            IgniteEx ignite = startGrid(0);

            changeManagementConsoleConfig(ignite, false);

            with().await()
                .pollInterval(2, SECONDS)
                .atMost(4, SECONDS)
                .until(() -> !interceptor.isSubscribedOn(buildActionRequestTopic(ignite.context().cluster().get().id())));
        }

        /**
         * Should connect to secured backend without trustore but with enabled trust all property.
         */
        @Test
        @WithSystemProperty(key = "trust.all", value = "true")
        public void shouldConnectToSecuredBackendWithoutTrustAll() throws Exception {
            IgniteEx ignite = startGrid(0);

            changeManagementConsoleConfig(ignite);
        }
    }

    /**
     * Websocket manager test with two way ssl.
     */
    @ActiveProfiles("ssl")
    @SpringBootTest(classes = {WebSocketConfig.class}, properties = {"server.ssl.client-auth=need"}, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
    public static class WebSocketManagerTwoWaySSLTest extends AgentCommonAbstractTest {
        /**
         * Should connect to secured backend with client authentication.
         */
        @Test
        @WithSystemProperty(key = "test.withTrustStore", value = "true")
        @WithSystemProperty(key = "test.withKeyStore", value = "true")
        public void shouldConnectToSecuredBackendWithClientCertificate() throws Exception {
            IgniteEx ignite = startGrid(0);

            changeManagementConsoleConfig(ignite);
        }
    }
}
