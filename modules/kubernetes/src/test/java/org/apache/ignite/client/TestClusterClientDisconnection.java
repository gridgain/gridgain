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

package org.apache.ignite.client;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.kubernetes.configuration.KubernetesConnectionConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.kubernetes.TcpDiscoveryKubernetesIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.matchers.Times;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.events.EventType.EVT_CLIENT_NODE_DISCONNECTED;
import static org.mockserver.integration.ClientAndServer.startClientAndServer;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

/** Test that thin client can detect cluster disconnection using {@link ThinClientKubernetesAddressFinder}. */
public class TestClusterClientDisconnection extends GridCommonAbstractTest {
    /** Mock of kubernetes API. */
    private static ClientAndServer mockServer;

    /** */
    private static final String namespace = "ns01";

    /** */
    private static final String service = "ignite";

    /** */
    @BeforeClass
    public static void startServer() {
        mockServer = startClientAndServer();
    }

    /** */
    @AfterClass
    public static void stopServer() {
        mockServer.stop();
    }

    /** */
    @After
    public void tearDown() {
        stopAllGrids();
    }

    /** */
    @Test
    public void testClientConnectsToCluster() throws Exception {
        mockServerResponse();

        IgniteEx crd = startGrid(getConfiguration(getTestIgniteInstanceName(), false));
        String crdAddr = crd.localNode().addresses().iterator().next();

        mockServerResponse(crdAddr);

        IgniteEx client = startGrid(getConfiguration("client", true));

        waitForTopology(2);

        Ignition.stop(crd.name(), true);

        CountDownLatch latch = new CountDownLatch(1);

        client.events().localListen(event -> {
            latch.countDown();

            return true;
        }, EVT_CLIENT_NODE_DISCONNECTED);

        assertTrue("Failed to wait for client node disconnected.", latch.await(60, SECONDS));

    }

    /** {@inheritDoc} */
    protected IgniteConfiguration getConfiguration(String instanceName, Boolean clientMode) throws Exception {
        IgniteConfiguration cfg = getConfiguration();
        cfg.setClientMode(clientMode);

        KubernetesConnectionConfiguration kccfg = prepareConfiguration();
        TcpDiscoveryKubernetesIpFinder ipFinder = new TcpDiscoveryKubernetesIpFinder(kccfg);

        cfg.setFailureDetectionTimeout(10000);

        TcpDiscoverySpi discoverySpi = new TcpDiscoverySpi();
        discoverySpi.setReconnectCount(1);
        discoverySpi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(discoverySpi);

        cfg.setIgniteInstanceName(instanceName);

        return cfg;
    }

    /** */
    private void mockServerResponse(String... addrs) {
        String ipAddrs = Arrays.stream(addrs)
                .map(addr -> String.format("{\"ip\":\"%s\"}", addr))
                .collect(Collectors.joining(","));

        mockServer
                .when(
                        request()
                                .withMethod("GET")
                                .withPath(String.format("/api/v1/namespaces/%s/endpoints/%s", namespace, service)),
                        Times.exactly(1)
                )
                .respond(
                        response()
                                .withStatusCode(200)
                                .withBody("{" +
                                        "  \"subsets\": [" +
                                        "     {" +
                                        "        \"addresses\": [" +
                                        "        " + ipAddrs +
                                        "        ]" +
                                        "     }" +
                                        "  ]" +
                                        "}"
                                ));
    }

    private void mockFailureServerResponse() {
        mockServer
                .when(
                        request()
                                .withMethod("GET")
                                .withPath(String.format("/api/v1/namespaces/%s/endpoints/%s", namespace, service)),
                        Times.exactly(1)
                )
                .respond(
                        response()
                                .withStatusCode(401));
    }

    /** */
    private KubernetesConnectionConfiguration prepareConfiguration()
            throws IOException
    {
        File account = File.createTempFile("kubernetes-test-account", "");
        FileWriter fw = new FileWriter(account);
        fw.write("account-token");
        fw.close();

        String accountFile = account.getAbsolutePath();

        KubernetesConnectionConfiguration cfg = new KubernetesConnectionConfiguration();
        cfg.setNamespace(namespace);
        cfg.setServiceName(service);
        cfg.setMasterUrl("https://localhost:" + mockServer.getLocalPort());
        cfg.setAccountToken(accountFile);

        return cfg;
    }
}
