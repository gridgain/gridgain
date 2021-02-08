package org.apache.ignite.discovery;

import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.kubernetes.configuration.KubernetesConnectionConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.kubernetes.TcpDiscoveryKubernetesIpFinder;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.matchers.Times;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Collectors;

import static org.mockserver.integration.ClientAndServer.startClientAndServer;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

/** Super class for Kubernetes discovery tests. */
public abstract class KubernetesDiscoveryAbstractTest extends GridCommonAbstractTest {
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
    protected IgniteConfiguration getConfiguration(String instanceName, Boolean clientMode) throws Exception {
        IgniteConfiguration cfg = getConfiguration();

        KubernetesConnectionConfiguration kccfg = prepareConfiguration();
        TcpDiscoveryKubernetesIpFinder ipFinder = new TcpDiscoveryKubernetesIpFinder(kccfg);

        TcpDiscoverySpi discoverySpi = new TcpDiscoverySpi();
        discoverySpi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(discoverySpi);

        cfg.setGridLogger(log);

        cfg.setIgniteInstanceName(instanceName);
        cfg.setClientMode(clientMode);

        return cfg;
    }

    /**
     * Mock HTTP server responce
     *
     * @param addrs Address list to return.
     */
    protected final void mockServerResponse(String... addrs) {
        mockServerResponse(1, addrs);
    }

    /**
     * Mock HTTP server responce
     *
     * @param times How many times mock should return value.
     * @param addrs Address list to return.
     */
    protected final void mockServerResponse(int times, String... addrs) {
        String ipAddrs = Arrays.stream(addrs)
                .map(addr -> String.format("{\"ip\":\"%s\"}", addr))
                .collect(Collectors.joining(","));

        mockServer
                .when(
                        request()
                                .withMethod("GET")
                                .withPath(String.format("/api/v1/namespaces/%s/endpoints/%s", namespace, service)),
                        Times.exactly(times)
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

    /** */
    protected final KubernetesConnectionConfiguration prepareConfiguration()
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