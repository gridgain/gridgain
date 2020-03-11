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

package org.apache.ignite.agent;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.agent.config.TestChannelInterceptor;
import org.apache.ignite.agent.config.TestWebsocketDecoratedFactory;
import org.apache.ignite.agent.config.WebSocketConfig;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.failure.NoOpFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.management.ManagementConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.tracing.opencensus.OpenCensusTracingSpi;
import org.apache.ignite.testframework.junits.IgniteTestResources;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.embedded.LocalServerPort;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.env.Environment;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import static java.lang.Boolean.getBoolean;
import static java.util.Arrays.stream;
import static java.util.Collections.emptyList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.agent.StompDestinationsUtils.buildActionRequestTopic;
import static org.awaitility.Awaitility.with;

/**
 * Base class for test with agent.
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = WebSocketConfig.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
public abstract class AgentCommonAbstractTest extends GridCommonAbstractTest {
    /** Template. */
    @Autowired
    protected SimpMessagingTemplate template;

    /** Interceptor. */
    @Autowired
    protected TestChannelInterceptor interceptor;

    /** Websocket decorated factory. */
    @Autowired
    protected TestWebsocketDecoratedFactory websocketDecoratedFactory;

    /** Port. */
    @LocalServerPort
    protected int port;

    /** Environment. */
    @Autowired
    private Environment environment;

    /** Cluster. */
    protected IgniteCluster cluster;

    /**
     * Stop all grids and clear persistence dir.
     */
    @Before
    public void cleanup() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * Stop all grids and clear persistence dir.
     */
    @After
    public void stopAndClear() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        checkThreads();
    }

    /**
     * Cheks that all management agent threads was stopped.
     */
    protected void checkThreads() {
        List<String> mgmtThreadNames = Thread.getAllStackTraces().keySet().stream()
            .filter(thread -> thread.getName().startsWith("mgmt-"))
            .map(Thread::getName)
            .collect(toList());

        assertEqualsCollections(emptyList(), mgmtThreadNames);
    }

    /**
     * @param ignite Ignite.
     */
    protected void changeManagementConsoleConfig(IgniteEx ignite) {
        changeManagementConsoleConfig(ignite, true);
    }

    /**
     * @param ignite Ignite.
     */
    protected void changeManagementConsoleConfig(IgniteEx ignite, boolean isAssertNeeded) {
        boolean isSslEnabled = stream(environment.getActiveProfiles()).anyMatch("ssl"::equalsIgnoreCase);
        boolean isProxyEnabled = getBoolean("test.withProxy");

        ManagementConfiguration cfg = ignite.context().managementConsole().configuration();

        if (isSslEnabled) {
            boolean isKeyStoreNeeded = getBoolean("test.withKeyStore");
            boolean isTrustStoreNeeded = getBoolean("test.withTrustStore");

            if (isTrustStoreNeeded) {
                cfg.setConsoleTrustStore(AgentCommonAbstractTest.class.getClassLoader().getResource("ssl/server.p12").getPath());
                cfg.setConsoleTrustStorePassword("123456");
            }

            if (isKeyStoreNeeded) {
                cfg.setConsoleKeyStore(AgentCommonAbstractTest.class.getClassLoader().getResource("ssl/client.p12").getPath());
                cfg.setConsoleKeyStorePassword("123456");
            }
        }

        if (!isProxyEnabled)
            cfg.setConsoleUris(F.asList((isSslEnabled ? "https" : "http") + "://localhost:" + port));
        else if (isWindows() || isMac())
            cfg.setConsoleUris(F.asList((isSslEnabled ? "https" : "http") + "://host.docker.internal:" + port));
        else
            cfg.setConsoleUris(F.asList((isSslEnabled ? "https" : "http") + "://host.testcontainers.internal:" + port));


        ignite.context().managementConsole().configuration(cfg);

        if (isAssertNeeded) {
            assertWithPoll(
                () -> interceptor.isSubscribedOn(buildActionRequestTopic(ignite.context().cluster().get().id()))
            );
        }
    }

    /**
     * @param cond Condition.
     */
    protected void assertWithPoll(Callable<Boolean> cond) {
        with().pollInterval(500, MILLISECONDS).await().atMost(20, SECONDS).until(cond);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName, IgniteTestResources rsrcs) {
        return new IgniteConfiguration()
            .setAuthenticationEnabled(false)
            .setLocalHost("127.0.0.1")
            .setIgniteInstanceName(igniteInstanceName)
            .setMetricsLogFrequency(0)
            .setQueryThreadPoolSize(16)
            .setFailureDetectionTimeout(10000)
            .setClientFailureDetectionTimeout(10000)
            .setNetworkTimeout(10000)
            .setConnectorConfiguration(null)
            .setClientConnectorConfiguration(null)
            .setTransactionConfiguration(
                new TransactionConfiguration()
                    .setTxTimeoutOnPartitionMapExchange(60 * 1000)
            )
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(
                        new DataRegionConfiguration()
                            .setPersistenceEnabled(true)
                    )
            )
            .setTracingSpi(new OpenCensusTracingSpi())
            .setFailureHandler(new NoOpFailureHandler())
            .setDiscoverySpi(
                new TcpDiscoverySpi()
                    .setIpFinder(
                        new TcpDiscoveryVmIpFinder()
                            .setAddresses(Collections.singletonList("127.0.0.1:47500..47509"))
                    )
            );
    }

    /**
     * @return {@code True} is jvm running on windows.
     */
    private static boolean isWindows() {
        return System.getProperty("os.name").toLowerCase().contains("win");
    }

    /**
     * @return {@code True} is jvm running on mac OS.
     */
    private static boolean isMac() {
        return System.getProperty("os.name").toLowerCase().contains("mac");
    }
}
