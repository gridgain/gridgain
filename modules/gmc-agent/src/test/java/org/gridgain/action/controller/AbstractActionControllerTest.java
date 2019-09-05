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

package org.gridgain.action.controller;

import org.apache.ignite.IgniteCluster;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.failure.NoOpFailureHandler;
import org.apache.ignite.opencensus.spi.tracing.OpenCensusTracingSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.IgniteTestResources;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.gridgain.config.TestChannelInterceptor;
import org.gridgain.dto.action.Request;
import org.gridgain.dto.action.Response;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.function.Function;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.events.EventType.EVT_CLUSTER_ACTIVATED;
import static org.apache.ignite.events.EventType.EVT_CLUSTER_DEACTIVATED;
import static org.awaitility.Awaitility.with;
import static org.gridgain.agent.StompDestinationsUtils.buildActionRequestTopic;
import static org.gridgain.agent.StompDestinationsUtils.buildActionResponseDest;

/**
 * Abstract test for action controllers.
 */
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT, properties = "server.port=3000")
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
abstract class AbstractActionControllerTest extends GridCommonAbstractTest {
    /** Template. */
    @Autowired
    private SimpMessagingTemplate template;

    /** Interceptor. */
    @Autowired
    private TestChannelInterceptor interceptor;

    /** Cluster. */
    protected IgniteCluster cluster;

    /**
     * Start grid.
     */
    @Before
    public void startup() throws Exception {
        cluster = startGrid().cluster();
    }

    /**
     * Stop all grids and clear persistence dir.
     */
    @After
    public void stopAndClear() throws Exception {
        stopAllGrids();
        cleanPersistenceDir();
    }

    /**
     * Send action request and check execution result with assert function and specific grid instances count.
     *
     * @param req Request.
     * @param assertFn Assert fn.
     */
    protected void executeAction(Request req, Function<Response, Boolean> assertFn) {
        cluster.active(true);

        assertWithPoll(
            () -> interceptor.isSubscribedOn(buildActionRequestTopic(cluster.id()))
        );

        template.convertAndSend(buildActionRequestTopic(cluster.id()), req);

        assertWithPoll(
            () -> {
                Response res = interceptor.getPayload(buildActionResponseDest(cluster.id(), req.getId()), Response.class);
                return res != null && assertFn.apply(res);
            }
        );
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName, IgniteTestResources rsrcs) {
        return new IgniteConfiguration()
            .setAuthenticationEnabled(false)
            .setIgniteInstanceName(igniteInstanceName)
            .setMetricsLogFrequency(0)
            .setQueryThreadPoolSize(16)
            .setFailureDetectionTimeout(10000)
            .setClientFailureDetectionTimeout(10000)
            .setNetworkTimeout(10000)
            .setCacheConfiguration(
                new CacheConfiguration()
                    .setName("*")
                    .setCacheMode(CacheMode.PARTITIONED)
                    .setBackups(1)
                    .setAffinity(
                        new RendezvousAffinityFunction()
                            .setPartitions(256)
                    )
            )
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
            // TODO temporary fix for GG-22214
            .setIncludeEventTypes(EVT_CLUSTER_ACTIVATED, EVT_CLUSTER_DEACTIVATED)
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
     * @param cond Condition.
     */
    private void assertWithPoll(Callable<Boolean> cond) {
        with().pollInterval(500, MILLISECONDS).await().atMost(10, SECONDS).until(cond);
    }
}
