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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.failure.NoOpFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.cluster.IgniteClusterEx;
import org.apache.ignite.opencensus.spi.tracing.OpenCensusTracingSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.IgniteTestResources;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.gridgain.AbstractGridWithAgentTest;
import org.gridgain.dto.tracing.Span;
import org.gridgain.dto.topology.TopologySnapshot;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_BASELINE_AUTO_ADJUST_ENABLED;
import static org.apache.ignite.events.EventType.EVT_CLUSTER_ACTIVATED;
import static org.apache.ignite.events.EventType.EVT_CLUSTER_DEACTIVATED;
import static org.awaitility.Awaitility.with;
import static org.gridgain.agent.StompDestinationsUtils.buildBaselineTopologyDest;
import static org.gridgain.agent.StompDestinationsUtils.buildClusterActiveStateDest;
import static org.gridgain.agent.StompDestinationsUtils.buildClusterAddDest;
import static org.gridgain.agent.StompDestinationsUtils.buildClusterNodeConfigurationDest;
import static org.gridgain.agent.StompDestinationsUtils.buildClusterTopologyDest;
import static org.gridgain.agent.StompDestinationsUtils.buildMetricsDest;
import static org.gridgain.agent.StompDestinationsUtils.buildMetricsPullTopic;
import static org.gridgain.agent.StompDestinationsUtils.buildSaveSpanDest;

/**
 * Agent integration tests.
 */
public class AgentSelfTest extends AbstractGridWithAgentTest {
    /**
     * Should send initial states to backend.
     */
    @Test
    public void shouldSendInitialStates() throws Exception {
        IgniteEx ignite = (IgniteEx) startGrid();
        changeGmcUri(ignite);

        IgniteCluster cluster = ignite.cluster();
        cluster.active(true);

        assertWithPoll(() -> interceptor.getPayload(buildClusterAddDest()) != null);
        assertWithPoll(() -> interceptor.getPayload(buildClusterTopologyDest(cluster.id())) != null);
        assertWithPoll(() -> interceptor.getPayload(buildBaselineTopologyDest(cluster.id())) != null);
        assertWithPoll(() -> interceptor.getPayload(buildClusterActiveStateDest(cluster.id())) != null);
        assertWithPoll(() -> interceptor.getPayload(buildClusterNodeConfigurationDest(cluster.id())) != null);
        assertWithPoll(() -> interceptor.getPayload(buildSaveSpanDest(cluster.id())) != null);
    }

    /**
     * Should send changed cluster topology.
     */
    @Test
    public void shouldSendChangedClusterTopology() throws Exception {
        IgniteEx ignite = startGrid(0);
        changeGmcUri(ignite);

        IgniteClusterEx cluster = ignite.cluster();
        cluster.active(true);

        startGrid(1);

        assertWithPoll(
            () -> {
                TopologySnapshot top = interceptor.getPayload(buildClusterTopologyDest(cluster.id()), TopologySnapshot.class);
                return top != null && top.getNodes().size() == 2;
            }
        );
    }

    /**
     * Should send changed baseline topology.
     */
    @Test
    @WithSystemProperty(key = IGNITE_BASELINE_AUTO_ADJUST_ENABLED, value = "false")
    public void shouldSendChangedBaselineTopology() throws Exception {
        IgniteEx ignite_1 = startGrid(0);
        changeGmcUri(ignite_1);

        IgniteCluster cluster = ignite_1.cluster();
        cluster.active(true);

        assertWithPoll(
            () -> {
                TopologySnapshot top = interceptor.getPayload(buildBaselineTopologyDest(cluster.id()), TopologySnapshot.class);
                return top != null && top.getNodes().size() == 1;
            }
        );

        Ignite ignite_2 = startGrid(1);

        Collection<ClusterNode> nodes = ignite_1.cluster().forServers().nodes();
        ignite_1.cluster().setBaselineTopology(nodes);

        assertWithPoll(
            () -> {
                TopologySnapshot top = interceptor.getPayload(buildBaselineTopologyDest(cluster.id()), TopologySnapshot.class);
                return top != null && top.getNodes().size() == 2;
            }
        );
    }

    /**
     * Should send changed active state.
     */
    @Test
    public void shouldSendChangedActiveState() throws Exception {
        IgniteEx ignite_1 = startGrid(0);
        changeGmcUri(ignite_1);

        IgniteCluster cluster = ignite_1.cluster();
        cluster.active(true);

        assertWithPoll(
            () -> {
                Boolean state = interceptor.getPayload(buildClusterActiveStateDest(cluster.id()), Boolean.class);
                return state != null && state;
            }
        );

        cluster.active(false);

        assertWithPoll(
            () -> {
                Boolean state = interceptor.getPayload(buildClusterActiveStateDest(cluster.id()), Boolean.class);
                return state != null && !state;
            }
        );
    }

    /**
     * Should send changed baseline topology.
     */
    @Test
    public void
    shouldSendMetricsOnPoll() throws Exception {
        IgniteEx ignite_1 = startGrid(0);
        changeGmcUri(ignite_1);

        IgniteCluster cluster = ignite_1.cluster();
        cluster.active(true);

        assertWithPoll(
            () -> interceptor.isSubscribedOn(buildMetricsPullTopic())
        );

        template.convertAndSend("/topic/agent/metrics/pull", "pull");

        assertWithPoll(
            () -> {
                String metrics = new String((byte[]) interceptor.getPayload(buildMetricsDest(cluster.id())));
                return metrics != null && metrics.contains(cluster.tag());
            }
        );
    }

    /**
     * Should send changed baseline topology.
     */
    @Test
    public void shouldSendSpans() throws Exception {
        IgniteEx ignite_1 = startGrid(0);
        changeGmcUri(ignite_1);

        IgniteCluster cluster = ignite_1.cluster();
        cluster.active(true);

        assertWithPoll(
                () -> {
                    List<Span> spans = interceptor.getPayload(buildSaveSpanDest(cluster.id()), List.class);
                    return spans != null && !spans.isEmpty();
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
        with().pollInterval(500, MILLISECONDS).await().atMost(20, SECONDS).until(cond);
    }
}
