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

import java.util.Collections;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.failure.NoOpFailureHandler;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.opencensus.spi.tracing.OpenCensusTracingSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import static org.apache.ignite.events.EventType.EVT_CLUSTER_ACTIVATED;
import static org.apache.ignite.events.EventType.EVT_CLUSTER_DEACTIVATED;

/**
 * Node launcher with agent (as it will be in classpath).
 */
public class NodeWithAgentLauncher {
    /**
     * @param args Arguments.
     */
    public static void main(String[] args) {
        Ignite ignite = F.isEmpty(args) ? startFromCode() : startFromSpringXml(args[0]);

        ignite.cluster().active(true);

        CacheConfiguration<Integer, String> testCache = new CacheConfiguration<>("testCache");

        testCache.setIndexedTypes(Integer.class, String.class);

        ignite.getOrCreateCache(testCache);
    }

    /**
     * @return Ignite instance.
     */
    private static Ignite startFromCode() {
        IgniteConfiguration cfg = new IgniteConfiguration()
            .setIgniteInstanceName("node-with-gmc-agent")
            .setAuthenticationEnabled(false)
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
                            .setMaxSize(256 * 1024 * 1024)
                    )
            )
            // TODO temporary fix for GG-22214
            .setIncludeEventTypes(EVT_CLUSTER_ACTIVATED, EVT_CLUSTER_DEACTIVATED)
            .setTracingSpi(new OpenCensusTracingSpi())
            .setFailureHandler(new NoOpFailureHandler())
            .setDiscoverySpi(
                new TcpDiscoverySpi()
                    .setIpFinder(
                        new TcpDiscoveryVmIpFinder()
                            .setAddresses(Collections.singletonList("127.0.0.1:47500..47505"))
                    )
            );

        return Ignition.start(cfg);
    }

    /**
     * @param springCfgPath Spring XML configuration file path or URL.
     * @return Ignite instance.
     */
    private static Ignite startFromSpringXml(String springCfgPath) {
        return Ignition.start(springCfgPath);
    }
}
