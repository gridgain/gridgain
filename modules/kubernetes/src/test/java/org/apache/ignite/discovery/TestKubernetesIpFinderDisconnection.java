/*
 * Copyright 2021 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.discovery;

import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.events.EventType.EVT_CLIENT_NODE_DISCONNECTED;

/**
 * Test that client node is disconnected from a cluster if KubernetesIpFinder fails.
 */
public class TestKubernetesIpFinderDisconnection extends KubernetesDiscoveryAbstractTest {

    /** */
    @Test
    public void testClientNodeDisconnectsWithMaxRetries() throws Exception {

        IgniteConfiguration cfgSrv = getConfiguration(getTestIgniteInstanceName(), false);

        IgniteConfiguration cfgClient = getConfiguration("client", true);
        ((TcpDiscoverySpi) cfgClient.getDiscoverySpi()).setReconnectCount(1);

        runDisconnectionTest(cfgSrv, cfgClient);
    }

    /** */
    @Test
    public void testClientNodeDisconnectsWithDefaultSettings() throws Exception {

        IgniteConfiguration cfgSrv = getConfiguration(getTestIgniteInstanceName(), false);
        IgniteConfiguration cfgClient = getConfiguration("client", true);

        runDisconnectionTest(cfgSrv, cfgClient);
    }

    /**
     *
     */
    private void runDisconnectionTest(IgniteConfiguration cfgNode1, IgniteConfiguration cfgNode2) throws Exception {
        mockServerResponse();

        IgniteEx crd = startGrid(cfgNode1);
        String crdAddr = crd.localNode().addresses().iterator().next();

        mockServerResponse(crdAddr);

        IgniteEx client = startGrid(cfgNode2);

        waitForTopology(2);

        Ignition.stop(crd.name(), true);

        CountDownLatch latch = new CountDownLatch(1);

        client.events().localListen(event -> {
            latch.countDown();

            return true;
        }, EVT_CLIENT_NODE_DISCONNECTED);

        assertTrue("Failed to wait for client node disconnected.", latch.await(20, SECONDS));
    }
}
