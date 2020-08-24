/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteMessaging;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.EventType;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests that localId for cluster groups will be changed after reconnect.
 */
public class IgniteClientCheckClusterGroupLocalIdAfterReconnect extends GridCommonAbstractTest {
    /** Ignite start up mode*/
    private boolean serverMode;

    /** Object for messaging*/
    static class External implements Externalizable {
        /** */
        private External() {}

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {}

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {}
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpDiscoverySpi discoverySpi = new TcpDiscoverySpi();

        TcpCommunicationSpi communicationSpi = new TcpCommunicationSpi();

        communicationSpi.setSocketWriteTimeout(20000);

        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();

        List<String> ips = new ArrayList<>();

        ips.add("127.0.0.1");

        ipFinder.setAddresses(ips);

        discoverySpi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(discoverySpi);

        cfg.setCommunicationSpi(communicationSpi);

        cfg.setClientMode(!serverMode);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 600_000;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * Test check that local id in cluster group was change and client
     * will be able to send the message to itself after reconnect.
     */
    @Test
    public void testClusterGroupLocalIdAfterClientReconnect() throws Exception {
        serverMode = true;

        Ignite server = startGrid(0);

        serverMode = false;

        Ignite client = startGrid(1);

        UUID serverId = server.cluster().node().id();

        UUID clientId = client.cluster().node().id();

        assertFalse("Server and client nodes have identical ids", serverId.equals(clientId));

        ClusterGroup cg1 = client.cluster().forLocal();

        assertNotNull("Local client ID is different with local ClusterGroup node id. ", cg1.node(clientId));

        // check sending messages is possible while connected
        IgniteMessaging messaging = client.message(client.cluster().forLocal());

        messaging.localListen("topic", (IgniteBiPredicate<UUID, Object>) (uuid, n) -> {
            System.out.println(n);

            return true;
        });

        messaging.send("topic", new External());

        CountDownLatch discSignal = new CountDownLatch(1);

        client.events().localListen((IgnitePredicate<DiscoveryEvent>) evt -> {
            discSignal.countDown();

            return true;
        }, EventType.EVT_CLIENT_NODE_DISCONNECTED);

        server.close();

        assertTrue("client did not disconnect", discSignal.await(20, TimeUnit.SECONDS));

        serverMode = true;

        server = startGrid(0);

        // wait for client reconnect
        IgniteFuture future = client.cluster().clientReconnectFuture();

        assertNotNull(future);

        future.get(60_000);   // throws if times out

        ClusterGroup cg2 = client.cluster().forLocal();

        UUID newClientId = client.cluster().localNode().id();

        assertNotNull("Local client ID wasn't changed for local ClusterGroup.", cg2.node(newClientId));

        Thread.sleep(5000);

        // check sending messages is possible after reconnecting
        messaging = client.message(client.cluster().forLocal());

        messaging.send("topic", new External());

        client.close();

        server.close();
    }
}
