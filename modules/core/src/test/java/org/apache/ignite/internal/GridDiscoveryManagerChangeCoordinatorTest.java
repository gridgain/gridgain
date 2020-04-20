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

import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.util.UUID;
import java.util.regex.Pattern;
import org.junit.Test;

/**
 * Tests change coordinator event logging.
 */
public class GridDiscoveryManagerChangeCoordinatorTest extends GridCommonAbstractTest {
    /** Listener log messages. */
    private final ListeningTestLogger listeningLog = new ListeningTestLogger(false, log);

    /** */
    private static final String CRD_CHANGE_MSG  = "Coordinator changed \\[prev=TcpDiscoveryNode " +
        "\\[id=%s.*cur=TcpDiscoveryNode \\[id=%s.*";

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        listeningLog.clearListeners();

        stopAllGrids();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super
            .getConfiguration(igniteInstanceName)
            .setGridLogger(listeningLog)
            .setClientMode("client".equals(igniteInstanceName))
            .setDaemon("daemon".equals(igniteInstanceName));
    }

    /**
     * Checks that there are no messages like "coordinator changed" after client left topology.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testChangeCoordinatorLogging() throws Exception {
        //Start 2 server nodes
        IgniteEx srv1 = startGrid("server1");
        startGrid("server2");

        srv1.cluster().active();

        IgniteEx client = startGrid("client");

        stopGrid("server1");
        srv1 = startGrid("server1");

        stopGrid("server2");
        startGrid("server2");

        UUID clientClusterNode = client.localNode().id();
        UUID srv1ClusterNode = srv1.localNode().id();

        Pattern ptrn = Pattern.compile(String.format(CRD_CHANGE_MSG, srv1ClusterNode, clientClusterNode));

        LogListener lsnr = LogListener.matches(ptrn).build();

        listeningLog.registerListener(lsnr);

        stopGrid("client");

        // Check that there are no messages like "coordinator changed server1 -> client"
        assertFalse(lsnr.check());
    }

    /**
     * Checks change coordinator event logging with daemon node in topology.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testChangeCoordinatorLoggingDaemonNodeInTopology() throws Exception {
        // Start 2 server nodes
        IgniteEx srv1 = startGrid("server1");
        IgniteEx srv2 = startGrid("server2");

        srv1.cluster().active();

        // Check coordinator = server1
        assertTrue(((TcpDiscoverySpi) srv1.context().config().getDiscoverySpi()).isLocalNodeCoordinator());

        // Add a client node, daemon node and 3-d server node, which is not in the baseline
        startGrid("client");
        IgniteEx daemon = startGrid("daemon");
        IgniteEx srv3 = startGrid("server3");

        UUID srv1ClusterNode = srv1.localNode().id();
        UUID srv2ClusterNode = srv2.localNode().id();

        Pattern ptrn = Pattern.compile(String.format(CRD_CHANGE_MSG, srv1ClusterNode, srv2ClusterNode));

        LogListener lsnr = LogListener.matches(ptrn).build();

        listeningLog.registerListener(lsnr);

        stopGrid("server1");

        UUID crdUUID = ((TcpDiscoverySpi)srv3.context().config().getDiscoverySpi()).getCoordinator();

        // Coordinator changed server1 -> server2
        assertEquals(srv2.localNode().id(), crdUUID);

        // Check that there is message like "coordinator changed server1 -> server2"
        assertTrue(lsnr.check());

        ptrn = Pattern.compile(String.format(CRD_CHANGE_MSG, srv2ClusterNode, srv1ClusterNode));

        lsnr = LogListener.matches(ptrn).build();

        listeningLog.registerListener(lsnr);

        srv1 = startGrid("server1");

        crdUUID = ((TcpDiscoverySpi) srv3.context().config().getDiscoverySpi()).getCoordinator();

        // Server2 is still the coordinator
        assertEquals(srv2.localNode().id(), crdUUID);

        // Check that there are no messages like "coordinator changed server2 -> server1"
        assertFalse(lsnr.check());

        ptrn = Pattern.compile(String.format(CRD_CHANGE_MSG, srv2ClusterNode, daemon.localNode().id()));

        lsnr = LogListener.matches(ptrn).build();

        listeningLog.registerListener(lsnr);

        stopGrid("server2");

        crdUUID = ((TcpDiscoverySpi) srv3.context().config().getDiscoverySpi()).getCoordinator();

        // Coordinator changed server2 -> daemon
        assertEquals(daemon.localNode().id(), crdUUID);

        // Check that there is message like "coordinator changed server2 -> daemon"
        assertTrue(lsnr.check());

        lsnr = LogListener.matches("Coordinator changed").build();

        listeningLog.registerListener(lsnr);

        stopGrid("client");

        crdUUID = ((TcpDiscoverySpi) srv3.context().config().getDiscoverySpi()).getCoordinator();

        // daemon is still coordinator
        assertEquals(daemon.localNode().id(), crdUUID);

        // Check that there are no messages like "coordinator changed"
        assertFalse(lsnr.check());

        stopGrid("server3");

        crdUUID = ((TcpDiscoverySpi) srv1.context().config().getDiscoverySpi()).getCoordinator();

        // daemon is still coordinator
        assertEquals(daemon.localNode().id(), crdUUID);

        // Check that there are no messages like "coordinator changed"
        assertFalse(lsnr.check());
    }
}
