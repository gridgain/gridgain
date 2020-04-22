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

package org.apache.ignite.internal.managers.discovery;

import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class IgniteTopologyPrintFormatSelfTest extends GridCommonAbstractTest {
    /** */
    private static final String ALIVE_NODES_MSG = "aliveNodes=[";

    /** */
    private ListeningTestLogger testLog = new ListeningTestLogger(false, log);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName)
            .setGridLogger(testLog);

        if (igniteInstanceName.endsWith("client"))
            cfg.setClientMode(true);

        if (igniteInstanceName.endsWith("client_force_server")) {
            cfg.setClientMode(true);
            ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setForceServerMode(true);
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testServerLogs() throws Exception {
        doServerLogTest(false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testServerDebugLogs() throws Exception {
        doServerLogTest(true);
    }

    /**
     * @param dbg debug flag.
     * @throws Exception If failed.
     */
    private void doServerLogTest(boolean dbg) throws Exception {
        String nodeId8;

        testLog = new ListeningTestLogger(dbg, log);

        LogListener aliveNodesLsnr = LogListener.matches(ALIVE_NODES_MSG).times(dbg ? 0 : 4).build();

        testLog.registerListener(aliveNodesLsnr);

        LogListener lsnr;

        try {
            Ignite srv = startGrid("server");

            nodeId8 = U.id8(srv.cluster().localNode().id());

            lsnr = LogListener.matches( s -> s.contains("Topology snapshot [ver=2, locNode=" + nodeId8 + ", servers=2, clients=0,")
                    || (s.contains(">>> Number of server nodes: 2") && s.contains(">>> Number of client nodes: 0"))).build();

            testLog.registerListener(lsnr);

            Ignite srv1 = startGrid("server1");

            waitForDiscovery(srv, srv1);
        }
        finally {
            stopAllGrids();
        }

        assertTrue(lsnr.check());

        checkNodesAdditionalLogging(aliveNodesLsnr);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testServerAndClientLogs() throws Exception {
        doServerAndClientTest(false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testServerAndClientDebugLogs() throws Exception {
        doServerAndClientTest(true);
    }

    /**
     * @param dbg Log.
     * @throws Exception If failed.
     */
    private void doServerAndClientTest(boolean dbg) throws Exception {
        String nodeId8;

        testLog = new ListeningTestLogger(dbg, log);

        LogListener aliveNodesLsnr = LogListener.matches(ALIVE_NODES_MSG).times(dbg ? 0 : 16).build();

        testLog.registerListener(aliveNodesLsnr);

        LogListener lsnr;

        try {
            Ignite srv = startGrid("server");

            nodeId8 = U.id8(srv.cluster().localNode().id());

            lsnr = LogListener.matches( s -> s.contains("Topology snapshot [ver=4, locNode=" + nodeId8 + ", servers=2, clients=2,")
                    || (s.contains(">>> Number of server nodes: 2") && s.contains(">>> Number of client nodes: 2"))).build();

            testLog.registerListener(lsnr);

            Ignite srv1 = startGrid("server1");
            Ignite client1 = startGrid("first client");
            Ignite client2 = startGrid("second client");

            waitForDiscovery(srv, srv1, client1, client2);
        }
        finally {
            stopAllGrids();
        }

        assertTrue(lsnr.check());

        checkNodesAdditionalLogging(aliveNodesLsnr);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testForceServerAndClientLogs() throws Exception {
        doForceServerAndClientTest(false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testForceServerAndClientDebugLogs() throws Exception {
        doForceServerAndClientTest(true);
    }

    /**
     * @param dbg Log.
     * @throws Exception If failed.
     */
    private void doForceServerAndClientTest(boolean dbg) throws Exception {
        String nodeId8;

        testLog = new ListeningTestLogger(dbg, log);

        LogListener aliveNodesLsnr = LogListener.matches(ALIVE_NODES_MSG).times(dbg ? 0 : 25).build();

        testLog.registerListener(aliveNodesLsnr);

        LogListener lsnr;

        try {
            Ignite srv = startGrid("server");

            nodeId8 = U.id8(srv.cluster().localNode().id());

            lsnr = LogListener.matches( s -> s.contains("Topology snapshot [ver=5, locNode=" + nodeId8 + ", servers=2, clients=3,")
                    || (s.contains(">>> Number of server nodes: 2") && s.contains(">>> Number of client nodes: 3"))).build();

            testLog.registerListener(lsnr);

            Ignite srv1 = startGrid("server1");
            Ignite client1 = startGrid("first client");
            Ignite client2 = startGrid("second client");
            Ignite forceServClnt3 = startGrid("third client_force_server");

            waitForDiscovery(srv, srv1, client1, client2, forceServClnt3);

            assertTrue(lsnr.check());
        }
        finally {
            stopAllGrids();
        }

        assertTrue(lsnr.check());

        checkNodesAdditionalLogging(aliveNodesLsnr);
    }

    /**
     * Check nodes details in log.
     *
     * @param aliveNodesLsnr log listener.
     */
    private void checkNodesAdditionalLogging(LogListener aliveNodesLsnr) {
        if (log.isDebugEnabled())
            assertFalse(aliveNodesLsnr.check());
        else if (log.isInfoEnabled())
            assertTrue(aliveNodesLsnr.check());
    }
}
