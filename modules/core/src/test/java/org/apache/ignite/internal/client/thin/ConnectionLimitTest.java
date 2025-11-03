/*
 * Copyright 2023 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.client.thin;

import org.apache.ignite.Ignite;
import org.apache.ignite.client.ClientConnectionException;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.odbc.ClientListenerProcessor;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.mxbean.ClientProcessorMXBean;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

/**
 * Thin client connections limit tests.
 */
public class ConnectionLimitTest extends AbstractThinClientTest {
    /**
     * Default timeout value.
     */
    private static final int MAX_CONNECTIONS = 4;

    /** */
    @Test
    public void testConnectionsRejectedOnLimitReached() throws Exception {
        try (Ignite ignite = startGrid(0)) {
            ClientProcessorMXBean mxBean = getMxBean(ignite.name(), "Clients",
                    ClientProcessorMXBean.class, ClientListenerProcessor.class);

            mxBean.setMaxConnectionsPerNode(MAX_CONNECTIONS);

            List<IgniteClient> clients = new ArrayList<>();
            try {
                for (int i = 0; i < MAX_CONNECTIONS; ++i) {
                    clients.add(startClient(0));
                }

                assertClientConnectionRejected();
            }
            finally {
                clients.forEach(IgniteClient::close);
            }
        }
    }

    /** */
    @Test
    public void testConnectionsRejectedOnLimitLowered() throws Exception {
        try (Ignite ignite = startGrid(0)) {
            ClientProcessorMXBean mxBean = getMxBean(ignite.name(), "Clients",
                    ClientProcessorMXBean.class, ClientListenerProcessor.class);

            mxBean.setMaxConnectionsPerNode(MAX_CONNECTIONS);

            List<IgniteClient> clients = new ArrayList<>();
            try {
                for (int i = 0; i < MAX_CONNECTIONS; ++i) {
                    clients.add(startClient(0));
                }

                assertClientConnectionRejected();

                mxBean.setMaxConnectionsPerNode(MAX_CONNECTIONS - 2);

                assertClientConnectionRejected();
            }
            finally {
                clients.forEach(IgniteClient::close);
            }
        }
    }

    /** */
    @Test
    public void testConnectionsRejectedOnLimitRaised() throws Exception {
        try (Ignite ignite = startGrid(0)) {
            ClientProcessorMXBean mxBean = getMxBean(ignite.name(), "Clients",
                    ClientProcessorMXBean.class, ClientListenerProcessor.class);

            mxBean.setMaxConnectionsPerNode(MAX_CONNECTIONS);

            List<IgniteClient> clients = new ArrayList<>();
            try {
                for (int i = 0; i < MAX_CONNECTIONS; ++i) {
                    clients.add(startClient(0));
                }

                assertClientConnectionRejected();

                mxBean.setMaxConnectionsPerNode(MAX_CONNECTIONS + 2);

                for (int i = 0; i < 2; ++i) {
                    clients.add(startClient(0));
                }

                assertClientConnectionRejected();
            }
            finally {
                clients.forEach(IgniteClient::close);
            }
        }
    }

    /** */
    @Test
    public void testIdleConnectionsRejectedOnLimitReached() throws Exception {
        IgniteConfiguration cfg = getConfiguration();
        ClientConnectorConfiguration connectorCfg = cfg.getClientConnectorConfiguration();
        connectorCfg.setIdleTimeout(0);
        connectorCfg.setHandshakeTimeout(0);
        connectorCfg.setSocketReceiveBufferSize(4 * 1024 * 1024);
        connectorCfg.setSocketSendBufferSize(4 * 1024 * 1024);
        cfg.setClientConnectorConfiguration(new ClientConnectorConfiguration(connectorCfg));

        try (Ignite ignite = startGrid(cfg)) {
            ClientProcessorMXBean mxBean = getMxBean(ignite.name(), "Clients",
                    ClientProcessorMXBean.class, ClientListenerProcessor.class);

            int socketNum = 10_000;
            int connectionLimit = 10;

            mxBean.setMaxConnectionsPerNode(connectionLimit);

            ClusterNode node = ignite.cluster().localNode();
            List<Socket> conns = new ArrayList<>();
            try {
                for (int i = 0; i < socketNum; ++i) {
                    conns.add(new Socket(clientHost(node), clientPort(node)));
                }
            }
            finally {
                for (Socket conn : conns) {
                    IgniteUtils.close(conn, log());
                }
            }

            // Now check that we still can connect 10 clients
            List<IgniteClient> clients = new ArrayList<>();
            try {
                for (int i = 0; i < connectionLimit; ++i) {
                    GridTestUtils.waitForCondition(() -> {
                        try {
                            IgniteClient client = startClient(ignite);

                            // Just checking that the client is functioning
                            client.cluster().forRandom().node();
                            clients.add(client);
                            return true;
                        } catch (Exception e) {
                            return false;
                        }
                    }, 5000, 1000);
                }
            }
            finally {
                clients.forEach(IgniteClient::close);
            }
        }
    }

    private void assertClientConnectionRejected() {
        ClientConnectionException err = GridTestUtils.assertThrows(log(),
                () -> startClient(0),
                ClientConnectionException.class,
                null);

        GridTestUtils.assertMatches(log(), err.getMessage(), "(Channel is closed)|(connection was closed)");
    }
}
