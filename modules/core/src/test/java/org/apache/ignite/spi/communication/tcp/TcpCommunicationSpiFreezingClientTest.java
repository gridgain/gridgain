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

package org.apache.ignite.spi.communication.tcp;

import java.util.Map;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.nio.GridCommunicationClient;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.multijvm.IgniteProcessProxy;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.junit.Assume.assumeTrue;

/**
 * Tests that freezing due to JVM STW client will be failed if connection can't be established.
 */
@WithSystemProperty(key = "IGNITE_ENABLE_FORCIBLE_NODE_KILL", value = "true")
public class TcpCommunicationSpiFreezingClientTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setFailureDetectionTimeout(getTestTimeout());
        cfg.setClientFailureDetectionTimeout(getTestTimeout());

        TcpCommunicationSpi spi = new TcpCommunicationSpi();

        spi.setConnectTimeout(1000);
        spi.setMaxConnectTimeout(1000);
        spi.setIdleConnectionTimeout(100);
        spi.setSharedMemoryPort(-1);

        cfg.setCommunicationSpi(spi);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected boolean isMultiJvm() {
        return true;
    }

    @Before
    public void before() throws Exception {
        stopAllGrids();
    }

    /** */
    @After
    public void after() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("ThrowableNotThrown")
    @Test
    public void testFreezingClient() throws Exception {
        assumeTrue("The test reqires the 'kill' command.", U.isUnix() || U.isMacOs());

        Ignite srv = startGrid(0);
        IgniteProcessProxy client = (IgniteProcessProxy)startClientGrid("client");

        // Close communication connections by idle.
        waitConnectionsClosed(srv);

        // Simulate freeze/STW on the client.
        client.getProcess().suspend();

        // Open new communication connection to the freezing client.
        GridTestUtils.assertThrowsWithCause(
            () -> srv.compute(srv.cluster().forClients()).withNoFailover().run(() -> {}),
            ClusterTopologyException.class);

        assertTrue(
            "Did not see the client leaving the topology",
            waitForCondition(() -> srv.cluster().nodes().size() == 1, 10_000)
        );
    }

    /** Waits for all communication connections closed by idle. */
    private void waitConnectionsClosed(Ignite node) {
        TcpCommunicationSpi spi = (TcpCommunicationSpi)node.configuration().getCommunicationSpi();
        Map<UUID, GridCommunicationClient[]> clientsMap = GridTestUtils.getFieldValue(spi, "clientPool", "clients");

        try {
            assertTrue(waitForCondition(() -> {
                for (GridCommunicationClient[] clients : clientsMap.values()) {
                    if (clients == null)
                        continue;

                    for (GridCommunicationClient client : clients) {
                        if (client != null)
                            return false;
                    }
                }

                return true;
            }, getTestTimeout()));
        }
        catch (IgniteInterruptedCheckedException e) {
            throw U.convertException(e);
        }
    }
}
