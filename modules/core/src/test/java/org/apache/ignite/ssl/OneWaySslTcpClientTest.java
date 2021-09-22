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

package org.apache.ignite.ssl;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientFactory;
import org.apache.ignite.internal.client.impl.GridClientImpl;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Test;

import java.util.Collections;
import java.util.UUID;

/** Test one-way SSL works with thin clients. */
@SuppressWarnings("ThrowableNotThrown")
@WithSystemProperty(key = "IGNITE_TCP_CLIENT_INIT_RETRY_CNT", value = "1")
@WithSystemProperty(key = "IGNITE_TCP_CLIENT_INIT_RETRY_INTERVAL", value = "50")
public class OneWaySslTcpClientTest extends GridCommonAbstractTest {

    /** */
    private SslContextFactory sslContextFactory;

    /** */
    private boolean sslClientAuth;

    /** */
    @After
    public void tearDown() throws Exception {
        stopAllGrids();
    }

    /** */
    @Override
    protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setConnectorConfiguration(new ConnectorConfiguration().setSslEnabled(true).setSslClientAuth(sslClientAuth))
            .setSslContextFactory(sslContextFactory);
    }

    /** */
    @Test
    public void testClientHasKeyServerDoesntTrust() throws Exception {
        sslContextFactory = GridTestUtils.sslTrustedFactory("node01", null);
        sslClientAuth = false;
        startGrid(0);

        try (GridClientImpl client = createClient("node02", "trustone")) {
            UUID nodeId = grid(0).cluster().localNode().id();

            assertNotNull(client.topology().node(nodeId));
        }
    }

    /** */
    @Test
    public void testServerTrustsAnother() throws Exception {
        sslContextFactory = GridTestUtils.sslTrustedFactory("node01", "trustthree");
        sslClientAuth = false;
        startGrid(0);

        try (GridClientImpl client = createClient("node02", "trustone")) {
            UUID nodeId = grid(0).cluster().localNode().id();

            assertNotNull(client.topology().node(nodeId));
        }
    }

    /** */
    @Test
    public void testClientTrustsAnother() throws Exception {
        sslContextFactory = GridTestUtils.sslTrustedFactory("node01", null);
        sslClientAuth = false;
        startGrid(0);

        try (GridClientImpl client = createClient("node02", "trusttwo")) {
            GridTestUtils.assertThrowsAnyCause(log,
                client::connection,
                IgniteCheckedException.class, "SSL handshake failed");
        }
    }

    /** */
    @Test
    public void testClientAuthOverridesSslFactoryAuthTrue() throws Exception {
        sslContextFactory = GridTestUtils.sslTrustedFactory("node01", "trustone");
        sslContextFactory.setNeedClientAuth(false);
        sslClientAuth = true;
        startGrid(0);

        try (GridClientImpl client = createClient("node02", "trustone")) {
            GridTestUtils.assertThrowsAnyCause(log,
                client::connection,
                IgniteCheckedException.class, "SSL handshake failed");
        }
    }

    /** */
    @Test
    public void testClientAuthOverridesSslFactoryAuthFalse() throws Exception {
        sslContextFactory = GridTestUtils.sslTrustedFactory("node01", null);
        sslContextFactory.setNeedClientAuth(true);
        sslClientAuth = false;
        startGrid(0);

        try (GridClientImpl client = createClient("node02", "trustone")) {
            UUID nodeId = grid(0).cluster().localNode().id();

            assertNotNull(client.topology().node(nodeId));
        }
    }

    private GridClientImpl createClient(String keyStore, String trustStore) throws Exception {
        GridClientConfiguration cfg = new GridClientConfiguration()
            .setSslContextFactory(GridTestUtils.gridSslTrustedFactory(keyStore, trustStore))
            .setServers(Collections.singleton("127.0.0.1:11211"));

        return (GridClientImpl) GridClientFactory.start(cfg);
    }
}
