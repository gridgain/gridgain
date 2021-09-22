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
import org.apache.ignite.Ignition;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.SslMode;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import javax.net.ssl.SSLHandshakeException;

/** Test one-way SSL works with thin clients. */
@SuppressWarnings("ThrowableNotThrown")
public class OneWaySslThinClientTest extends GridCommonAbstractTest {

    /** */
    private SslContextFactory sslContextFactory;

    /** */
    private boolean sslClientAuth;

    /** */
    @After
    public void tearDown() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** */
    @Override
    protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setClientConnectorConfiguration(
                new ClientConnectorConfiguration().setSslEnabled(true).setSslClientAuth(sslClientAuth))
            .setSslContextFactory(sslContextFactory);
    }

    /** */
    private ClientConfiguration clientConfiguration() {
        ClientConfiguration clientCfg = new ClientConfiguration().setAddresses("127.0.0.1:10800");
        clientCfg.setSslContextFactory(sslContextFactory);
        clientCfg.setSslMode(SslMode.REQUIRED);

        return clientCfg;
    }

    /** */
    @Test
    public void testSimpleOneWay() throws Exception {
        sslContextFactory = GridTestUtils.sslTrustedFactory("node01", null);
        sslClientAuth = false;
        startGrid(0);

        sslContextFactory = GridTestUtils.sslTrustedFactory(null, "trustone");
        IgniteClient client = Ignition.startClient(clientConfiguration());

        client.createCache("foo").put("a", "b");

        Assert.assertEquals("b", client.cache("foo").get("a"));
    }

    /** */
    @Test
    public void testClientHasKeyServerDoesntTrust() throws Exception {
        sslContextFactory = GridTestUtils.sslTrustedFactory("node01", null);
        sslClientAuth = false;
        startGrid(0);

        sslContextFactory = GridTestUtils.sslTrustedFactory("node02", "trustone");
        IgniteClient client = Ignition.startClient(clientConfiguration());

        client.createCache("foo").put("a", "b");

        Assert.assertEquals("b", client.cache("foo").get("a"));
    }

    /** */
    @Test
    public void testClientTrustsNoOne() throws Exception {
        sslContextFactory = GridTestUtils.sslTrustedFactory("node01", null);
        sslClientAuth = false;
        startGrid(0);

        sslContextFactory = GridTestUtils.sslTrustedFactory(null, null);
        GridTestUtils.assertThrowsAnyCause(log, () ->  Ignition.startClient(clientConfiguration()),
            IgniteCheckedException.class, "SSL handshake failed");
    }

    /** */
    @Test
    public void testClientTrustsAnother() throws Exception {
        sslContextFactory = GridTestUtils.sslTrustedFactory("node01", null);
        sslClientAuth = false;
        startGrid(0);

        sslContextFactory = GridTestUtils.sslTrustedFactory(null, "trusttwo");
        GridTestUtils.assertThrowsAnyCause(log, () ->  Ignition.startClient(clientConfiguration()),
            IgniteCheckedException.class, "SSL handshake failed");
    }

    /** */
    @Test
    public void testClientAuthOverridesSslFactoryAuthTrue() throws Exception {
        sslContextFactory = GridTestUtils.sslTrustedFactory("node01", null);
        sslContextFactory.setNeedClientAuth(false);
        sslClientAuth = true;
        startGrid(0);

        sslContextFactory = GridTestUtils.sslTrustedFactory(null, "trustone");
        GridTestUtils.assertThrowsAnyCause(log, () ->  Ignition.startClient(clientConfiguration()),
            IgniteCheckedException.class, "SSL handshake failed");
    }

    /** */
    @Test
    public void testClientAuthOverridesSslFactoryAuthFalse() throws Exception {
        sslContextFactory = GridTestUtils.sslTrustedFactory("node01", null);
        sslContextFactory.setNeedClientAuth(true);
        sslClientAuth = false;
        startGrid(0);

        sslContextFactory = GridTestUtils.sslTrustedFactory(null, "trustone");
        IgniteClient client = Ignition.startClient(clientConfiguration());

        client.createCache("foo").put("a", "b");

        Assert.assertEquals("b", client.cache("foo").get("a"));
    }
}
