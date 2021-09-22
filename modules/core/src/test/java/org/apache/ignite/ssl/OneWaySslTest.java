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

import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import javax.net.ssl.SSLHandshakeException;

/** Test one-way SSL works with thick clients. */
@SuppressWarnings("ThrowableNotThrown")
public class OneWaySslTest extends GridCommonAbstractTest {

    /** */
    private SslContextFactory sslContextFactory;

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
            .setSslContextFactory(sslContextFactory)
            // forceClientToServerConnections shouldn't matter in this particular test
            // but in general one-way SSL may not work without it.
            .setCommunicationSpi(new TcpCommunicationSpi().setForceClientToServerConnections(true));
    }

    /** */
    @Test
    public void testSimpleOneWay() throws Exception {
        sslContextFactory = GridTestUtils.sslTrustedFactory("node01", null);
        sslContextFactory.setNeedClientAuth(false);
        startGrid(0);

        sslContextFactory = GridTestUtils.sslTrustedFactory(null, "trustone");
        Ignite client = startClientGrid(1);

        client.createCache("foo").put("a", "b");

        Assert.assertEquals("b", client.cache("foo").get("a"));
    }

    /** */
    @Test
    public void testClientHasKeyServerDoesntTrust() throws Exception {
        sslContextFactory = GridTestUtils.sslTrustedFactory("node01", null);
        sslContextFactory.setNeedClientAuth(false);
        startGrid(0);

        sslContextFactory = GridTestUtils.sslTrustedFactory("node02", "trustone");
        Ignite client = startClientGrid(1);

        client.createCache("foo").put("a", "b");

        Assert.assertEquals("b", client.cache("foo").get("a"));
    }

    /** */
    @Test
    public void testClientTrustsNoOne() throws Exception {
        sslContextFactory = GridTestUtils.sslTrustedFactory("node01", null);
        sslContextFactory.setNeedClientAuth(false);
        startGrid(0);

        sslContextFactory = GridTestUtils.sslTrustedFactory(null, null);
        GridTestUtils.assertThrowsWithCause(() -> startClientGrid(1), SSLHandshakeException.class);
    }

    /** */
    @Test
    public void testClientTrustsAnother() throws Exception {
        sslContextFactory = GridTestUtils.sslTrustedFactory("node01", null);
        sslContextFactory.setNeedClientAuth(false);
        startGrid(0);

        sslContextFactory = GridTestUtils.sslTrustedFactory(null, "trusttwo");
        GridTestUtils.assertThrowsWithCause(() -> startClientGrid(1), SSLHandshakeException.class);
    }
}
