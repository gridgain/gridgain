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

import java.util.Collection;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.spi.GridSpiAbstractConfigTest;
import org.apache.ignite.testframework.junits.spi.GridSpiTest;
import org.junit.Test;

import static java.lang.Boolean.TRUE;
import static java.lang.System.setProperty;
import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_TCP_COMM_SET_LOCAL_HOST_ATTR;
import static org.apache.ignite.internal.util.IgniteUtils.spiAttribute;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.ATTR_HOST_NAMES;
import static org.apache.ignite.testframework.GridTestUtils.getFreeCommPort;

/**
 * TCP communication SPI config test.
 */
@GridSpiTest(spi = TcpCommunicationSpi.class, group = "Communication SPI")
public class GridTcpCommunicationSpiConfigSelfTest extends GridSpiAbstractConfigTest<TcpCommunicationSpi> {
    /** Set null to {@link IgniteConfiguration#setLocalHost}. */
    boolean setNullLocalHost;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setLocalHost("0.0.0.0");
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration optimize(IgniteConfiguration cfg) {
        return super.optimize(cfg).setLocalHost(setNullLocalHost ? null : cfg.getLocalHost());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNegativeConfig() throws Exception {
        checkNegativeSpiProperty(new TcpCommunicationSpi(), "localPort", 1023);
        checkNegativeSpiProperty(new TcpCommunicationSpi(), "localPort", 65636);
        checkNegativeSpiProperty(new TcpCommunicationSpi(), "localPortRange", -1);
        checkNegativeSpiProperty(new TcpCommunicationSpi(), "idleConnectionTimeout", 0);
        checkNegativeSpiProperty(new TcpCommunicationSpi(), "socketReceiveBuffer", -1);
        checkNegativeSpiProperty(new TcpCommunicationSpi(), "socketSendBuffer", -1);
        checkNegativeSpiProperty(new TcpCommunicationSpi(), "messageQueueLimit", -1);
        checkNegativeSpiProperty(new TcpCommunicationSpi(), "reconnectCount", 0);
        checkNegativeSpiProperty(new TcpCommunicationSpi(), "selectorsCount", 0);
        checkNegativeSpiProperty(new TcpCommunicationSpi(), "connectTimeout", -1);
        checkNegativeSpiProperty(new TcpCommunicationSpi(), "maxConnectTimeout", -1);
        checkNegativeSpiProperty(new TcpCommunicationSpi(), "socketWriteTimeout", -1);
        checkNegativeSpiProperty(new TcpCommunicationSpi(), "ackSendThreshold", 0);
        checkNegativeSpiProperty(new TcpCommunicationSpi(), "ackSendThreshold", -1);
        checkNegativeSpiProperty(new TcpCommunicationSpi(), "unacknowledgedMessagesBufferSize", -1);
        checkNegativeSpiProperty(new TcpCommunicationSpi(), "connectionsPerNode", 0);
        checkNegativeSpiProperty(new TcpCommunicationSpi(), "connectionsPerNode", -1);
        checkNegativeSpiProperty(new TcpCommunicationSpi(), "connectionsPerNode", Integer.MAX_VALUE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLocalPortRange() throws Exception {
        IgniteConfiguration cfg = getConfiguration();

        TcpCommunicationSpi commSpi = new TcpCommunicationSpi();

        commSpi.setLocalPortRange(0);
        commSpi.setLocalPort(getFreeCommPort());

        cfg.setCommunicationSpi(commSpi);

        startGrid(cfg);
    }

    /**
     * Test checks that {@link TcpCommunicationSpi#ATTR_HOST_NAMES} attribute
     * is empty only if {@link
     * IgniteSystemProperties#IGNITE_TCP_COMM_SET_LOCAL_HOST_ATTR} ==
     * {@code false} and local host is set via {@link
     * IgniteConfiguration#setLocalHost}.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_TCP_COMM_SET_LOCAL_HOST_ATTR, value = "false")
    public void testLocalHost() throws Exception {
        int nodeIdx = 0;
        checkLocalHost(startGrid(nodeIdx++), false, true);

        setNullLocalHost = true;
        checkLocalHost(startGrid(nodeIdx++), true, false);

        setProperty(IGNITE_TCP_COMM_SET_LOCAL_HOST_ATTR, TRUE.toString());

        setNullLocalHost = false;
        checkLocalHost(startGrid(nodeIdx++), false, false);

        setNullLocalHost = true;
        checkLocalHost(startGrid(nodeIdx++), true, false);
    }

    /**
     * Checking local host on node.
     *
     * @param node Node.
     * @param emptyLocHost Check whether {@link
     *      IgniteConfiguration#getLocalHost} is empty.
     * @param emptyHostNamesAttr Check whether {@link
     *      TcpCommunicationSpi#ATTR_HOST_NAMES} attribute is empty.
     */
    private void checkLocalHost(IgniteEx node, boolean emptyLocHost, boolean emptyHostNamesAttr) {
        requireNonNull(node);

        IgniteConfiguration cfg = node.configuration();
        assertEquals(emptyLocHost, isNull(cfg.getLocalHost()));

        TcpCommunicationSpi spi = (TcpCommunicationSpi)cfg.getCommunicationSpi();
        assertEquals(
            emptyHostNamesAttr,
            ((Collection<String>)node.localNode().attribute(spiAttribute(spi, ATTR_HOST_NAMES))).isEmpty()
        );
    }
}
