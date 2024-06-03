/*
 * Copyright 2024 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.spi.communication.tcp.internal;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests filtering of network interfaces.
 */
public class BlacklistNetwokInterfacesTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpCommunicationSpi communicationSpi = new TcpCommunicationSpi();

        // These settings enable wildcared local address and blacklist all IPv4 network interfaces.
        // It will dissalow a node to start due to all IPv4 addresses being blacklisted.
        // If IPv6 network interfaces are present, the node will start successfully
        // but communication attributes will not contain any IPv4 addresses.
        communicationSpi.setLocalAddress("0.0.0.0");
        communicationSpi.setNetworkInterfacesBlacklist(Collections.singletonList("*.*.*.*"));

        cfg.setCommunicationSpi(communicationSpi);

        return cfg;
    }

    @Test
    public void testBlacklistFilter() throws Exception {
        try {
            IgniteUtils.resetCachedLocalAddressAndHostNames();

            IgniteEx srv = startGrid(0);

            Map<String, Object> attributes = srv.cluster().localNode().attributes();

            List<String> locAddrs = (List) attributes.get("TcpCommunicationSpi.comm.tcp.addrs");

            IPv4Matcher matcher = new IPv4Matcher("*.*.*.*");
            for (String addr : locAddrs) {
                if (matcher.matches(InetAddress.getByName(addr))) {
                    fail("Blacklisted address found: " + addr);
                }
            }
        }
        catch (IgniteCheckedException ignored) {
            // There are no IPv4 & IPv6 network interfaces.
        }
        finally {
            IgniteUtils.resetCachedLocalAddressAndHostNames();
        }
    }

    @Test
    public void testEmptyNetworkInterfacePattern() throws Exception {
        BlacklistFilter filter = new BlacklistFilter(Arrays.asList("", "   "));

        assertTrue(filter.apply(InetAddress.getByName("127.0.0.1")));
    }

    @Test
    public void testNullNetworkInterfacePattern() throws Exception {
        BlacklistFilter filter = new BlacklistFilter(Collections.singletonList(null));

        assertTrue(filter.apply(InetAddress.getByName("127.0.0.1")));
    }
}
