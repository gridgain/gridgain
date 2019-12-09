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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.AddressResolver;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Contains tests for TcpCommunicationSpi that require multi JVM setup.
 */
public class TcpCommunicationSpiMultiJvmTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected boolean isMultiJvm() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected List<String> additionalRemoteJvmArgs() {
        return Collections.singletonList("-Djava.net.preferIPv4Stack=true");
    }

    /**
     *  Special communication SPI that replaces node's communication address in node attributes with
     *  equal IPv6 address.
     *  This enables to emulate situation with nodes on different machines bound to the same communication port.
     */
    private static class TestCommunicationSpi extends TcpCommunicationSpi {
        /** */
        private boolean modifyAddrAttribute;

        /** */
        public TestCommunicationSpi(boolean modifyAddrAttribute) {
            this.modifyAddrAttribute = modifyAddrAttribute;
        }

        /** {@inheritDoc} */
        @Override public Map<String, Object> getNodeAttributes() throws IgniteSpiException {
            Map<String, Object> attrs = super.getNodeAttributes();

            if (!modifyAddrAttribute)
                return attrs;

            attrs.put(createSpiAttributeName(ATTR_ADDRS), Arrays.asList("0:0:0:0:0:0:0:1%lo"));

            return attrs;
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        boolean firstGrid = igniteInstanceName.endsWith("0");

        TcpCommunicationSpi commSpi = new TestCommunicationSpi(firstGrid);
        commSpi.setLocalPort(45010);

        if (firstGrid) {
            commSpi.setLocalAddress("127.0.0.1");

            InetSocketAddress extAddr = new InetSocketAddress("localhost", 45010);
            commSpi.setAddressResolver(new AddressResolver() {
                @Override public Collection<InetSocketAddress> getExternalAddresses(
                    InetSocketAddress addr) throws IgniteCheckedException {
                    return Collections.singletonList(extAddr);
                }
            });
        }
        else
            commSpi.setLocalAddress(InetAddress.getLocalHost().getHostName());

        cfg.setCommunicationSpi(commSpi);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * Verifies that node not supporting IPv6 successfully connects and communicates with other node
     * even if that node enlists IPv6 address in its address list.
     *
     * @throws Exception If any error occurs.
     */
    @Test
    public void testIPv6AddressIsSkippedOnNodeNotSupportingIPv6() throws Exception {
        Ignite g = startGrid(0);

        // This node should wait until any node "from ipFinder" appears, see log messages.
        startGrid(1);

        AtomicBoolean cacheCreatedAndLoaded = new AtomicBoolean(false);

        GridTestUtils.runAsync(() -> {
                IgniteCache<Object, Object> cache = g.getOrCreateCache(DEFAULT_CACHE_NAME);

                for (int i = 0; i < 100; i++) {
                    cache.put(i, i);
                }

                cacheCreatedAndLoaded.set(true);
            }
        , "start_cache_thread");

        assertTrue(GridTestUtils.waitForCondition(() -> cacheCreatedAndLoaded.get(), 10_000));
    }
}
