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

package org.apache.ignite.spi.discovery.tcp.ipfinder;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.IgniteTestResources;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Abstract test for ip finder.
 */
public abstract class TcpDiscoveryIpFinderAbstractSelfTest<T extends TcpDiscoveryIpFinder>
    extends GridCommonAbstractTest {
    /** */
    protected T finder;

    /** */
    protected final IgniteTestResources resources = new IgniteTestResources();

    /**
     * Constructor.
     *
     * @throws Exception If any error occurs.
     */
    protected TcpDiscoveryIpFinderAbstractSelfTest() throws Exception {
        super(false);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        finder = ipFinder();

        resources.inject(finder);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * @throws Exception If any error occurs.
     */
    @Test
    public void testIpFinder() throws Exception {
        finder.initializeLocalAddresses(Arrays.asList(new InetSocketAddress(InetAddress.getLocalHost(), 1000)));

        InetSocketAddress node1 = new InetSocketAddress(InetAddress.getLocalHost(), 1000);
        InetSocketAddress node2 = new InetSocketAddress(InetAddress.getLocalHost(), 1001);
        InetSocketAddress node3 = new InetSocketAddress(
            Inet6Address.getByName("2001:0db8:85a3:08d3:1319:47ff:fe3b:7fd3"), 1002);

        List<InetSocketAddress> initAddrs = Arrays.asList(node1, node2, node3);

        finder.registerAddresses(Collections.singletonList(node1));

        finder.registerAddresses(initAddrs);

        Collection<InetSocketAddress> addrs = finder.getRegisteredAddresses();

        for (int i = 0; i < 5 && addrs.size() != 3; i++) {
            U.sleep(1000);

            addrs = finder.getRegisteredAddresses();
        }

        assertEquals("Wrong collection size", 3, addrs.size());

        for (InetSocketAddress addr : initAddrs)
            assert addrs.contains(addr) : "Address is missing (got inconsistent addrs collection): " + addr;

        finder.unregisterAddresses(Collections.singletonList(node2));

        addrs = finder.getRegisteredAddresses();

        for (int i = 0; i < 5 && addrs.size() != 2; i++) {
            U.sleep(1000);

            addrs = finder.getRegisteredAddresses();
        }

        assertEquals("Wrong collection size", 2, addrs.size());

        finder.unregisterAddresses(finder.getRegisteredAddresses());

        finder.close();
    }

    /**
     * Creates and initializes ip finder.
     *
     * @return IP finder.
     * @throws Exception If any error occurs.
     */
    protected abstract T ipFinder() throws Exception;
}
