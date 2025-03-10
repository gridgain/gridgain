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

package org.apache.ignite.spi.discovery.tcp.ipfinder.cloud;

import com.google.common.collect.ImmutableList;
import java.net.InetSocketAddress;
import java.util.Collection;

import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinderAbstractSelfTest;
import org.apache.ignite.testsuites.IgniteCloudTestSuite;
import org.junit.Ignore;
import org.junit.Test;

/**
 * TcpDiscoveryCloudIpFinder test.
 */
public class TcpDiscoveryCloudIpFinderSelfTest extends
    TcpDiscoveryIpFinderAbstractSelfTest<TcpDiscoveryCloudIpFinder> {
    /**
     * Constructor.
     *
     * @throws Exception If any error occurs.
     */
    public TcpDiscoveryCloudIpFinderSelfTest() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected TcpDiscoveryCloudIpFinder ipFinder() throws Exception {
        // No-op.
        return null;
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testIpFinder() throws Exception {
        // No-op
    }

    /**
     * Tests GCE.
     *
     * @throws Exception If any error occurs.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-1585")
    @Test
    public void testGoogleComputeEngine() throws Exception {
        testCloudProvider("google-compute-engine");
    }

    /**
     * Tests Rackspace.
     *
     * @throws Exception If any error occurs.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-9444")
    @Test
    public void testRackspace() throws Exception {
        testCloudProvider("rackspace-cloudservers-us");
    }

    /**
     * Tests a given provider.
     *
     * @param provider Provider name.
     * @throws Exception If any error occurs.
     */
    private void testCloudProvider(String provider) throws Exception {
        info("Testing provider: " + provider);

        TcpDiscoveryCloudIpFinder ipFinder = new TcpDiscoveryCloudIpFinder();

        resources.inject(ipFinder);

        ipFinder.setProvider(provider);
        ipFinder.setIdentity(IgniteCloudTestSuite.getAccessKey(provider));
        ipFinder.setRegions(IgniteCloudTestSuite.getRegions(provider));
        ipFinder.setZones(IgniteCloudTestSuite.getZones(provider));

        if (provider.equals("google-compute-engine"))
            ipFinder.setCredentialPath(IgniteCloudTestSuite.getSecretKey(provider));
        else
            ipFinder.setCredential(IgniteCloudTestSuite.getSecretKey(provider));

        Collection<InetSocketAddress> addresses = ipFinder.getRegisteredAddresses();

        for (InetSocketAddress addr : addresses)
            info("Registered instance: " + addr.getAddress().getHostAddress() + ":" + addr.getPort());

        ipFinder.unregisterAddresses(addresses);

        assert addresses.size() == ipFinder.getRegisteredAddresses().size();

        ipFinder.registerAddresses(ImmutableList.of(
            new InetSocketAddress("192.168.0.1", TcpDiscoverySpi.DFLT_PORT)));

        assert addresses.size() == ipFinder.getRegisteredAddresses().size();
    }
}
