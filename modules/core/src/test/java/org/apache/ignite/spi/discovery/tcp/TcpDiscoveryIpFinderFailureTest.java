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

package org.apache.ignite.spi.discovery.tcp;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.events.EventType.EVT_CLIENT_NODE_DISCONNECTED;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;

/**
 * Check different scenarios in respect to {@link TcpDiscoveryIpFinder#isShared()} cfg
 * alongside possible failures for IpResolver
 *
 */
public class TcpDiscoveryIpFinderFailureTest extends GridCommonAbstractTest {

    /**  */
    private TestDynamicIpFinder dynamicIpFinder;

    /** */
    @Before
    public void initDynamicIpFinder() {
        dynamicIpFinder = new TestDynamicIpFinder();
    }

    /** */
    @After
    public void tearDown() {
        stopAllGrids();
    }

    /**
     * Tests client node disconnection with shared dynamic IP finder.
     * Client should be stuck trying to access remote nodes.
     */
    @Test
    public void testClientNodeSharedIpFinderFailure() throws Exception {
        dynamicIpFinder.setAddresses(Collections.singleton("127.0.0.1:47500"));

        IgniteConfiguration cfgSrv = getConfigurationDynamicIpFinder("Server", false);

        IgniteConfiguration cfgClient = getConfigurationDynamicIpFinder("Client", true);

        IgniteEx crd = startGrid(cfgSrv);

        IgniteEx client = startGrid(cfgClient);

        waitForTopology(2);

        dynamicIpFinder.breakService();

        Ignition.stop(crd.name(), true);

        CountDownLatch latch = new CountDownLatch(1);

        client.events().localListen(event -> {
            latch.countDown();

            return true;
        }, EVT_CLIENT_NODE_DISCONNECTED);

        assertTrue("Failed to wait for client node disconnected.", latch.await(10, SECONDS));
    }

    /**
     * Test client node disconnection with non shared dynamic IP finder.
     * Client should try several reconnection attempts and disconnect from a cluster.
     */
    @Test
    public void testClientNodeDynamicIpFinderFailure() throws Exception {
        dynamicIpFinder.setShared(false);
        dynamicIpFinder.setAddresses(Collections.singleton("127.0.0.1:47500"));

        IgniteConfiguration cfgSrv = getConfigurationDynamicIpFinder("Server", false);

        IgniteConfiguration cfgClient = getConfigurationDynamicIpFinder("Client", true);

        IgniteEx crd = startGrid(cfgSrv);

        IgniteEx client = startGrid(cfgClient);

        waitForTopology(2);

        dynamicIpFinder.breakService();

        Ignition.stop(crd.name(), true);

        CountDownLatch latch = new CountDownLatch(1);

        client.events().localListen(event -> {
            latch.countDown();

            return true;
        }, EVT_CLIENT_NODE_DISCONNECTED);

        assertFalse("Client should be stuck in ip resolution state.", latch.await(10, SECONDS));
    }

    /**
     * Tests that client node behavior is the same as for non-shared IP finder
     * if reconnectCount value is big enough.
     */
    @Test
    public void testClientNodeDynamicIpFinderFailureAndAdjsutedReconnectCount() throws Exception {
        dynamicIpFinder.setAddresses(Collections.singleton("127.0.0.1:47500"));
        IgniteConfiguration cfgSrv = getConfigurationDynamicIpFinder("Server", false);

        TcpDiscoverySpi discoverySpi = new TcpDiscoverySpi();
        discoverySpi.setReconnectCount(100);
        discoverySpi.setReconnectDelay(1000);
        discoverySpi.setIpFinder(dynamicIpFinder);

        IgniteConfiguration cfgClient = getConfigurationDynamicIpFinder("Client", true, discoverySpi);

        IgniteEx crd = startGrid(cfgSrv);

        IgniteEx client = startGrid(cfgClient);

        waitForTopology(2);

        dynamicIpFinder.breakService();

        Ignition.stop(crd.name(), true);

        CountDownLatch latch = new CountDownLatch(1);

        client.events().localListen(event -> {
            latch.countDown();

            return true;
        }, EVT_CLIENT_NODE_DISCONNECTED);

        assertFalse("Client should be stuck in ip resolution state.", latch.await(10, SECONDS));
    }

    /**
     * Test server node disconnection with dynamic IP finder.
     * Server node should catch NODE_LEFT event.
     */
    @Test
    public void testServerNodeDynamicIpFinderFailureInTheMiddle() throws Exception {
        dynamicIpFinder.setAddresses(Collections.singleton("127.0.0.1:47500"));

        IgniteConfiguration cfgSrv = getConfigurationDynamicIpFinder("Server1", false);

        IgniteConfiguration cfgSrv2 = getConfigurationDynamicIpFinder("Server2", false);

        IgniteEx crd = startGrid(cfgSrv);

        IgniteEx srv = startGrid(cfgSrv2);

        waitForTopology(2);

        dynamicIpFinder.breakService();

        CountDownLatch latch = new CountDownLatch(1);

        srv.events().localListen(event -> {
            latch.countDown();

            return true;
        }, EVT_NODE_LEFT, EVT_NODE_FAILED);

        Ignition.stop(crd.name(), true);

        assertTrue("Failed to wait for server node disconnected.", latch.await(10, SECONDS));
    }


    /**
     * Tests that dynamic IP finder doesn't allow server node to join topology
     * if IpResolver is unavailable.
     *
     * A server node should fail itself.
     */
    @Test
    public void testServerNodeBrokenDynamicIpFinderFromTheStart() throws Exception {
        dynamicIpFinder.setShared(false);

        IgniteConfiguration cfgSrv = getConfigurationDynamicIpFinder("Server1", false);

        IgniteConfiguration cfgSrv2 = getConfigurationDynamicIpFinder("Server2", false);

        dynamicIpFinder.breakService();

        final Boolean[] done = {false};

        IgniteInternalFuture<Boolean> fut = GridTestUtils.runAsync(() -> {
            try {
                IgniteEx grid1 = startGrid(cfgSrv);

                IgniteEx grid2 = startGrid(cfgSrv2);

                assertEquals(1, grid1.cluster().topologyVersion());
                assertEquals(1, grid2.cluster().topologyVersion());

                fail("Server node should not join a cluster if shared dynamic service is not working");
            }
            catch (IgniteCheckedException e) {
                if (e.getCause() != null && e.getCause() instanceof IgniteCheckedException) {
                    Throwable cause = e.getCause();
                    if (cause.getCause() != null && cause.getCause() instanceof IgniteSpiException)
                        return true;
                }
            }
            catch (Exception e) {
                return false;
            }
            finally {
                done[0] = true;
            }

            return false;
        });

        if (!GridTestUtils.waitForCondition(() -> done[0], 10_000)) {
            fut.cancel();

            fail("Node was stuck in joining topology");
        }

        Assert.assertEquals("Node was not failed", fut.get(), true);
    }

    /**
     * Test that shared IP finder blocks server node from joining topology.
     * A server node should be constantly trying to obtain IP addresses.
     */
    @Test
    public void testServerNodeBrokenStaticIpFinderFromTheStart() throws Exception {
        IgniteConfiguration cfgSrv = getConfigurationDynamicIpFinder("Server1", false);

        dynamicIpFinder.breakService();

        final Boolean[] done = {false};

        IgniteInternalFuture<Boolean> fut = GridTestUtils.runAsync(() -> {
            try {
                startGrid(cfgSrv);

                fail("Server node should not join a cluster if shared dynamic service is not working");
            }
            catch (IgniteCheckedException e) {
                if (e.getCause() != null && e.getCause() instanceof IgniteCheckedException) {
                    Throwable cause = e.getCause();
                    if (cause.getCause() != null && cause.getCause() instanceof IgniteSpiException)
                        return true;
                }
            }
            catch (Exception e) {
                return false;
            }
            finally {
                done[0] = true;
            }

            return false;
        });

        if (!GridTestUtils.waitForCondition(() -> done[0], 10_000)) {
            fut.cancel();

            Assert.assertEquals("Node was not failed", fut.get(), true);
        } else {
            String nodeState = fut.get() ? "Connected" : "Failed";

            fail("Node should be still trying to join topology. State=" + nodeState);
        }
    }

    /**
     * Tests shared IP finder with empty list allows server to start.
     */
    @Test
    public void testServerNodeStartupWithEmptySharedIpFinder() throws Exception {
        IgniteConfiguration cfgSrv = getConfigurationDynamicIpFinder("Server", false);

        dynamicIpFinder.setAddresses(null);

        IgniteEx grid = startGrid(cfgSrv);

        assertEquals(1, grid.cluster().topologyVersion());
    }

    /**
     * Tests non shared IP finder with empty list is not allowed.
     */
    @Test
    public void testServerNodeDynamicIpFinderWithEmptyAddresses() throws Exception {
        dynamicIpFinder.setShared(false);
        IgniteConfiguration cfgSrv = getConfigurationDynamicIpFinder("Server1", false);

        dynamicIpFinder.setAddresses(null);

        boolean isSpiExThrown = false;

        try {
            startGrid(cfgSrv);

            fail("Server node must fail if non-shared ip finder returns empty list");
        }
        catch (IgniteCheckedException e) {
            if (e.getCause() != null && e.getCause() instanceof IgniteCheckedException) {
                Throwable cause = e.getCause();
                if (cause.getCause() != null && cause.getCause() instanceof IgniteSpiException)
                    isSpiExThrown = true;
            }
        }

        assertTrue("Server node must fail if nonshared dynamic service returns empty list", isSpiExThrown);
    }

    /**
     * Gets node configuration with dynamic IP finder
     */
    private IgniteConfiguration getConfigurationDynamicIpFinder(String instanceName, boolean clientMode) throws Exception {
        TcpDiscoverySpi discoverySpi = new TcpDiscoverySpi();
        discoverySpi.setReconnectCount(2);
        discoverySpi.setReconnectDelay(1000);
        discoverySpi.setIpFinder(dynamicIpFinder);

        return getConfigurationDynamicIpFinder(instanceName, clientMode, discoverySpi);
    }

    /**
     * Gets node configuration with dynamic IP finder
     */
    private IgniteConfiguration getConfigurationDynamicIpFinder(String instanceName, boolean clientMode, TcpDiscoverySpi discoverySpi) throws Exception {
        IgniteConfiguration cfg = getConfiguration();
        cfg.setNodeId(null);

        cfg.setDiscoverySpi(discoverySpi);

        cfg.setGridLogger(log);

        cfg.setIgniteInstanceName(instanceName);
        cfg.setClientMode(clientMode);

        return cfg;
    }
}
