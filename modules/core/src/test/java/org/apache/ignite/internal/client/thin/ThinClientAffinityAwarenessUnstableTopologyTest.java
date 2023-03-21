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


package org.apache.ignite.internal.client.thin;

import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientConnectionException;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.SslMode;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.odbc.ClientListenerProcessor;
import org.apache.ignite.internal.util.nio.GridNioServer;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.mxbean.ClientProcessorMXBean;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;

import static org.apache.ignite.testframework.GridTestUtils.getFieldValue;
import static org.apache.ignite.testframework.GridTestUtils.setFieldValue;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Test affinity awareness of thin client on unstable topology.
 */
@RunWith(Parameterized.class)
public class ThinClientAffinityAwarenessUnstableTopologyTest extends ThinClientAbstractAffinityAwarenessTest {
    /** */
    @Parameterized.Parameter
    public boolean sslEnabled;

    /** @return Test parameters. */
    @Parameterized.Parameters(name = "sslEnabled={0}")
    public static Collection<?> parameters() {
        return Arrays.asList(new Object[][] {{false}, {true}});
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (sslEnabled) {
            cfg.setClientConnectorConfiguration(new ClientConnectorConfiguration()
                .setSslEnabled(true)
                .setSslClientAuth(true)
                .setUseIgniteSslContextFactory(false)
                .setSslContextFactory(GridTestUtils.sslFactory()));
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected ClientConfiguration getClientConfiguration(int... nodeIdxs) {
        ClientConfiguration cfg = super.getClientConfiguration(nodeIdxs);

        if (sslEnabled) {
            cfg.setSslMode(SslMode.REQUIRED)
                .setSslContextFactory(GridTestUtils.sslFactory());
        }

        return cfg;
    }

    /**
     * Test that join of the new node is detected by the client and affects affinity awareness.
     */
    @Test
    public void testAffinityAwarenessOnNodeJoin() throws Exception {
        startGrids(3);

        awaitPartitionMapExchange();

        initClient(getClientConfiguration(1, 2, 3), 1, 2);

        // Test affinity awareness before node join.
        testAffinityAwareness(true);

        assertNull(channels[3]);

        startGrid(3);

        awaitPartitionMapExchange();

        // Send non-affinity request to detect topology change.
        ClientCache<Object, Object> cache = client.getOrCreateCache(PART_CACHE_NAME);

        awaitChannelsInit(3);

        assertOpOnChannel(null, ClientOperation.CACHE_GET_OR_CREATE_WITH_NAME);

        Integer key = primaryKey(grid(3).cache(PART_CACHE_NAME));

        assertNotNull("Not found key for node 3", key);

        cache.put(key, 0);

        assertOpOnChannel(null, ClientOperation.CACHE_PARTITIONS);

        assertOpOnChannel(channels[3], ClientOperation.CACHE_PUT);

        // Test affinity awareness after node join.
        testAffinityAwareness(false);
    }

    /**
     * Test that node left event affects affinity awareness.
     */
    @Test
    public void testAffinityAwarenessOnNodeLeft() throws Exception {
        startGrids(4);

        awaitPartitionMapExchange();

        initClient(getClientConfiguration(1, 2, 3), 1, 2, 3);

        // Test affinity awareness before node left.
        testAffinityAwareness(true);

        stopGrid(3);

        channels[3] = null;

        awaitPartitionMapExchange();

        // Detect topology change.
        detectTopologyChange();

        // Test affinity awareness after node join.
        testAffinityAwareness(true);
    }

    /**
     * Test connection restore to affinity nodes.
     */
    @Test
    public void testConnectionLoss() throws Exception {
        startGrids(2);

        awaitPartitionMapExchange();

        initClient(getClientConfiguration(0, 1), 0, 1);

        // Test affinity awareness before connection to node lost.
        testAffinityAwareness(true);

        // Choose node to disconnect.
        int disconnectNodeIdx = 0;

        // Drop all thin connections from the node.
        ObjectName mbeanName = U.makeMBeanName(grid(disconnectNodeIdx).name(), "Clients",
            ClientListenerProcessor.class.getSimpleName());

        MBeanServerInvocationHandler.newProxyInstance(ManagementFactory.getPlatformMBeanServer(), mbeanName,
            ClientProcessorMXBean.class, true).dropAllConnections();

        channels[disconnectNodeIdx] = null;

        // Send request to disconnected node.
        ClientCache<Object, Object> cache = client.cache(PART_CACHE_NAME);

        Integer key = primaryKey(grid(disconnectNodeIdx).cache(PART_CACHE_NAME));

        assertNotNull("Not found key for node " + disconnectNodeIdx, key);

        cache.put(key, 0);

        // Request goes to the connected channel, since affinity node is disconnected.
        assertOpOnChannel(channels[1], ClientOperation.CACHE_PUT);

        cache.put(key, 0);

        // Connection to disconnected node should be restored after retry.
        assertOpOnChannel(channels[disconnectNodeIdx], ClientOperation.CACHE_PUT);

        // Test affinity awareness.
        testAffinityAwareness(false);
    }

    /**
     * Test that partition awareness works when reconnecting to the new cluster (with lower topology version)
     */
    @Test
    public void testPartitionAwarenessOnClusterRestart() throws Exception {
        startGrids(3);

        awaitPartitionMapExchange();

        initClient(getClientConfiguration(0, 1, 2), 0, 1, 2);

        // Test affinity awareness before cluster restart.
        testAffinityAwareness(true);

        stopAllGrids();

        Arrays.fill(channels, null);

        // Start 2 grids, so topology version of the new cluster will be less then old cluster.
        startGrids(2);

        awaitPartitionMapExchange();

        // Send any request to failover.
        client.cache(REPL_CACHE_NAME).put(0, 0);

        detectTopologyChange();

        awaitChannelsInit(0, 1);

        testAffinityAwareness(true);
    }

    /**
     * Checks that each request goes to right node.
     *
     * @param partReq Next operation should request partitions map.
     */
    private void testAffinityAwareness(boolean partReq) {
        ClientCache<Object, Object> clientCache = client.cache(PART_CACHE_NAME);
        IgniteInternalCache<Object, Object> igniteCache = grid(0).context().cache().cache(PART_CACHE_NAME);

        for (int i = 0; i < KEY_CNT; i++) {
            TestTcpClientChannel opCh = affinityChannel(i, igniteCache);

            clientCache.put(i, i);

            if (partReq) {
                assertOpOnChannel(null, ClientOperation.CACHE_PARTITIONS);

                partReq = false;
            }

            assertOpOnChannel(opCh, ClientOperation.CACHE_PUT);
        }
    }

    /** */
    @Test
    public void testSessionCloseBeforeHandshake() throws Exception {
        startGrid(0);

        // TODO: We don't have events.
        ClientConfiguration cliCfg = getClientConfiguration(0);
//            .setEventListeners(new ConnectionEventListener() {
//                @Override public void onHandshakeStart(HandshakeStartEvent event) {
//                    // Close connection.
//                    stopAllGrids();
//                }
//            });

        GridTestUtils.assertThrowsWithCause(() -> {
            try (IgniteClient client = Ignition.startClient(cliCfg)) {
                return client;
            }
        }, ClientConnectionException.class);
    }

    /** */
    @Test
    public void testCreateSessionAfterClose() throws Exception {
        startGrids(2);

        CountDownLatch srvStopped = new CountDownLatch(1);

        AtomicBoolean dfltInited = new AtomicBoolean();

        // The client should close pending requests on closing without waiting.
        try (TcpIgniteClient client = new TcpIgniteClient((cfg, connMgr) -> {
            // Skip default channel to successful client start.
            if (!dfltInited.compareAndSet(false, true)) {
                try {
                    // Connection manager should be stopped before opening a new connection.
                    srvStopped.await(getTestTimeout(), TimeUnit.MILLISECONDS);
                }
                catch (InterruptedException ignored) {
                    // No-op.
                }
            }

            return new TcpClientChannel(cfg, connMgr);
        }, getClientConfiguration(0))) {
            GridNioServer<ByteBuffer> srv = getFieldValue(client.reliableChannel(), "connMgr", "srv");

            // Make sure handshake data will not be recieved.
            setFieldValue(srv, "skipRead", true);

            GridTestUtils.runAsync(() -> {
                assertTrue(waitForCondition(() -> getFieldValue(srv, "closed"), getTestTimeout()));

                srvStopped.countDown();
            });
        }
    }
}
