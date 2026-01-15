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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.nio.GridCommunicationClient;
import org.apache.ignite.internal.util.nio.GridNioFilter;
import org.apache.ignite.internal.util.nio.GridNioFilterAdapter;
import org.apache.ignite.internal.util.nio.GridNioFilterChain;
import org.apache.ignite.internal.util.nio.GridNioFuture;
import org.apache.ignite.internal.util.nio.GridNioRecoveryDescriptor;
import org.apache.ignite.internal.util.nio.GridNioServerListener;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.nio.GridNioSessionImpl;
import org.apache.ignite.internal.util.nio.GridTcpNioCommunicationClient;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.communication.tcp.messages.ConnectionCheckMessage;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.ignite.testframework.GridTestUtils.assertTimeout;
import static org.apache.ignite.testframework.GridTestUtils.getFieldValue;
import static org.apache.ignite.testframework.GridTestUtils.invoke;

/**
 * Test class containing a test for the following scenario:
 *
 * <ol>
 *     <li>We have a cluster with two nodes: a server and a client;</li>
 *     <li>We then emulate a situation when it is possible to send messages from the client to the server but
 *     <b>not</b> in the opposite direction;</li>
 *     <li>We then force the client to abandon the existing connection, which makes it to try to open a new one;</li>
 *     <li>The client sends a series of handshakes because messages from the server do not reach the client;</li>
 *     <li>We unblock the server-to-client connection and expect normal operations afterward. Previously there
 *     has been a bug that resulted in the selector threads on the server side to be deadlocked.</li>
 * </ol>
 */
// TODO: consider removing this code, https://ggsystems.atlassian.net/browse/GG-46827.
public class GridTcpCommunicationSpiWriteSemaphoreDeadlockTest extends GridCommonAbstractTest {
    private static final int SERVER_SELECTORS_COUNT = 4;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpCommunicationSpi communicationSpi = (TcpCommunicationSpi) cfg.getCommunicationSpi();

        boolean isServerNode = igniteInstanceName.equals(getTestIgniteInstanceName(0));

        if (isServerNode) {
            // Set a small outgoing message queue size on the server. This will make the server threads more likely to
            // block when sending messages to the client.
            communicationSpi.setMessageQueueLimit(1);
            communicationSpi.setSelectorsCount(SERVER_SELECTORS_COUNT);
        } else {
            // Set a small connection timeout to increase the number of handshake messages sent by the client.
            communicationSpi.setConnectTimeout(1);
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids(true);

        super.afterTest();
    }

    @Test
    public void testDeadlockOnMultipleReconnectionAttempts() throws Exception {
        IgniteEx server = startGrid(0);

        IgniteEx client = startClientGrid(1);

        IgniteCache<Integer, String> cache = client.getOrCreateCache("TEST");

        cache.put(1, "1");

        // Block sending messages from the server to the client.
        GridNioSession serverSession = getSession(server, client);

        insertBlockingFilter((GridNioSessionImpl) serverSession);

        // Force the client to abandon the existing connection and issue a bunch of handshakes to establish a new one.
        breakConnection(client, server);

        assertTimeout(10, TimeUnit.SECONDS, () -> assertEquals("1", cache.get(1)));
    }

    /**
     * Forces one node to abandon connection to the other node without any notifications on the receiving side.
     *
     * <p>This approach is borrowed from
     * {@link GridTcpCommunicationSpiLogTest#testClientHalfOpenedConnectionDebugLogMessage}.
     */
    private void breakConnection(IgniteEx from, Ignite to) {
        TcpCommunicationSpi fromSpi = communicationSpi(from);

        Map<?, GridNioRecoveryDescriptor> recoveryDescs = getFieldValue(fromSpi, "nioSrvWrapper", "recoveryDescs");
        Map<?, GridNioRecoveryDescriptor> outRecDescs = getFieldValue(fromSpi, "nioSrvWrapper", "outRecDescs");
        Map<?, GridNioRecoveryDescriptor> inRecDescs = getFieldValue(fromSpi, "nioSrvWrapper", "inRecDescs");

        @SuppressWarnings("unchecked")
        Iterator<GridNioRecoveryDescriptor> it = F.concat(
                recoveryDescs.values().iterator(),
                outRecDescs.values().iterator(),
                inRecDescs.values().iterator()
        );

        while (it.hasNext()) {
            GridNioRecoveryDescriptor desc = it.next();

            desc.release();
        }

        GridNioServerListener<Message> lsnr = U.field(fromSpi, "srvLsnr");

        lsnr.onDisconnected(getAndRemoveSession(from, to), new IOException("Test exception"));
    }

    private GridNioSession getAndRemoveSession(IgniteEx from, Ignite to) {
        return getSession(from, to, true);
    }

    private GridNioSession getSession(IgniteEx from, Ignite to) {
        return getSession(from, to, false);
    }

    private GridNioSession getSession(IgniteEx from, Ignite to, boolean remove) {
        UUID toNodeId = to.cluster().localNode().id();

        TcpCommunicationSpi fromSpi = communicationSpi(from);

        Map<UUID, GridCommunicationClient[]> clientsMap = getFieldValue(fromSpi, "clientPool", "clients");

        GridTcpNioCommunicationClient client = null;

        while (client == null) {
            GridCommunicationClient[] clients = remove ? clientsMap.remove(toNodeId) : clientsMap.get(toNodeId);

            if (clients == null || clients.length == 0) {
                continue;
            }

            assertEquals(1, clients.length);

            client = (GridTcpNioCommunicationClient) clients[0];
        }

        return client.session();
    }

    private TcpCommunicationSpi communicationSpi(IgniteEx ignite) {
        return (TcpCommunicationSpi) ignite.configuration().getCommunicationSpi();
    }

    /**
     * A special filter that blocks writing of ConnectionCheckMessage.
     */
    private static class BlockingNioFilter extends GridNioFilterAdapter {
        // Barrier needed to block all selector threads on a node.
        private final CyclicBarrier barrier = new CyclicBarrier(SERVER_SELECTORS_COUNT);

        BlockingNioFilter() {
            super("test-blocking");
        }

        @Override public void onSessionOpened(GridNioSession ses) throws IgniteCheckedException {
            proceedSessionOpened(ses);
        }

        @Override public void onSessionClosed(GridNioSession ses) throws IgniteCheckedException {
            proceedSessionClosed(ses);
        }

        @Override public void onExceptionCaught(GridNioSession ses, IgniteCheckedException ex) throws IgniteCheckedException {
            proceedExceptionCaught(ses, ex);
        }

        @Override public GridNioFuture<?> onSessionWrite(GridNioSession ses, Object msg, boolean fut, IgniteInClosure<IgniteException> ackC) throws IgniteCheckedException {
            if (msg instanceof ConnectionCheckMessage) {
                try {
                    // Block the current selector thread.
                    barrier.await(10, TimeUnit.SECONDS);

                    // Use an arbitrary sleep to increase the number of incoming handshake messages, which in turn
                    // increases the possibility of the deadlock.
                    Thread.sleep(1000);
                } catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
                    throw new RuntimeException(e);
                }
            }

            return proceedSessionWrite(ses, msg, fut, ackC);
        }

        @Override public void onMessageReceived(GridNioSession ses, Object msg) throws IgniteCheckedException {
            proceedMessageReceived(ses, msg);
        }

        @Override public GridNioFuture<Boolean> onSessionClose(GridNioSession ses) throws IgniteCheckedException {
            return proceedSessionClose(ses);
        }

        @Override public void onSessionIdleTimeout(GridNioSession ses) throws IgniteCheckedException {
            proceedSessionIdleTimeout(ses);
        }

        @Override public void onSessionWriteTimeout(GridNioSession ses) throws IgniteCheckedException {
            proceedSessionWriteTimeout(ses);
        }
    }

    /**
     * Inserts the blocking filter into the given session's filter chain.
     */
    private void insertBlockingFilter(GridNioSessionImpl session) throws Exception {
        GridNioFilterChain<?> filterChain = invoke(session, "chain");

        BlockingNioFilter f = new BlockingNioFilter();

        GridNioFilter tail = getFieldValue(filterChain, "tail");

        GridNioFilter oldNext = tail.nextFilter();

        tail.nextFilter(f);

        oldNext.previousFilter(f);

        f.previousFilter(tail);
        f.nextFilter(oldNext);
    }
}
