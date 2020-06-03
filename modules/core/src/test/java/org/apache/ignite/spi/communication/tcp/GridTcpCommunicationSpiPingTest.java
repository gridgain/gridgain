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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.util.lang.IgniteInClosure2X;
import org.apache.ignite.internal.util.nio.GridCommunicationClient;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.nio.GridTcpNioCommunicationClient;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.communication.GridTestMessage;
import org.apache.ignite.spi.communication.tcp.internal.TcpCommunicationConfiguration;
import org.apache.ignite.testframework.GridSpiTestContext;
import org.apache.ignite.testframework.GridTestNode;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.IgniteTestResources;
import org.apache.ignite.testframework.junits.spi.GridSpiAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_MACS;

/**
 * Class for multithreaded {@link TcpCommunicationSpi} test.
 */
public class GridTcpCommunicationSpiPingTest extends GridSpiAbstractTest<TcpCommunicationSpi> {
    /** Connection idle timeout. */
    public static final int IDLE_CONN_TIMEOUT = 2000;

    /** SPI for sender. */
    private TcpCommunicationSpi spi;

    /** Spis. */
    private final List<org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi> spis = new ArrayList<>();

    /** Local node. */
    private GridTestNode locNode;

    /** Remote node. */
    private GridTestNode remoteNode;

    /** Nodes. */
    protected static final List<ClusterNode> nodes = new ArrayList<>();

    /** Constructor. */
    public GridTcpCommunicationSpiPingTest() {
        super(false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRunSender() throws Exception {
        info(">>> Starting send to remote node multithreaded test. <<<");

        IgniteInternalFuture<Boolean> ping = GridTestUtils.runAsync(() -> spi.ping(remoteNode));

        IgniteInternalFuture<Boolean> sendMessage = GridTestUtils.runAsync(() -> {
            GridTestMessage msg = new GridTestMessage(locNode.id(), 1, 0);

            msg.payload(new byte[13 * 1024]);
            spi.sendMessage(remoteNode, msg);
            return true;
        });

        U.sleep(IDLE_CONN_TIMEOUT * 2);

        assertTrue(ping.get());

        spi.alreadyPinged = true;

        assertTrue(sendMessage.get());
    }

    /**
     * @return Spi.
     */
    private TcpCommunicationSpi createSpi() {
        TcpCommunicationSpi spi = new TcpCommunicationSpi();

        try {
            spi.getConfiguration().localHost(U.resolveLocalHost("127.0.0.1"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        spi.setLocalPort(GridTestUtils.getNextCommPort(getClass()));
        spi.setIdleConnectionTimeout(IDLE_CONN_TIMEOUT);
        spi.setConnectTimeout(10000);

        return spi;
    }

    /**
     * @return Spi.
     */
    private org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi createRemoteSpi() {
        org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi spi = new org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi();

        spi.setLocalPort(GridTestUtils.getNextCommPort(getClass()));
        spi.setIdleConnectionTimeout(IDLE_CONN_TIMEOUT);
        spi.setConnectTimeout(10000);

        return spi;
    }

    /**
     * SPI that creates either client that awaits closing before sending a message or a normal client,
     * depending on whether ping command was executed.
     */
    private static class TcpCommunicationSpi extends org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi {

        /** Already pinged. */
        private volatile boolean alreadyPinged;

        /**
         * Get communiction configuration.
         */
        TcpCommunicationConfiguration getConfiguration() {
            return cfg;
        }

        /** {@inheritDoc} */
        @Override protected GridCommunicationClient createTcpClient(ClusterNode node, int connIdx) throws IgniteCheckedException {
            GridCommunicationClient client = super.createTcpClient(node, connIdx);

            if (alreadyPinged)
                return client;

            return new Client((GridTcpNioCommunicationClient) client);
        }

        /**
         * Client wrapper that awaits closing before executing sendMessage.
         */
        private static class Client implements GridCommunicationClient {

            /** Wrapped client. */
            private final GridTcpNioCommunicationClient client;

            /** Latch. */
            private final CountDownLatch latch = new CountDownLatch(1);

            /** Constructor. */
            private Client(GridTcpNioCommunicationClient client) {
                this.client = client;
            }

            /**
             * Nio session.
             */
            public GridNioSession session() {
                return client.session();
            }

            /** {@inheritDoc} */
            @Override public void doHandshake(IgniteInClosure2X<InputStream, OutputStream> handshakeC) {
                client.doHandshake(handshakeC);
            }

            /** {@inheritDoc} */
            @Override public boolean close() {
                boolean close = client.close();
                latch.countDown();
                return close;
            }

            /** {@inheritDoc} */
            @Override public void forceClose() {
                client.forceClose();
            }

            /** {@inheritDoc} */
            @Override public void sendMessage(byte[] data, int len) throws IgniteCheckedException {
                client.sendMessage(data, len);
            }

            /** {@inheritDoc} */
            @Override public void sendMessage(ByteBuffer data) throws IgniteCheckedException {
                client.sendMessage(data);
            }

            /** {@inheritDoc} */
            @Override public boolean sendMessage(@Nullable UUID nodeId, Message msg, IgniteInClosure<IgniteException> c) throws IgniteCheckedException {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                return client.sendMessage(nodeId, msg, c);
            }

            /** {@inheritDoc} */
            @Override public boolean async() {
                return client.async();
            }

            /** {@inheritDoc} */
            @Override public boolean isOutgoingConnection() {
                return client.isOutgoingConnection();
            }

            /** {@inheritDoc} */
            @Override public long getIdleTime() {
                return client.getIdleTime();
            }

            /** {@inheritDoc} */
            @Override public String toString() {
                return client.toString();
            }

            /** {@inheritDoc} */
            @Override public int connectionIndex() {
                return client.connectionIndex();
            }

            /** {@inheritDoc} */
            @Override public boolean closed() {
                return client.closed();
            }

            /** {@inheritDoc} */
            @Override public boolean reserve() {
                return client.reserve();
            }

            /** {@inheritDoc} */
            @Override public void release() {
                client.release();
            }
        }

    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        spi = createSpi();

        spis.add(spi);

        GridSpiTestContext locNodeCtx = startNode(spi, 0);

        org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi remoteNodeSpi = new org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi();
        spis.add(remoteNodeSpi);

        GridSpiTestContext remoteNodeCtx = startNode(remoteNodeSpi, 1);

        locNode = (GridTestNode) locNodeCtx.localNode();
        remoteNode = (GridTestNode) remoteNodeCtx.localNode();

        locNodeCtx.addNode(remoteNode);
        remoteNodeCtx.addNode(locNode);
    }

    /**
     * @param spi Communication Spi.
     * @param i node order.
     */
    GridSpiTestContext startNode(org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi spi, int i) throws Exception {
        GridTestUtils.setFieldValue(spi, IgniteSpiAdapter.class, "igniteInstanceName", "grid-" + i);

        IgniteTestResources rsrcs = new IgniteTestResources();

        GridTestNode node = new GridTestNode(rsrcs.getNodeId());

        node.order(i);

        GridSpiTestContext ctx = initSpiContext();

        ctx.setLocalNode(node);

        info(">>> Initialized context: nodeId=" + ctx.localNode().id());

        rsrcs.inject(spi);

        spi.spiStart(getTestIgniteInstanceName() + (i + 1));

        node.setAttributes(spi.getNodeAttributes());
        node.setAttribute(ATTR_MACS, F.concat(U.allLocalMACs(), ", "));
        node.setAttribute(U.spiAttribute(spi, IgniteNodeAttributes.ATTR_SPI_CLASS), spi.getClass().getName());

        spi.onContextInitialized(ctx);

        return ctx;
    }


    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        for (org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi communicationSpi : spis) {
            communicationSpi.onContextDestroyed();
            communicationSpi.spiStop();
        }
    }
    
}
