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

import java.net.BindException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.managers.communication.GridIoMessageFactory;
import org.apache.ignite.internal.managers.communication.IgniteMessageFactoryImpl;
import org.apache.ignite.internal.processors.timeout.GridTimeoutProcessor;
import org.apache.ignite.internal.util.nio.GridCommunicationClient;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.plugin.extensions.communication.IgniteMessageFactory;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactoryProvider;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.communication.CommunicationListener;
import org.apache.ignite.spi.communication.CommunicationSpi;
import org.apache.ignite.spi.communication.GridTestMessage;
import org.apache.ignite.testframework.GridSpiTestContext;
import org.apache.ignite.testframework.GridTestNode;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.MemorizingAppender;
import org.apache.ignite.testframework.junits.GridTestKernalContext;
import org.apache.ignite.testframework.junits.IgniteMock;
import org.apache.ignite.testframework.junits.IgniteTestResources;
import org.apache.ignite.testframework.junits.spi.GridSpiAbstractTest;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Tests that make sure that at most one physical connection is created when establishing a logical
 * connection between the same nodes and with the same connection index.
 */
@RunWith(Parameterized.class)
public class GridTcpCommunicationConnectionExclusivenessTest extends GridSpiAbstractTest<CommunicationSpi<Message>> {
    /** */
    private static final int SPI_CNT = 2;

    /**
     * Whether the order of the preceding node (A) is lower than the order of next node (B) (this influences whether
     * the incoming connection can even try to be established, or it will be rejected early).
     */
    @Parameterized.Parameter
    public boolean ascendingOrder;

    /** */
    private final Collection<IgniteTestResources> spiRsrcs = new ArrayList<>();

    /** */
    private final List<CommunicationSpi<Message>> spis = new ArrayList<>();

    /** */
    private final List<ClusterNode> nodes = new ArrayList<>();

    /** */
    private GridTimeoutProcessor timeoutProcessor;

    /** */
    private static int port = 60_000;

    /** Log4j Appender to peek at logging. */
    private final MemorizingAppender appender = new MemorizingAppender();

    /** Used to restore logging levels after tests. */
    private final Map<Logger, Level> oldLogginglevels = new HashMap<>();

    /** */
    private final AtomicInteger msgId = new AtomicInteger();

    /**
     * Disable SPI auto-start.
     */
    public GridTcpCommunicationConnectionExclusivenessTest() {
        super(false);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        appender.removeSelfFromEverywhere();

        oldLogginglevels.forEach(Logger::setLevel);

        super.afterTest();
    }

    /**
     * @return Test parameters.
     */
    @Parameterized.Parameters(name = "ascendingOrder={0}")
    public static Collection<Object[]> parameters() {
        Set<Object[]> paramsSet = new LinkedHashSet<>();

        paramsSet.add(new Object[]{false});
        paramsSet.add(new Object[]{true});

        return paramsSet;
    }

    /**
     * Tests the following scenario:
     *
     * <ul>
     *     <li>Threads T1 and T2 start to send a message from node A to node B</li>
     *     <li>One of them starts establishing a connection, another one waits on a future</li>
     *     <li>It takes some time to establish the connection (we artificially inject an invalid address,
     *     so T1/T2 can only establish a connection after connection timeout elapses)</li>
     *     <li>While the outbound connection (A to B) is being established, a message is sent from B to A,
     *     so an inbound connection is tried to be established</li>
     *     <li>This fails (as the recovery descriptor is reserved by T1 or T2), but this should not complete
     *     T1/T2 connection establishment future (as it happened due to a bug in InboundConnectionHandler)</li>
     * </ul>
     *
     * @throws Exception If something goes wrong.
     */
    @Test
    public void ensureNoDuplicatesWhenInboundCompetesWithOutbounds() throws Exception {
        prepareLogger(AsymmetricSlowlyConnectingCommunicationSpi.class);

        final int competingOutboundSenders = 2;

        // Each outbound sender sends a message, and also one inbound message is sent in the opposite direction.
        final int expectedMessages = competingOutboundSenders + 1;

        CountDownLatch allMessagesReceived = new CountDownLatch(expectedMessages);
        MessageListener messageListener = new MessageListener(allMessagesReceived);

        createSpis(messageListener, ascendingOrder);

        try {
            // Connections from A to B are slow.
            int nodeIndexA = 0;
            int nodeIndexB = 1;

            CommunicationSpi<Message> spiA = spis.get(nodeIndexA);
            CommunicationSpi<Message> spiB = spis.get(nodeIndexB);
            ClusterNode nodeA = nodes.get(nodeIndexA);
            ClusterNode nodeB = nodes.get(nodeIndexB);

            // Start a competition between threads sending from A to B.
            multithreadedAsync(
                () -> spiA.sendMessage(nodeB, new GridTestMessage(nodeA.id(), msgId.incrementAndGet(), 0)),
                competingOutboundSenders
            );

            waitTillRecoveryDescriptorIsReserved(nodeB);

            // Send a message in the opposite direction (B->A).
            spiB.sendMessage(nodeA, new GridTestMessage(nodeB.id(), msgId.incrementAndGet(), 0));

            assertTrue(allMessagesReceived.await(10, TimeUnit.SECONDS));

            assertExactlyOnePhysicalConnectionCreated(nodeB);
        }
        finally {
            stopSpis();
        }
    }

    /**
     * Installs our appender on the given logger, also remembers its logging level and changes it to DEBUG so that
     * we see even debug messages.
     *
     * @param target Target logger.
     */
    private void prepareLogger(Class<?> target) {
        appender.installSelfOn(target);

        Logger logger = Logger.getLogger(target);

        oldLogginglevels.put(logger, logger.getLevel());

        logger.setLevel(Level.DEBUG);
    }

    /**
     * @return SPI.
     */
    private CommunicationSpi<Message> createSpi() {
        TcpCommunicationSpi spi = new AsymmetricSlowlyConnectingCommunicationSpi();

        spi.setLocalAddress("127.0.0.1");
        spi.setLocalPort(port++);
        spi.setIdleConnectionTimeout(60_000);
        spi.setConnectTimeout(3000);
        spi.setMaxConnectTimeout(3000);
        spi.setSharedMemoryPort(-1);
        spi.setUsePairedConnections(false);

        return spi;
    }

    /**
     * @param lsnr        Message listener.
     * @param ascendingOrder If {@code true}, the nodes get orders from 1 to N; otherwise, it's N to 1.
     * @throws Exception If failed.
     */
    private void startSpis(MessageListener lsnr, boolean ascendingOrder) throws Exception {
        spis.clear();
        nodes.clear();
        spiRsrcs.clear();

        Map<ClusterNode, GridSpiTestContext> ctxs = new HashMap<>();

        timeoutProcessor = new GridTimeoutProcessor(new GridTestKernalContext(log));

        timeoutProcessor.start();

        timeoutProcessor.onKernalStart(true);

        for (int i = 0; i < SPI_CNT; i++) {
            CommunicationSpi<Message> spi = createSpi();

            IgniteTestResources rsrcs = new IgniteTestResources();

            GridTestNode node = new GridTestNode(rsrcs.getNodeId());

            node.setAttribute(IgniteNodeAttributes.ATTR_CLIENT_MODE, false);

            node.order(ascendingOrder ? i + 1 : SPI_CNT - i);

            GridSpiTestContext ctx = initSpiContext();

            MessageFactoryProvider testMsgFactory = new MessageFactoryProvider() {
                @Override public void registerAll(IgniteMessageFactory factory) {
                    factory.register(GridTestMessage.DIRECT_TYPE, GridTestMessage::new);
                }
            };

            ctx.messageFactory(new IgniteMessageFactoryImpl(
                    new MessageFactoryProvider[] {new GridIoMessageFactory(), testMsgFactory})
            );

            ctx.setLocalNode(node);

            ctx.timeoutProcessor(timeoutProcessor);

            info(">>> Initialized context: nodeId=" + ctx.localNode().id());

            spiRsrcs.add(rsrcs);

            rsrcs.inject(spi);

            GridTestUtils.setFieldValue(spi, IgniteSpiAdapter.class, "igniteInstanceName", "grid-" + i);

            IgniteMock ignite = GridTestUtils.getFieldValue(spi, IgniteSpiAdapter.class, "ignite");

            ignite.setCommunicationSpi(spi);

            spi.setListener(lsnr);

            nodes.add(node);

            spi.spiStart(getTestIgniteInstanceName() + (i + 1));

            node.setAttributes(spi.getNodeAttributes());

            spis.add(spi);

            spi.onContextInitialized(ctx);

            ctxs.put(node, ctx);
        }

        // For each context set remote nodes.
        for (Map.Entry<ClusterNode, GridSpiTestContext> e : ctxs.entrySet()) {
            for (ClusterNode n : nodes) {
                if (!n.equals(e.getKey()))
                    e.getValue().remoteNodes().add(n);
            }
        }
    }

    /**
     * @param lsnr        Message listener.
     * @param ascendingOrder If {@code true}, the nodes get orders from 1 to N; otherwise, it's N to 1.
     * @throws Exception If failed.
     */
    private void createSpis(MessageListener lsnr, boolean ascendingOrder) throws Exception {
        for (int i = 0; i < 3; i++) {
            try {
                startSpis(lsnr, ascendingOrder);

                break;
            }
            catch (IgniteCheckedException e) {
                if (e.hasCause(BindException.class)) {
                    if (i < 2) {
                        info("Failed to start SPIs because of BindException, will retry after delay.");

                        stopSpis();

                        U.sleep(10_000);
                    }
                    else
                        throw e;
                }
                else
                    throw e;
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    private void stopSpis() throws Exception {
        if (timeoutProcessor != null) {
            timeoutProcessor.onKernalStop(true);

            timeoutProcessor.stop(true);

            timeoutProcessor = null;
        }

        for (CommunicationSpi<Message> spi : spis) {
            spi.onContextDestroyed();

            spi.setListener(null);

            spi.spiStop();
        }

        for (IgniteTestResources rsrcs : spiRsrcs)
            rsrcs.stopThreads();
    }

    /** */
    private void waitTillRecoveryDescriptorIsReserved(ClusterNode nodeB) throws IgniteInterruptedCheckedException {
        assertTrue(waitForCondition(
            () -> appender.events().stream()
                .anyMatch(event -> {
                    String message = event.getRenderedMessage();
                    return message.startsWith("Reserved recovery descriptor for outbound connection")
                        && message.contains(nodeB.id().toString());
                }),
            3000
        ));
    }

    /** */
    private void assertExactlyOnePhysicalConnectionCreated(ClusterNode nodeB) {
        long physicalConnectionsCreated = appender.events().stream()
            .filter(event -> ("Creating NIO client to node: " + nodeB).equals(event.getRenderedMessage()))
            .count();

        assertEquals("Not exactly one physical connection was opened", 1L, physicalConnectionsCreated);
    }

    /**
     * TcpCommunicationSpi variant that, when connecting from node 0 to node 1, first tries to connect to
     * an unreachable IP address, so a connection establishment is slow in this direction. In another direction,
     * it works as usual.
     */
    private class AsymmetricSlowlyConnectingCommunicationSpi extends TcpCommunicationSpi {
        /** */
        private static final String UNREACHABLE_IP = "172.31.30.132";

        /** {@inheritDoc} */
        @Override protected GridCommunicationClient createTcpClient(ClusterNode node, int connIdx) throws IgniteCheckedException {
            if (node == nodes.get(1)) {
                injectUnreachableIpAsFirstToBeTried(node);
            }

            return super.createTcpClient(node, connIdx);
        }

        /** */
        private void injectUnreachableIpAsFirstToBeTried(ClusterNode node) {
            GridTestNode testNode = (GridTestNode) node;

            String addrsAttributeName = createAttributeName(ATTR_ADDRS);
            List<String> addrs = new ArrayList<>((Collection<String>) node.attributes().get(addrsAttributeName));
            if (!addrs.contains(UNREACHABLE_IP))
                addrs.add(0, UNREACHABLE_IP);

            testNode.setAttribute(addrsAttributeName, U.sealList(addrs));

            // Change MAC to make sure we are not in the 'same MAC' situation, so loopback addresses
            // are tried last, so #UNREACHABLE_IP goes first and does its job of slowing down.
            testNode.setAttribute(IgniteNodeAttributes.ATTR_MACS, "not-a-real-MAC");
        }

        /**
         * @param name Name.
         */
        private String createAttributeName(String name) {
            return getClass().getSimpleName() + '.' + name;
        }
    }

    /**
     *
     */
    private static class MessageListener implements CommunicationListener<Message> {
        /** */
        private final CountDownLatch latch;

        /**
         * @param latch Latch.
         */
        MessageListener(CountDownLatch latch) {
            this.latch = latch;
        }

        /** {@inheritDoc} */
        @Override public void onMessage(UUID nodeId, Message msg, IgniteRunnable msgC) {
            msgC.run();

            assertTrue(msg instanceof GridTestMessage);

            latch.countDown();
        }

        /** {@inheritDoc} */
        @Override public void onDisconnected(UUID nodeId) {
            // No-op.
        }
    }
}
