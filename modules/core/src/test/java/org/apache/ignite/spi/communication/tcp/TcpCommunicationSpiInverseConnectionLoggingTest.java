/*
 * Copyright 2022 GridGain Systems, Inc. and Contributors.
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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.util.UUIDCollectionMessage;
import org.apache.ignite.internal.util.nio.GridCommunicationClient;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.internal.TcpInverseConnectionResponseMessage;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.MemorizingAppender;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.log4j.Level;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * Tests logging in {@link TcpCommunicationSpi} when inverse connection to a client is tried to be established.
 */
public class TcpCommunicationSpiInverseConnectionLoggingTest extends GridCommonAbstractTest {
    /***/
    private static final String SERVER_NAME = "server";

    /***/
    private static final String CLIENT_NAME = "client";

    /** */
    private static final String UNREACHABLE_IP = "172.31.30.132";

    /***/
    private final MemorizingAppender log4jAppender = new MemorizingAppender();

    /***/
    private volatile TcpCommunicationSpi server1CommunicationSpi;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setFailureDetectionTimeout(2_000);

        TcpCommunicationSpi spi = new TestCommunicationSpi();

        spi.setForceClientToServerConnections(true);

        if (CLIENT_NAME.equals(gridName))
            cfg.setClientMode(true);
        else if (SERVER_NAME.equals(gridName))
            server1CommunicationSpi = spi;

        cfg.setCommunicationSpi(spi);

        return cfg;
    }

    /***/
    @Before
    public void installAppender() {
        log4jAppender.installSelfOn(TestCommunicationSpi.class);
    }

    /***/
    @After
    public void removeAppender() {
        log4jAppender.removeSelfFrom(TestCommunicationSpi.class);
    }

    /***/
    @After
    public void stopAll() {
        stopAllGrids();
    }

    /***/
    @Test
    public void logsInfoForExceptionMeaningSwitchToInverseConnection() throws Exception {
        IgniteEx server = startGrid(SERVER_NAME);
        IgniteEx client = startGrid(CLIENT_NAME);

        ClusterNode clientNode = client.localNode();

        interruptCommWorkerThreads(client.name());

        TcpCommunicationSpi spi = (TcpCommunicationSpi)server.configuration().getCommunicationSpi();

        GridTestUtils.invoke(spi, "onNodeLeft", clientNode.consistentId(), clientNode.id());

        sendFailingMessageTo(clientNode);

        LoggingEvent event = log4jAppender.singleEventSatisfying(evt -> evt.getRenderedMessage().startsWith("Failed to send message to remote node "));
        assertThat(event.getLevel(), is(Level.INFO));
    }

    /**
     * We need to interrupt communication worker client nodes so that
     * closed connection won't automatically reopen when we don't expect it.
     *
     * @param clientName The name of the client whose threads we want to interrupt.
     */
    private void interruptCommWorkerThreads(String clientName) {
        CommWorkerThreadUtils.interruptCommWorkerThreads(clientName, log);
    }

    /***/
    private void sendFailingMessageTo(ClusterNode clientNode) {
        GridTestUtils.assertThrows(
            log,
            () -> server1CommunicationSpi.sendMessage(clientNode, someMessage()),
            Exception.class,
            null
        );
    }

    /***/
    private UUIDCollectionMessage someMessage() {
        return new UUIDCollectionMessage(singletonList(UUID.randomUUID()));
    }

    /**
     * A custom {@link TcpCommunicationSpi} that allows to model the situation when an inverse connection must be
     * established, but it cannot be.
     */
    private static class TestCommunicationSpi extends TcpCommunicationSpi {
        /** {@inheritDoc} */
        @Override protected GridCommunicationClient createTcpClient(ClusterNode node, int connIdx) throws IgniteCheckedException {
            if (node.isClient()) {
                Map<String, Object> attrs = new HashMap<>(node.attributes());

                attrs.put(createAttributeName(ATTR_ADDRS), Collections.singleton(UNREACHABLE_IP));
                attrs.put(createAttributeName(ATTR_PORT), 47200);
                attrs.put(createAttributeName(ATTR_EXT_ADDRS), Collections.emptyList());
                attrs.put(createAttributeName(ATTR_HOST_NAMES), Collections.emptyList());

                ((TcpDiscoveryNode)(node)).setAttributes(attrs);
            }

            return super.createTcpClient(node, connIdx);
        }

        /**
         * Creates an attribute name by prepending it with the class name (and a dot).
         *
         * @param name Name.
         */
        private String createAttributeName(String name) {
            return getClass().getSimpleName() + '.' + name;
        }

        /** {@inheritDoc} */
        @Override public void sendMessage(
            ClusterNode node,
            Message msg,
            IgniteInClosure<IgniteException> ackC
        ) throws IgniteSpiException {
            if (msg instanceof GridIoMessage) {
                GridIoMessage msg0 = (GridIoMessage)msg;

                if (msg0.message() instanceof TcpInverseConnectionResponseMessage) {
                    if (log.isInfoEnabled())
                        log.info("Client skips inverse connection response to server: " + node);

                    return;
                }
            }

            super.sendMessage(node, msg, ackC);
        }
    }
}
