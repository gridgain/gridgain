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

import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.UUIDCollectionMessage;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.MemorizingAppender;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.log4j.Level;
import org.apache.log4j.spi.LoggingEvent;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static java.util.Collections.singletonList;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * Tests logging in {@link TcpCommunicationSpi} in relation to 'node left' event.
 */
public class TcpCommunicationSpiNodeLeftLoggingTest extends GridCommonAbstractTest {
    /***/
    private static final String SERVER1_NAME = "server1";

    /***/
    private static final String CLIENT_NAME = "client";

    /***/
    private final MemorizingAppender log4jAppender = new MemorizingAppender();

    /***/
    private volatile TcpCommunicationSpi server1CommunicationSpi;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpCommunicationSpi spi = new TcpCommunicationSpi();

        if (CLIENT_NAME.equals(gridName)) {
            cfg.setClientMode(true);
        } else if (SERVER1_NAME.equals(gridName)) {
            server1CommunicationSpi = spi;
        }

        cfg.setCommunicationSpi(spi);

        return cfg;
    }

    /***/
    @Before
    public void installAppender() {
        log4jAppender.installSelfOn(TcpCommunicationSpi.class);
    }

    /***/
    @After
    public void removeAppender() {
        log4jAppender.removeSelfFrom(TcpCommunicationSpi.class);
    }

    @After
    public void stopAll() {
        stopAllGrids();
    }

    /***/
    @Test
    public void logsWithErrorWhenCantSendMessageToServerWhichLeft() throws Exception {
        IgniteEx server1 = startGrid(SERVER1_NAME);
        IgniteEx server2 = startGrid("server2");
        ClusterNode server2Node = server2.localNode();

        server1.cluster().state(ACTIVE);

        server2.close();

        sendFailingMessageTo(server2Node);

        LoggingEvent event = log4jAppender.singleEventSatisfying(
            evt -> evt.getRenderedMessage().startsWith("Failed to send message to remote node")
        );
        assertThat(event.getLevel(), is(Level.ERROR));
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
    @NotNull
    private UUIDCollectionMessage someMessage() {
        return new UUIDCollectionMessage(singletonList(UUID.randomUUID()));
    }

    /***/
    @Test
    public void logsWithInfoWhenCantSendMessageToClientWhichLeft() throws Exception {
        IgniteEx server = startGrid(SERVER1_NAME);
        IgniteEx client = startGrid(CLIENT_NAME);
        ClusterNode clientNode = client.localNode();

        server.cluster().state(ACTIVE);

        client.close();

        sendFailingMessageTo(clientNode);

        LoggingEvent event = log4jAppender.singleEventSatisfying(
            evt -> evt.getRenderedMessage().startsWith("Failed to send message to remote node")
        );
        assertThat(event.getLevel(), is(Level.INFO));
    }
}
