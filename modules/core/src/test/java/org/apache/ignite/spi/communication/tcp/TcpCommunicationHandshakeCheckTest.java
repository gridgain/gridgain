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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.IgniteSpi;
import org.apache.ignite.spi.communication.tcp.messages.HandshakeMessage;
import org.apache.ignite.spi.communication.tcp.messages.HandshakeMessage2;
import org.apache.ignite.spi.communication.tcp.messages.NodeIdMessage;
import org.apache.ignite.spi.communication.tcp.messages.RecoveryLastReceivedMessage;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_CHECK_COMMUNICATION_HANDSHAKE_MESSAGE_SENDER;
import static org.apache.ignite.plugin.extensions.communication.Message.DIRECT_TYPE_SIZE;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.ATTR_ADDRS;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.ATTR_HOST_NAMES;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.ATTR_PORT;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.HANDSHAKE_WAIT_MSG_TYPE;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.makeMessageType;
import static org.apache.ignite.spi.communication.tcp.messages.RecoveryLastReceivedMessage.UNKNOWN_NODE;

/**
 * Tests handshake procedure from security point of view.
 */
@WithSystemProperty(key = IGNITE_CHECK_COMMUNICATION_HANDSHAKE_MESSAGE_SENDER, value = "true")
public class TcpCommunicationHandshakeCheckTest extends GridCommonAbstractTest {
    /**
     * Tests that the replacement of node id in handshake message leads to failure of handshake procedure.
     * @throws Exception
     */
    @Test
    public void testReplacementNodeId() throws Exception {
        IgniteEx crd = startGrids(2);

        IgniteSpi commSpi = crd.context().config().getCommunicationSpi();

        Collection<String> rmtAddrs = crd.localNode().attribute(U.spiAttribute(commSpi, ATTR_ADDRS));
        Collection<String> rmtHostNames = crd.localNode().attribute(U.spiAttribute(commSpi, ATTR_HOST_NAMES));
        Integer rmtBoundPort = crd.localNode().attribute(U.spiAttribute(commSpi, ATTR_PORT));

        // Try to connect first on bound addresses.
        List<InetSocketAddress> addrs = new ArrayList<>(U.toSocketAddresses(rmtAddrs, rmtHostNames, rmtBoundPort, true));

        addrs.sort(U.inetAddressesComparator(true));

        // Server nodes are started on localhost (127.0.0.1), need to try another host in order to emulate the case.
        String justAnotherLocalhost = "127.0.0.3";

        // Step 1. Get remote node response with the remote nodeId value.
        InetAddress addr = InetAddress.getByName("127.0.0.3");
        InetSocketAddress inetSockAddr = new InetSocketAddress(addr, getAvailablePort());

        try (SocketChannel ch = SocketChannel.open()) {
            ch.configureBlocking(true);
            ch.socket().setTcpNoDelay(true);
            ch.socket().setKeepAlive(true);
            ch.socket().bind(inetSockAddr);
            ch.socket().connect(addrs.iterator().next(), (int)getTestTimeout());

            ByteBuffer buf = ByteBuffer.allocate(NodeIdMessage.MESSAGE_FULL_SIZE);

            for (int i = 0; i < NodeIdMessage.MESSAGE_FULL_SIZE; ) {
                int read = ch.read(buf);

                assertTrue("Failed to read remote node ID.", read != -1);

                if (read >= DIRECT_TYPE_SIZE) {
                    short msgType = makeMessageType(buf.get(0), buf.get(1));

                    assertFalse("Unexpected response [NEED_WAI]", msgType == HANDSHAKE_WAIT_MSG_TYPE);
                }

                i += read;
            }

            U.writeFully(ch, ByteBuffer.wrap(U.IGNITE_HEADER));

            // Try to send already registered node id.
            HandshakeMessage msg = new HandshakeMessage2(grid(1).localNode().id(), 1, 0, 0);

            // Step 2. Prepare Handshake message to send to the remote node.
            buf = ByteBuffer.allocate(msg.getMessageSize());

            buf.order(ByteOrder.LITTLE_ENDIAN);

            boolean written = msg.writeTo(buf, null);

            assert written;

            U.flip(buf);

            U.writeFully(ch, buf);

            // Step 3. Waiting for response from the remote node with their receive count message.
            buf = ByteBuffer.allocate(RecoveryLastReceivedMessage.MESSAGE_FULL_SIZE);

            buf.order(ByteOrder.LITTLE_ENDIAN);

            for (int i = 0; i < RecoveryLastReceivedMessage.MESSAGE_FULL_SIZE; ) {
                int read = ch.read(buf);

                assertTrue("Failed to read remote node recovery handshake", read != -1);

                i += read;
            }

            long rcvCnt = buf.getLong(DIRECT_TYPE_SIZE);

            assertTrue("Connection is not rejected [response=" + rcvCnt + ']', rcvCnt == UNKNOWN_NODE);
        }
    }

    /**
     * Return first available port.
     *
     * @return Available port.
     * @throws IOException If failed.
     */
    private static int getAvailablePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }
}
