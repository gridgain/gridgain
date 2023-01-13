/*
 * Copyright 2023 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.spi.communication.tcp.internal;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.Collections;
import java.util.UUID;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.nio.GridNioServer;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.communication.tcp.internal.TcpCommunicationConnectionCheckFuture.SingleAddressConnectFuture;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * {@link TcpCommunicationConnectionCheckFutureSelfTest} tests.
 */
public class TcpCommunicationConnectionCheckFutureSelfTest {

    private final GridNioServer<?> server = mock(GridNioServer.class);

    @Test
    public void singleAddressConnectBeforeCancel() throws Exception {
        TcpCommunicationConnectionCheckFuture fut = createCheckFuture();

        SocketChannel mockChannel = createChannel();

        SingleAddressConnectFuture single = createSingleCheckFuture(fut, mockChannel);

        // Should just open normally.
        single.init(mock(InetSocketAddress.class), "test", UUID.randomUUID());

        verify(server, never()).cancelConnect(eq(mockChannel), any());

        // Should close connection normally.
        single.cancel();

        verify(server, times(1)).cancelConnect(eq(mockChannel), any());
    }

    @Test
    public void singleAddressConnectAfterCancel() throws Exception {
        TcpCommunicationConnectionCheckFuture fut = createCheckFuture();

        SocketChannel mockChannel = createChannel();

        SingleAddressConnectFuture single = createSingleCheckFuture(fut, mockChannel);

        // Should install a flag, based on which channel will be closed right after it's open.
        single.cancel();

        verify(server, never()).cancelConnect(eq(mockChannel), any());

        // Should be closed after this.
        single.init(mock(InetSocketAddress.class), "test", UUID.randomUUID());

        verify(server, times(1)).cancelConnect(eq(mockChannel), any());
    }

    @NotNull private static SingleAddressConnectFuture createSingleCheckFuture(TcpCommunicationConnectionCheckFuture fut,
        SocketChannel mockChannel) {
        return fut.new SingleAddressConnectFuture(1337) {
            @Override SocketChannel createChannel() throws IOException {
                return mockChannel;
            }

            @Override void onStatusReceived(boolean res) {
                // No-op.
            }
        };
    }

    TcpCommunicationConnectionCheckFuture createCheckFuture() {
        TcpCommunicationSpi commSpi = mock(TcpCommunicationSpi.class);
        IgniteLogger logger = mock(IgniteLogger.class);

        return new TcpCommunicationConnectionCheckFuture(commSpi, logger, server, Collections.emptyList());
    }

    SocketChannel createChannel() throws IOException {
        SocketChannel mockChannel = mock(SocketChannel.class);
        // Connect should return false;
        when(mockChannel.connect(any())).thenReturn(false);

        return mockChannel;
    }

}