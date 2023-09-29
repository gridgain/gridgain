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

package org.apache.ignite.internal.util.nio;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteOrder;
import java.util.concurrent.CountDownLatch;
import javax.net.ssl.SSLContext;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.nio.ssl.GridNioSslFilter;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Tests for new NIO server with SSL enabled.
 */
public class GridNioSslSelfTest extends GridNioSelfTest {
    /** Test SSL context. */
    private static SSLContext sslCtx;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        sslCtx = GridTestUtils.sslContext();
    }

    /** {@inheritDoc} */
    @Override protected Socket createSocket() throws IgniteCheckedException {
        try {
            return sslCtx.getSocketFactory().createSocket();
        }
        catch (Exception e) {
            throw new IgniteCheckedException(e);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected GridNioServer.Builder<?> serverBuilder(int port,
        GridNioParser parser,
        GridNioServerListener lsnr)
        throws Exception
    {
        return GridNioServer.builder()
            .address(U.getLocalHost())
            .port(port)
            .listener(lsnr)
            .logger(log)
            .selectorCount(2)
            .igniteInstanceName("nio-test-grid")
            .tcpNoDelay(false)
            .directBuffer(true)
            .byteOrder(ByteOrder.nativeOrder())
            .socketSendBufferSize(0)
            .socketReceiveBufferSize(0)
            .sendQueueLimit(0)
            .filters(
                new GridNioCodecFilter(parser, log, false),
                new GridNioSslFilter(sslCtx, true, ByteOrder.nativeOrder(), log, null));
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testWriteTimeout() throws Exception {
        // Skip base test because it enables "skipWrite" mode in the GridNioServer
        // which makes SSL handshake impossible.
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testAsyncSendReceive() throws Exception {
        // No-op, do not want to mess with SSL channel.
    }

    @Test
    public void testSendReceiveRaw() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        NioListener lsnr = new NioListener(latch);
        GridNioServer<?> srvr = startServer(new GridBufferedParser(true, ByteOrder.nativeOrder()), lsnr);

        byte[] payload = new byte[]{
                22, 3, 3, 1, 122, 1, 0, 1, 118, 3, 3, -89, 70, -84, -111, -109, 27, 95, -94, -90, -106, 105, 22, 102, -55, 1, 113, 70, -73, 36, -5, 62, 32, -101, 95, 38, 82, -106, -91, 16, 10, -114, -9, 32, 53, 35, -69, 29, 47, 22, -12, 88, 52, 101, -95, -38, 38, -120, 113, -6, -47, 67, -33, 1, 22, 53, -59, 123, -71, 19, -76, 108, -100, 99, -110, -61, 0, 98, 19, 2, 19, 1, 19, 3, -64, 44, -64, 43, -52, -87, -64, 48, -52, -88, -64, 47, 0, -97, -52, -86, 0, -93, 0, -98, 0, -94, -64, 36, -64, 40, -64, 35, -64, 39, 0, 107, 0, 106, 0, 103, 0, 64, -64, 46, -64, 50, -64, 45, -64, 49, -64, 38, -64, 42, -64, 37, -64, 41, -64, 10, -64, 20, -64, 9, -64, 19, 0, 57, 0, 56, 0, 51, 0, 50, -64, 5, -64, 15, -64, 4, -64, 14, 0, -99, 0, -100, 0, 61, 0, 60, 0, 53, 0, 47, 0, -1, 1, 0, 0, -53, 0, 5, 0, 5, 1, 0, 0, 0, 0, 0, 10, 0, 22, 0, 20, 0, 29, 0, 23, 0, 24, 0, 25, 0, 30, 1, 0, 1, 1, 1, 2, 1, 3, 1, 4, 0, 11, 0, 2, 1, 0, 0, 13, 0, 40, 0, 38, 4, 3, 5, 3, 6, 3, 8, 4, 8, 5, 8, 6, 8, 9, 8, 10, 8, 11, 4, 1, 5, 1, 6, 1, 4, 2, 3, 3, 3, 1, 3, 2, 2, 3, 2, 1, 2, 2, 0, 50, 0, 40, 0, 38, 4, 3, 5, 3, 6, 3, 8, 4, 8, 5, 8, 6, 8, 9, 8, 10, 8, 11, 4, 1, 5, 1, 6, 1, 4, 2, 3, 3, 3, 1, 3, 2, 2, 3, 2, 1, 2, 2, 0, 17, 0, 9, 0, 7, 2, 0, 4, 0, 0, 0, 0, 0, 23, 0, 0, 0, 43, 0, 5, 4, 3, 4, 3, 3, 0, 45, 0, 2, 1, 1, 0, 51, 0, 38, 0, 36, 0, 29, 0, 32, 77, 68, 70, 52, -34, -13, 76, -28, 71, -39, 36, 86, 7, 80, -124, -80, -40, -23, 92, 11, -64, 100, 95, 12, -28, 81, -102, 19, -6, 62, -65, 98,
                20, 3, 3, 0, 1, 1,
                23, 3, 3, 0, 85, -72, 81, 118, 56, -73, -71, 111, 94, -71, 76, -15, -118, 68, 68, 65, 48, -90, -11, 47, -46, -62, 18, 21, 35, 58, -84, 38, -91, -56, 72, -86, 100, 115, 88, 91, -77, 71, 67, 13, -115, -102, 44, -19, 45, -64, -101, 57, 66, -71, 127, 101, 122, -19, -34, -67, -48, 42, -1, 15, -101, 86, 32, -71, 33, -47, -95, -26, 113, 50, -106, 42, 11, -19, 66, 113, 38, 64, -58, 115, 90, -13, 17, -69, 5, -120, 23, 3, 3, 0, 37, -22, -108, -104, -123, -24, 115, -79, 47, 70, -75, 28, -88, 110, 125, -78, -94, 76, -18, 39, 51, 107, -107, -127, -20, -25, -101, 112, -30, -114, -63, 53, 38, -98, 53, -72, 93, -110, 23, 3, 3, 0, 43, -14, 118, -100, -104, -55, -35, 64, -33, 44, 110, 67, -116, -46, -89, -97, -27, 112, 6, -43, -114, 4, -15, 37, 25, 124, 65, 92, 12, -95, 53, -72, -98, 88, -2, -20, 91, -28, 54, -33, 125, 17, -33, -83, 23, 3, 3, 0, 35, -44, -57, -117, -111, 73, 58, -108, -59, -90, 122, 52, -78, 36, 110, 69, -84, -58, -110, 15, -93, -76, 97, -41, 120, 99, 27, 53, -56, 43, -19, -125, 60, -10, -94, 55,
        };

        // Send payload to a raw Java socket
        try (Socket s = createSocket()) {
            s.connect(new InetSocketAddress(U.getLocalHost(), srvr.port()), 1000);
            OutputStream outputStream = s.getOutputStream();

            // Write payload byte by byte
            for (byte b : payload) {
                Thread.sleep(100);
                outputStream.write(b);
                outputStream.flush();
            }
        }

        assert latch.await(5, SECONDS);
        srvr.stop();
    }
}
