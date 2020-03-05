package org.apache.ignite.spi.discovery.tcp;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import javax.net.SocketFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocketFactory;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.ssl.SslContextFactory;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Check a ssl socket configuration which used in discovery.
 */
public class DiscoveryClientSocketTest {
    /** Port to listen. */
    public static final int PORT_TO_LNSR = 12346;
    /** Host for bind. */
    public static final String HOST = "localhost";

    /** SSL server socket factory. */
    private SSLServerSocketFactory sslSrvSockFactory;
    /** SSL socket factory. */
    private SocketFactory sslSockFactory1;
    /** Fake TCP discovery SPI. */
    private TcpDiscoverySpi fakeTcpDiscoverySpi;

    /**
     * Configure SSL and Discovery.
     */
    @Before
    public void before() {
        SslContextFactory socketFactory = (SslContextFactory)GridTestUtils.sslTrustedFactory("node01", "trustone");
        SSLContext sslCtx = socketFactory.create();

        sslSrvSockFactory = sslCtx.getServerSocketFactory();
        sslSockFactory1 = sslCtx.getSocketFactory();
        fakeTcpDiscoverySpi = new TcpDiscoverySpi();

        fakeTcpDiscoverySpi.setSoLinger(1);
    }

    /**
     * It creates a SSL socket server and client for checks correctness closing when write exceed read.
     *
     * @throws Exception If failed.
     */
    @Test
    public void sslSocketTest() throws Exception {
        try (ServerSocket listen = sslSrvSockFactory.createServerSocket(PORT_TO_LNSR)) {
            System.out.println("Server started.");

            Socket connection = null;

            IgniteInternalFuture clientFut = GridTestUtils.runAsync(this::startSslClient);

            while ((connection = listen.accept()) != null) {
                try {
                    fakeTcpDiscoverySpi.configureSocketOptions(connection);

                    InputStream in = connection.getInputStream();
                    OutputStream out = connection.getOutputStream();

                    int c;
                    while ((c = in.read()) != -1)
                        out.write(c);

                    out.close();
                    in.close();
                    connection.close();
                }
                catch (Exception e) {
                    U.closeQuiet(connection);

                    System.out.println("Ex: " + e.getMessage() + " (Socket closed)");

                    break;
                }
            }

            clientFut.get();
        }
    }

    /**
     * Test starts ssl client socket and writes data until socket's write blocking. When the socket is blocking on write
     * tries to close it.
     */
    public void startSslClient() {
        try (Socket clientSocket = sslSockFactory1.createSocket(HOST, PORT_TO_LNSR)) {
            System.out.println("Client started.");

            fakeTcpDiscoverySpi.configureSocketOptions(clientSocket);

            long handshakeStartTime = System.currentTimeMillis();

            //need to send message in order to ssl handshake passed.
            clientSocket.getOutputStream().write(IgniteUtils.IGNITE_HEADER);

            byte[] buf = new byte[4];
            int read = 0;

            while (read < buf.length) {
                int r = clientSocket.getInputStream().read(buf, read, buf.length - read);

                if (r >= 0)
                    read += r;
                else
                    fail("Failed to read from socket.");
            }

            assertEquals("Handshake did not pass, readed bytes: " + read, read, buf.length);

            long handshakeInterval = System.currentTimeMillis() - handshakeStartTime;

            System.out.println("Echo: " + U.bytesToInt(buf, 0) + " handshake time: " + handshakeInterval + "ms");

            int iter = 0;

            try {
                while (true) {
                    iter++;

                    IgniteInternalFuture writeFut = GridTestUtils.runAsync(() -> {
                        try {
                            clientSocket.getOutputStream().write(new byte[4 * 1024]);
                        }
                        catch (IOException e) {
                            assertEquals("Socket closed", e.getMessage());
                        }
                    });

                    writeFut.get(10 * handshakeInterval);
                }
            }
            catch (IgniteFutureTimeoutCheckedException e) {
                System.out.println("Socket stuck on write, when passed " + (iter * 4) + "KB through itself.");
            }

            System.out.println("Try to close a socket.");

            clientSocket.close();
        }
        catch (Exception e) {
            fail(e.getMessage());
        }
    }
}
