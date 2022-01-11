/*
 * Copyright 2021 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.ssl;

import javax.net.ssl.*;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.KeyManagementException;
import java.security.SecureRandom;

/**
 * Wrapper for SSLContextSpi that allows to reconfigure SSL settings.
 */
public abstract class SSLContextSpiWrapper extends SSLContextSpi {
    /** */
    protected final SSLContext sslContextDelegate;

    public SSLContextSpiWrapper(SSLContext sslContextDelegate) {
        this.sslContextDelegate = sslContextDelegate;
    }

    /**
     * Sets the necessary parameters off the SSLEngine.
     * @param engine engine to configure.
     */
    protected abstract void configureSSLEngine(SSLEngine engine);

    /**
     * Sets the necessary parameters off the Socket.
     * @param socket socket to configure.
     */
    protected abstract void configureSocket(Socket socket);

    /**
     * Sets the necessary parameters off the ServerSocket.
     * @param socket socket to configure.
     */
    protected abstract void configureServerSocket(ServerSocket socket);

    /** {@inheritDoc} */
    @Override protected void engineInit(KeyManager[] keyManagers, TrustManager[] trustManagers,
        SecureRandom secureRandom) throws KeyManagementException {
        sslContextDelegate.init(keyManagers, trustManagers, secureRandom);
    }

    /** {@inheritDoc} */
    @Override protected SSLSocketFactory engineGetSocketFactory() {
        return new SSLSocketFactoryClientAuthWrapper(sslContextDelegate.getSocketFactory());
    }

    /** {@inheritDoc} */
    @Override protected SSLServerSocketFactory engineGetServerSocketFactory() {
        return new SSLServerSocketFactoryClientAuthWrapper(sslContextDelegate.getServerSocketFactory());
    }

    /** {@inheritDoc} */
    @Override protected SSLEngine engineCreateSSLEngine() {
        final SSLEngine engine = sslContextDelegate.createSSLEngine();
        configureSSLEngine(engine);
        return engine;
    }

    /** {@inheritDoc} */
    @Override protected SSLEngine engineCreateSSLEngine(String s, int i) {
        final SSLEngine engine = sslContextDelegate.createSSLEngine(s, i);
        configureSSLEngine(engine);
        return engine;
    }

    /** {@inheritDoc} */
    @Override protected SSLSessionContext engineGetServerSessionContext() {
        return sslContextDelegate.getServerSessionContext();
    }

    /** {@inheritDoc} */
    @Override protected SSLSessionContext engineGetClientSessionContext() {
        return sslContextDelegate.getClientSessionContext();
    }

    /** {@inheritDoc} */
    @Override protected SSLParameters engineGetDefaultSSLParameters() {
        return sslContextDelegate.getDefaultSSLParameters();
    }

    /** {@inheritDoc} */
    @Override protected SSLParameters engineGetSupportedSSLParameters() {
        return sslContextDelegate.getSupportedSSLParameters();
    }

    private class SSLSocketFactoryClientAuthWrapper extends SSLSocketFactory {
        /** */
        private final SSLSocketFactory sslSocketFactoryDelegate;

        /** */
        SSLSocketFactoryClientAuthWrapper(SSLSocketFactory sslSocketFactoryDelegate) {
            this.sslSocketFactoryDelegate = sslSocketFactoryDelegate;
        }

        /** {@inheritDoc} */
        @Override public String[] getDefaultCipherSuites() {
            return sslSocketFactoryDelegate.getDefaultCipherSuites();
        }

        /** {@inheritDoc} */
        @Override public String[] getSupportedCipherSuites() {
            return sslSocketFactoryDelegate.getSupportedCipherSuites();
        }

        /** {@inheritDoc} */
        @Override public Socket createSocket() throws IOException {
            Socket socket = sslSocketFactoryDelegate.createSocket();
            configureSocket(socket);
            return socket;
        }

        /** {@inheritDoc} */
        @Override public Socket createSocket(Socket sock, String host, int port, boolean autoClose) throws IOException {
            Socket socket = sslSocketFactoryDelegate.createSocket(sock, host, port, autoClose);
            configureSocket(socket);
            return socket;
        }

        /** {@inheritDoc} */
        @Override public Socket createSocket(String host, int port) throws IOException {
            Socket socket = sslSocketFactoryDelegate.createSocket(host, port);
            configureSocket(socket);
            return socket;
        }

        /** {@inheritDoc} */
        @Override public Socket createSocket(String host, int port, InetAddress locAddr, int locPort) throws IOException {
            Socket socket = sslSocketFactoryDelegate.createSocket(host, port, locAddr, locPort);
            configureSocket(socket);
            return socket;
        }

        /** {@inheritDoc} */
        @Override public Socket createSocket(InetAddress addr, int port) throws IOException {
            Socket socket = sslSocketFactoryDelegate.createSocket(addr, port);
            configureSocket(socket);
            return socket;
        }

        /** {@inheritDoc} */
        @Override public Socket createSocket(InetAddress addr, int port, InetAddress locAddr, int locPort) throws IOException {
            Socket socket = sslSocketFactoryDelegate.createSocket(addr, port, locAddr, locPort);
            configureSocket(socket);
            return socket;
        }
    }

    class SSLServerSocketFactoryClientAuthWrapper extends SSLServerSocketFactory {
        /** */
        private final SSLServerSocketFactory sslServerSocketFactoryDelegate;

        /** */
        SSLServerSocketFactoryClientAuthWrapper(SSLServerSocketFactory sslServerSocketFactoryDelegate) {
            this.sslServerSocketFactoryDelegate = sslServerSocketFactoryDelegate;
        }

        /** {@inheritDoc} */
        @Override public String[] getDefaultCipherSuites() {
            return sslServerSocketFactoryDelegate.getDefaultCipherSuites();
        }

        /** {@inheritDoc} */
        @Override public String[] getSupportedCipherSuites() {
            return sslServerSocketFactoryDelegate.getSupportedCipherSuites();
        }

        /** {@inheritDoc} */
        @Override public ServerSocket createServerSocket(int port) throws IOException {
            ServerSocket serverSocket = sslServerSocketFactoryDelegate.createServerSocket(port);
            configureServerSocket(serverSocket);
            return serverSocket;
        }

        /** {@inheritDoc} */
        @Override public ServerSocket createServerSocket(int port, int backlog) throws IOException {
            ServerSocket serverSocket = sslServerSocketFactoryDelegate.createServerSocket(port, backlog);
            configureServerSocket(serverSocket);
            return serverSocket;
        }

        /** {@inheritDoc} */
        @Override public ServerSocket createServerSocket(int port, int backlog, InetAddress locAddr) throws IOException {
            ServerSocket serverSocket = sslServerSocketFactoryDelegate.createServerSocket(port, backlog, locAddr);
            configureServerSocket(serverSocket);
            return serverSocket;
        }
    }
}
