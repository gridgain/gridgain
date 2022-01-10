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

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * An SSLContext wrapper that overrides only {@code needClientAuth} parameter.
 */
public class SSLContextClientAuthWrapper extends SSLContext {
    /**
     * @param delegate Wrapped SSL context.
     * @param needClientAuth Whether client auth is needed.
     */
    public SSLContextClientAuthWrapper(SSLContext delegate, boolean needClientAuth) {
        super(new SSLContextSpiWrapperImpl(delegate, needClientAuth),
            delegate.getProvider(),
            delegate.getProtocol());
    }

    private static class SSLContextSpiWrapperImpl extends SSLContextSpiWrapper {
        /** */
        private final boolean needClientAuth;

        /** */
        SSLContextSpiWrapperImpl(SSLContext sslContextDelegate, boolean needClientAuth) {
            super(sslContextDelegate);

            this.needClientAuth = needClientAuth;
        }

        /** {@inheritDoc} */
        @Override protected void configureSSLEngine(SSLEngine engine) {
            engine.setNeedClientAuth(needClientAuth);
        }

        /** {@inheritDoc} */
        @Override protected void configureSocket(Socket socket) {
            ((SSLSocket)socket).setNeedClientAuth(needClientAuth);
        }

        /** {@inheritDoc} */
        @Override protected void configureServerSocket(ServerSocket socket) {
            ((SSLServerSocket)socket).setNeedClientAuth(needClientAuth);
        }
    }
}
