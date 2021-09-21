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
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Wrapper for {@link SSLContext} that extend source context with custom SSL parameters.
 */
public class SSLContextParametersWrapper extends SSLContext {
    /**
     * @param delegate Wrapped SSL context.
     * @param sslParameters Extended SSL parameters.
     */
    public SSLContextParametersWrapper(SSLContext delegate, SSLParameters sslParameters) {
        super(new SSLContextParametersSpiWrapper(delegate, sslParameters),
            delegate.getProvider(),
            delegate.getProtocol());
    }

    private static class SSLContextParametersSpiWrapper extends SSLContextSpiWrapper {
        /** */
        private final SSLParameters sslParameters;

        /** */
        SSLContextParametersSpiWrapper(SSLContext sslContextDelegate, SSLParameters sslParameters) {
            super(sslContextDelegate);

            this.sslParameters = sslParameters;
        }

        /** {@inheritDoc} */
        @Override protected void configureSSLEngine(SSLEngine engine) {
            if (sslParameters != null)
                engine.setSSLParameters(sslParameters);
        }

        /** {@inheritDoc} */
        @Override protected void configureSocket(Socket socket) {
            if (sslParameters != null)
                ((SSLSocket) socket).setSSLParameters(sslParameters);
        }

        /** {@inheritDoc} */
        @Override protected void configureServerSocket(ServerSocket socket) {
            if (sslParameters != null)
                ((SSLServerSocket) socket).setSSLParameters(sslParameters);
        }
    }
}
