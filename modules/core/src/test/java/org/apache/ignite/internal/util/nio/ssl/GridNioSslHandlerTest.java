/*
 * Copyright 2024 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.util.nio.ssl;

import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.apache.ignite.internal.util.nio.ssl.GridNioSslHandler.SSL_HANDSHAKE_TIMEOUT_MSG_PREFIX;
import static org.apache.ignite.internal.util.nio.ssl.GridNioSslHandler.SSL_UNWRAP_TIMEOUT_MSG_PREFIX;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link GridNioSslHandler}.
 */
@RunWith(MockitoJUnitRunner.class)
public class GridNioSslHandlerTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDataUnwrapTimeout() throws Exception {
        testTimeout(false, HandshakeStatus.NEED_TASK, SSL_UNWRAP_TIMEOUT_MSG_PREFIX);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testHandshakeUnwrapTimeout() throws Exception {
        testTimeout(true, HandshakeStatus.NEED_UNWRAP, SSL_UNWRAP_TIMEOUT_MSG_PREFIX);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testHandshakeTimeout() throws Exception {
        testTimeout(true, HandshakeStatus.NEED_TASK, SSL_HANDSHAKE_TIMEOUT_MSG_PREFIX);
    }

    /** */
    private void testTimeout(boolean handshake, HandshakeStatus handshakeStatus, String errMsg) throws Exception {
        GridNioSslFilter filter = mock(GridNioSslFilter.class);

        GridNioSession ses = mock(GridNioSession.class);

        SSLSession sslSession = mock(SSLSession.class);

        when(sslSession.getPacketBufferSize()).thenReturn(128);
        when(sslSession.getApplicationBufferSize()).thenReturn(128);

        SSLEngine engine = mock(SSLEngine.class);

        when(engine.getHandshakeStatus()).thenReturn(handshakeStatus);
        when(engine.getSession()).thenReturn(sslSession);
        when(engine.unwrap(any(ByteBuffer.class), any(ByteBuffer.class)))
                .thenReturn(new SSLEngineResult(SSLEngineResult.Status.OK, handshakeStatus, 0, 0));

        GridNioSslHandler hnd = new GridNioSslHandler(filter,
                ses,
                engine,
                true,
                ByteOrder.nativeOrder(),
                log,
                handshake,
                null);

        ByteBuffer msg = ByteBuffer.allocateDirect(64);

        try {
            hnd.messageReceived(msg);

            fail();
        } catch (SSLException ex) {
            assertTrue(ex.getMessage() != null && ex.getMessage().startsWith(errMsg));
        }
    }
}
