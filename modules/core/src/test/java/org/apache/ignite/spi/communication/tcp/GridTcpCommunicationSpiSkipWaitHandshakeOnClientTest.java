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

import java.io.InputStream;
import java.net.Socket;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteFeatures;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.GridAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.NODE_ID_MSG_TYPE;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.makeMessageType;

/**
 * This test check that client sends only Node ID message type on connect. There is a gap when {@link
 * IgniteFeatures#allNodesSupports} isn't consistent, because the list of nodes is empty.
 */
public class GridTcpCommunicationSpiSkipWaitHandshakeOnClientTest extends GridCommonAbstractTest {
    /** Tcp communication port. */
    private static final int TCP_COMMUNICATION_PORT = 45010;

    /** Local adders. */
    private static final String LOCAL_ADDERS = "127.0.0.1";

    /** Message type bytes. */
    private static final int MESSAGE_TYPE_BYTES = 2;

    /** Test logger. */
    private final ListeningTestLogger log = new ListeningTestLogger(false, GridAbstractTest.log);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setGridLogger(log);

        cfg.setClientMode(true);

        return cfg;
    }

    /**
     *
     */
    @Test
    public void clientCanNotSendHandshakeWaitMessage() throws Exception {
        startClientAndWaitCommunicationActivation();

        try (Socket sock = new Socket(LOCAL_ADDERS, TCP_COMMUNICATION_PORT)) {
            try (InputStream in = sock.getInputStream()) {
                byte[] b = new byte[MESSAGE_TYPE_BYTES];

                int read = in.read(b);

                assertEquals(MESSAGE_TYPE_BYTES, read);

                short respMsgType = makeMessageType(b[0], b[1]);

                // Client can't give HANDSHAKE_WAIT_MSG_TYPE.
                Assert.assertEquals(NODE_ID_MSG_TYPE, respMsgType);
            }
        }
    }

    /**
     *
     */
    private void startClientAndWaitCommunicationActivation() throws IgniteInterruptedCheckedException {
        LogListener lsnr = LogListener
            .matches(s -> s.startsWith("Successfully bound communication NIO server to TCP port [port=" + TCP_COMMUNICATION_PORT))
            .times(1)
            .build();
        log.registerListener(lsnr);

        new Thread(() -> {
            try {
                startGrid(0);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }).start();

        assertTrue(GridTestUtils.waitForCondition(lsnr::check, 20_000));
    }
}
