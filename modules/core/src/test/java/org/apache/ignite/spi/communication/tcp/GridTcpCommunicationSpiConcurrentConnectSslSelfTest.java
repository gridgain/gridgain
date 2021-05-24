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

import org.junit.Test;

/**
 *
 */
public class GridTcpCommunicationSpiConcurrentConnectSslSelfTest extends GridTcpCommunicationSpiConcurrentConnectSelfTest {
    /**
     * Default constructor.
     */
    public GridTcpCommunicationSpiConcurrentConnectSslSelfTest() {
        useSsl = true;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return super.getTestTimeout() * 4;
    }

    @Test
    @Override public void testMultithreaded_NoPairedConnections() throws Exception {
        super.testMultithreaded_NoPairedConnections();
    }

    @Test
    public void testMultithreaded_NoPairedConnections1() throws Exception {
        super.testMultithreaded_NoPairedConnections();
    }

    @Test
    public void testMultithreaded_NoPairedConnections2() throws Exception {
        super.testMultithreaded_NoPairedConnections();
    }

    @Test
    public void testMultithreaded_NoPairedConnections3() throws Exception {
        super.testMultithreaded_NoPairedConnections();
    }

    @Test
    public void testMultithreaded_NoPairedConnections4() throws Exception {
        super.testMultithreaded_NoPairedConnections();
    }

    @Test
    public void testMultithreaded_NoPairedConnections5() throws Exception {
        super.testMultithreaded_NoPairedConnections();
    }

    @Test
    public void testMultithreaded_NoPairedConnections6() throws Exception {
        super.testMultithreaded_NoPairedConnections();
    }

    @Test
    public void testMultithreaded_NoPairedConnections7() throws Exception {
        super.testMultithreaded_NoPairedConnections();
    }

    @Test
    public void testMultithreaded_NoPairedConnections8() throws Exception {
        super.testMultithreaded_NoPairedConnections();
    }

    @Test
    public void testMultithreaded_NoPairedConnections9() throws Exception {
        super.testMultithreaded_NoPairedConnections();
    }

    @Test
    public void testMultithreaded_NoPairedConnections10() throws Exception {
        super.testMultithreaded_NoPairedConnections();
    }

    @Test
    public void testMultithreaded_NoPairedConnections11() throws Exception {
        super.testMultithreaded_NoPairedConnections();
    }

    @Test
    public void testMultithreaded_NoPairedConnections12() throws Exception {
        super.testMultithreaded_NoPairedConnections();
    }

    @Test
    public void testMultithreaded_NoPairedConnections13() throws Exception {
        super.testMultithreaded_NoPairedConnections();
    }

    @Test
    public void testMultithreaded_NoPairedConnections14() throws Exception {
        super.testMultithreaded_NoPairedConnections();
    }

    @Test
    @Override public void sentTestMessageOneToAnotherWithHandshake() throws Exception {
        super.sentTestMessageOneToAnotherWithHandshake();
    }
}
