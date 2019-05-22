/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal;

import java.io.IOException;
import java.net.Socket;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.cluster.IgniteClusterEx;
import org.apache.ignite.internal.managers.GridManagerAdapter;
import org.apache.ignite.internal.util.nio.GridNioServer;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridStringLogger;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.logger.GridTestLog4jLogger;
import org.junit.Test;

/**
 *
 */
public class IgniteTcpCommNotBlockedOnClientFailureTest extends GridCommonAbstractTest {
    /** */
    private boolean clientMode;

    /** */
    private GridStringLogger inMemoryLog;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setClientMode(clientMode);

        if (!clientMode) {
            cfg.setClientFailureDetectionTimeout(10_000);

            cfg.setSystemWorkerBlockedTimeout(5_000);

            cfg.setGridLogger(inMemoryLog);
        }

        return cfg;
    }

    /**
     * Test verifies that FailureProcessor doesn't treat tcp-comm-worker thread as blocked when
     * the thread handles situation of failed client node.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNoMessagesFromFailureProcessor() throws Exception {
        inMemoryLog = new GridStringLogger(false, new GridTestLog4jLogger());

        inMemoryLog.logLength(1024 * 1024);

        IgniteEx srv = startGrid(0);

        clientMode = true;

        IgniteEx client00 = startGrid("client00");

        client00.getOrCreateCache(new CacheConfiguration<>("cache0"));

        breakClient(client00);

        boolean waitRes = GridTestUtils.waitForCondition(() -> {
            IgniteClusterEx cl = srv.cluster();

            return (cl.topology(cl.topologyVersion()).size() == 1);
        }, 30_000);

        assertTrue(waitRes);

        assertFalse(inMemoryLog.toString().contains("name=tcp-comm-worker"));
    }

    /** */
    private void breakClient(IgniteEx client) throws Exception {
        Object clientDiscoSpi = ((Object[])GridTestUtils.getFieldValue(client.context().discovery(), GridManagerAdapter.class, "spis"))[0];

        Object commSpi = ((Object[])GridTestUtils.getFieldValue(client.context().io(), GridManagerAdapter.class, "spis"))[0];

        GridNioServer nioServer = GridTestUtils.getFieldValue(commSpi, "nioSrvr");

        Object clientImpl = GridTestUtils.getFieldValue(clientDiscoSpi, TcpDiscoverySpi.class, "impl");

        GridWorker msgWorker = GridTestUtils.getFieldValue(clientImpl, "msgWorker");

        msgWorker.runner().interrupt();

        nioServer.stop();

        Socket currSock = GridTestUtils.getFieldValue(msgWorker, "currSock", "sock");
        currSock.close();
    }
}
