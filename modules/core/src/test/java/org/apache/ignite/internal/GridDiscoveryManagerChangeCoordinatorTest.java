/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal;

import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import org.junit.Test;

/**
 * Tests change coordinator event logging.
 */
public class GridDiscoveryManagerChangeCoordinatorTest extends GridCommonAbstractTest {
    /** Listener log messages. */
    private final ListeningTestLogger listeningLog = new ListeningTestLogger(false, log);

    /** */
    private static final String CRD_CHANGE_MSG = "Coordinator changed";

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        listeningLog.clearListeners();

        stopAllGrids();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super
            .getConfiguration(igniteInstanceName)
            .setGridLogger(listeningLog)
            .setClientMode("client".equals(igniteInstanceName))
            .setDaemon("daemon".equals(igniteInstanceName));
    }

    /**
     * Checks that there are no messages like "coordinator changed" after client left topology.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testChangeCoordinatorLogging() throws Exception {
        //Start 2 server nodes
        IgniteEx srv1 = startGrid("server1");
        IgniteEx client = startGrid("client");
        IgniteEx srv2 = startGrid("server2");

        String srv2Id = srv2.cluster().localNode().id().toString();

        srv1.cluster().active();

        LogListener lsnr = LogListener.matches(CRD_CHANGE_MSG)
                .andMatches(srv2Id)
                .build();

        listeningLog.registerListener(lsnr);

        stopGrid("server1");
        startGrid("server1");

        assertTrue(lsnr.check());

        lsnr.reset();

        LogListener lsnr2 = LogListener.matches(CRD_CHANGE_MSG).build();

        listeningLog.registerListener(lsnr);

        stopGrid("client");

        assertFalse(lsnr2.check());
    }

    /**
     * Checks change coordinator event logging with daemon node in topology.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testChangeCoordinatorLoggingDaemonNodeInTopology() throws Exception {
        // Start 2 server nodes
        IgniteEx srv1 = startGrid("server1");
        IgniteEx srv2 = startGrid("server2");

        srv1.cluster().active();

        // Add a client node, daemon node and 3rd server node, which is not in the baseline
        startGrid("client");
        IgniteEx daemon = startGrid("daemon");
        IgniteEx srv3 = startGrid("server3");

        LogListener lsnr = LogListener.matches(CRD_CHANGE_MSG)
                .andMatches(srv2.cluster().localNode().id().toString())
                .build();

        listeningLog.registerListener(lsnr);

        stopGrid("server1");

        assertTrue(lsnr.check());

        lsnr = LogListener.matches(CRD_CHANGE_MSG).build();

        listeningLog.registerListener(lsnr);

        srv1 = startGrid("server1");

        assertFalse(lsnr.check());

        lsnr = LogListener.matches(CRD_CHANGE_MSG)
                .andMatches(srv3.cluster().localNode().id().toString())
                .build();

        listeningLog.registerListener(lsnr);

        stopGrid("server2");

        assertTrue(lsnr.check());

        lsnr = LogListener.matches(CRD_CHANGE_MSG)
                .build();

        stopGrid("client");

        assertFalse(lsnr.check());

        lsnr = LogListener.matches(CRD_CHANGE_MSG)
                .andMatches(srv1.cluster().localNode().id().toString())
                .build();

        listeningLog.registerListener(lsnr);

        stopGrid("server3");

        assertTrue(lsnr.check());
    }
}
