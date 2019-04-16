/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 * 
 * Commons Clause Restriction
 * 
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 * 
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 * 
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.spi.discovery.zk.internal;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.events.EventType.EVT_CLIENT_NODE_DISCONNECTED;

/**
 * Tests for Zookeeper SPI discovery client reconnect.
 */
public class ZookeeperDiscoveryClientReconnectTest extends ZookeeperDiscoverySpiTestBase {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReconnectServersRestart_1() throws Exception {
        reconnectServersRestart(1);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReconnectServersRestart_2() throws Exception {
        reconnectServersRestart(3);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReconnectServersRestart_3() throws Exception {
        startGrid(0);

        helper.clientMode(true);

        startGridsMultiThreaded(5, 5);

        stopGrid(getTestIgniteInstanceName(0), true, false);

        final int srvIdx = ThreadLocalRandom.current().nextInt(5);

        final AtomicInteger idx = new AtomicInteger();

        info("Restart nodes.");

        // Test concurrent start when there are disconnected nodes from previous cluster.
        GridTestUtils.runMultiThreaded(new Callable<Void>() {
            @Override public Void call() throws Exception {
                int threadIdx = idx.getAndIncrement();

                helper.clientModeThreadLocal(threadIdx == srvIdx || ThreadLocalRandom.current().nextBoolean());

                startGrid(threadIdx);

                return null;
            }
        }, 5, "start-node");

        waitForTopology(10);

        evts.clear();
    }

    /**
     * Checks that a client will reconnect after previous cluster data was cleaned.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testReconnectServersRestart_4() throws Exception {
        startGrid(0);

        helper.clientMode(true);

        IgniteEx client = startGrid(1);

        helper.clientMode(false);

        CountDownLatch latch = new CountDownLatch(1);

        client.events().localListen(event -> {
            latch.countDown();

            return true;
        }, EVT_CLIENT_NODE_DISCONNECTED);

        waitForTopology(2);

        stopGrid(0);

        evts.clear();

        // Waiting for client starts to reconnect and create join request.
        assertTrue("Failed to wait for client node disconnected.", latch.await(15, SECONDS));

        // Restart cluster twice for incrementing internal order. (alive zk-nodes having lower order and containing
        // client join request will be removed). See {@link ZookeeperDiscoveryImpl#cleanupPreviousClusterData}.
        startGrid(0);

        stopGrid(0);

        evts.clear();

        startGrid(0);

        waitForTopology(2);
    }

    /**
     * @param srvs Number of server nodes in test.
     * @throws Exception If failed.
     */
    private void reconnectServersRestart(int srvs) throws Exception {
        sesTimeout = 30_000;

        startGridsMultiThreaded(srvs);

        helper.clientMode(true);

        final int CLIENTS = 10;

        startGridsMultiThreaded(srvs, CLIENTS);

        helper.clientMode(false);

        long stopTime = System.currentTimeMillis() + 30_000;

        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        final int NODES = srvs + CLIENTS;

        int iter = 0;

        while (System.currentTimeMillis() < stopTime) {
            int restarts = rnd.nextInt(10) + 1;

            info("Test iteration [iter=" + iter++ + ", restarts=" + restarts + ']');

            for (int i = 0; i < restarts; i++) {
                GridTestUtils.runMultiThreaded(new IgniteInClosure<Integer>() {
                    @Override public void apply(Integer threadIdx) {
                        stopGrid(getTestIgniteInstanceName(threadIdx), true, false);
                    }
                }, srvs, "stop-server");

                startGridsMultiThreaded(0, srvs);
            }

            final Ignite srv = ignite(0);

            assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    return srv.cluster().nodes().size() == NODES;
                }
            }, 30_000));

            waitForTopology(NODES);

            awaitPartitionMapExchange();
        }

        evts.clear();
    }
}
