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

package org.apache.ignite.internal.processors.service;

import org.apache.ignite.Ignite;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.service.inner.LongInitializedTestService;
import org.apache.ignite.internal.util.lang.gridfunc.AlwaysTruePredicate;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.services.ServiceConfiguration;
import org.junit.Test;

/**
 * Tests that requests of change service's state won't be missed and will be handled correctly on a node left.
 *
 * It uses {@link LongInitializedTestService} with long running #init method to delay requests processing and blocking
 * communication spi to be sure that single deployment message won't be sent by a node at shutdown.
 */
public class ServiceDeploymentProcessingOnNodesLeftTest extends ServiceDeploymentProcessAbstractTest {
    /**
     * @throws Exception In case of an error.
     */
    @Test
    public void testDeploymentProcessingOnServersLeaveTopology() throws Exception {
        try {
            startGrids(4);

            IgniteEx client = startGrid(getConfiguration("client").setClientMode(true));

            IgniteEx ignite1 = grid(1);
            IgniteEx ignite2 = grid(2);

            ((BlockingTcpCommunicationSpi)ignite1.configuration().getCommunicationSpi()).block();
            ((BlockingTcpCommunicationSpi)ignite2.configuration().getCommunicationSpi()).block();

            IgniteFuture fut = client.services().deployNodeSingletonAsync("testService",
                new LongInitializedTestService(5000L));
            IgniteFuture fut2 = client.services().deployNodeSingletonAsync("testService2",
                new LongInitializedTestService(5000L));

            stopNode(ignite1);
            stopNode(ignite2);

            fut.get(TEST_FUTURE_WAIT_TIMEOUT);
            fut2.get(TEST_FUTURE_WAIT_TIMEOUT);

            IgniteEx ignite3 = grid(3);

            assertNotNull(ignite3.services().service("testService"));
            assertNotNull(ignite3.services().service("testService2"));
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception In case of an error.
     */
    @Test
    public void testDeploymentProcessingOnServersAndClientsLeaveTopology() throws Exception {
        try {
            Ignite ignite0 = startGrids(4);

            IgniteEx client1 = startGrid(getConfiguration("client1").setClientMode(true));
            IgniteEx client2 = startGrid(getConfiguration("client2").setClientMode(true));

            IgniteEx ignite1 = grid(1);

            ((BlockingTcpCommunicationSpi)client1.configuration().getCommunicationSpi()).block();
            ((BlockingTcpCommunicationSpi)client2.configuration().getCommunicationSpi()).block();
            ((BlockingTcpCommunicationSpi)ignite1.configuration().getCommunicationSpi()).block();

            ServiceConfiguration srvcCfg = new ServiceConfiguration();

            srvcCfg.setName("testService");
            srvcCfg.setMaxPerNodeCount(1);
            srvcCfg.setService(new LongInitializedTestService(10_000));
            srvcCfg.setNodeFilter(new AlwaysTruePredicate<>());

            IgniteFuture fut = ignite0.services().deployAsync(srvcCfg);

            stopNode(client1);
            stopNode(client2);
            stopNode(ignite1);

            fut.get(TEST_FUTURE_WAIT_TIMEOUT);

            assertNotNull(ignite0.services().service("testService"));

            IgniteEx ignite3 = grid(3);

            assertNotNull(ignite3.services().service("testService"));
        }
        finally {
            stopAllGrids();
        }
    }
}
