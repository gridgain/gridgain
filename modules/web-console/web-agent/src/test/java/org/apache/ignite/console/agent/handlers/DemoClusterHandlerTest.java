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

package org.apache.ignite.console.agent.handlers;

import org.apache.ignite.console.agent.AgentConfiguration;
import org.apache.ignite.console.demo.AgentClusterDemo;
import org.apache.ignite.console.websocket.TopologySnapshot;
import org.apache.ignite.internal.IgniteFeatures;
import org.apache.ignite.internal.IgnitionEx;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Test for demo cluster handler.
 */
public class DemoClusterHandlerTest {
    /**
     * GG-26784 Test case 1:
     * 1. Disable demo in config.
     * 2. Collect demo topology.
     * 3. It should be {@code null}.
     */
    @Test
    public void shouldNotCollectTopologyIfDemoDisabled() {
        AgentConfiguration cfg = new AgentConfiguration();
        cfg.disableDemo(true);

        DemoClusterHandler demoHnd = new DemoClusterHandler(cfg);

        TopologySnapshot top = demoHnd.topologySnapshot();

        assertNull(top);
    }

    /**
     * GG-26784 Test case 2:
     * 1. Collect demo topology. It should be not {@code null} and contains empty nodes list.
     * 2. Start demo nodes.
     * 3. Check that demo URL is not null.
     * 4. Await while all demo nodes started.
     * 5. Collect demo topology.
     * 6. It should be not {@code null}.
     * 7. Supported  features should have correct value.
     * 8. Stop demo nodes.
     *
     * @throws Exception If failed.
     */
    @Test
    public void shouldCollectTopology() throws Exception {
        AgentConfiguration cfg = new AgentConfiguration();

        DemoClusterHandler demoHnd = new DemoClusterHandler(cfg);

        TopologySnapshot top = demoHnd.topologySnapshot();

        assertNotNull(top);
        assertEquals(0, top.nids().size());

        AgentClusterDemo.tryStart().await();

        assertNotNull(AgentClusterDemo.getDemoUrl());

        do {
            top = demoHnd.topologySnapshot();
        } while (top.nids().size() != 4);

        assertNotNull(top);

        byte[] allFeatures = IgniteFeatures.allFeatures(IgnitionEx.gridx("demo-server-0").context());

        assertArrayEquals(allFeatures, top.getSupportedFeatures());

        AgentClusterDemo.stop();
    }
}
