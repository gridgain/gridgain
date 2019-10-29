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

package org.apache.ignite.agent.service.event;

import java.util.List;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.agent.AbstractGridWithAgentTest;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.Test;

import static org.apache.ignite.agent.StompDestinationsUtils.buildEventsDest;
import static org.apache.ignite.events.EventType.EVT_CLUSTER_ACTIVATED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;

/**
 * Events service test.
 */
public class EventsServiceSelfTest extends AbstractGridWithAgentTest {
    /**
     * Should register handler and export events.
     */
    @Test
    public void shouldSendNodeJoinEvent() throws Exception {
        IgniteEx ignite = startGrid(0);
        changeGmcUri(ignite);

        startGrid(1);

        IgniteCluster cluster = ignite.cluster();
        cluster.active(true);

        assertWithPoll(() -> {
            List<JsonNode> evts = interceptor.getListPayload(buildEventsDest(cluster.id()), JsonNode.class);

            assertNotNull(evts);
            assertEquals(1, evts.size());

            JsonNode evt = F.first(evts);

            assertEquals(EVT_NODE_JOINED, evt.get("typeId").asInt());

            return true;
        });
    }

    /**
     * Should register handler and export events.
     */
    @Test
    public void shouldSendActivationEvent() throws Exception {
        IgniteEx ignite = (IgniteEx)startGrid();
        changeGmcUri(ignite);

        IgniteCluster cluster = ignite.cluster();
        cluster.active(true);

        assertWithPoll(() -> {
            List<JsonNode> evts = interceptor.getListPayload(buildEventsDest(cluster.id()), JsonNode.class);

            assertNotNull(evts);
            assertEquals(1, evts.size());

            JsonNode evt = F.first(evts);

            assertEquals(EVT_CLUSTER_ACTIVATED, evt.get("typeId").asInt());

            return true;
        });
    }
}
