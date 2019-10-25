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

package org.apache.ignite.agent.service.tracing;

import java.util.List;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.agent.AbstractGridWithAgentTest;
import org.apache.ignite.agent.dto.tracing.Span;
import org.apache.ignite.internal.IgniteEx;
import org.junit.Test;

import static org.apache.ignite.agent.StompDestinationsUtils.buildSaveSpanDest;

/**
 * Tracing service test.
 */
public class TracingServiceSelfTest extends AbstractGridWithAgentTest {
    /**
     * Should send initial states to backend.
     */
    @Test
    public void shouldSendInitialStates() throws Exception {
        IgniteEx ignite = (IgniteEx) startGrid();
        changeGmcUri(ignite);

        IgniteCluster cluster = ignite.cluster();
        cluster.active(true);

        assertWithPoll(() -> interceptor.getPayload(buildSaveSpanDest(cluster.id())) != null);
    }

    /**
     * Should send changed baseline topology.
     */
    @Test
    public void shouldSendSpans() throws Exception {
        IgniteEx ignite_1 = startGrid(0);
        changeGmcUri(ignite_1);

        IgniteCluster cluster = ignite_1.cluster();
        cluster.active(true);

        assertWithPoll(
            () -> {
                List<Span> spans = interceptor.getPayload(buildSaveSpanDest(cluster.id()), List.class);
                return spans != null && !spans.isEmpty();
            }
        );
    }
}
