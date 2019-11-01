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

package org.apache.ignite.agent.service.metrics;

import org.apache.ignite.IgniteCluster;
import org.apache.ignite.agent.AgentCommonAbstractTest;
import org.apache.ignite.internal.IgniteEx;
import org.junit.Test;

import static org.apache.ignite.agent.StompDestinationsUtils.buildMetricsDest;
import static org.apache.ignite.agent.StompDestinationsUtils.buildMetricsPullTopic;

/**
 * Metric service test.
 */
public class MetricsServiceSelfTest extends AgentCommonAbstractTest {
    /**
     * Should send cluster metrics.
     */
    @Test
    public void shouldSendMetricsOnPull() throws Exception {
        IgniteEx ignite_1 = startGrid(0);
        changeManagementConsoleUri(ignite_1);

        IgniteCluster cluster = ignite_1.cluster();
        cluster.active(true);

        assertWithPoll(
            () -> interceptor.isSubscribedOn(buildMetricsPullTopic())
        );

        template.convertAndSend(buildMetricsPullTopic(), "pull");

        assertWithPoll(
            () -> {
                String metrics = new String((byte[]) interceptor.getPayload(buildMetricsDest(cluster.id())));
                return metrics != null && metrics.contains(cluster.tag());
            }
        );
    }
}
