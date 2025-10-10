/*
 * Copyright 2025 GridGain Systems, Inc. and Contributors.
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

import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.internal.CommunicationMessageAcknowledgeTest.disableAcksForTestDuration;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

/**
 * Tests for communication-related metrics.
 */
public class CommunicationMetricsTest extends GridCommonAbstractTest {
    /**
     * Name of the test cache.
     */
    private static final String CACHE_NAME = "cache1";

    /**
     * Whether acks sending is enabled in all nodes that are started.
     * This flag only influences nodes started after it is assigned.
     */
    private boolean acksSendingEnabled = true;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpCommunicationSpi communicationSpi = (TcpCommunicationSpi) cfg.getCommunicationSpi();

        if (!acksSendingEnabled)
            disableAcksForTestDuration(communicationSpi);

        CacheConfiguration<?, ?> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        ccfg.setName(CACHE_NAME);
        ccfg.setCacheMode(PARTITIONED);
        ccfg.setBackups(1);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * Makes sure that unacked message queue size (as visible through metrics) reaches positive values
     * when acks are not being sent.
     */
    @Test
    public void unackedQueueRaiseShouldBeVisibleInMetrics() throws Exception {
        acksSendingEnabled = false;

        IgniteEx firstNode = startGrid("first");
        IgniteEx secondNode = startGrid("second");

        provokeMessageSends(firstNode, secondNode);

        assertThat(firstNode.cluster().metrics().getUnacknowledgedMessagesQueueSize(), is(greaterThan(0)));
    }

    /**
     * Makes sure that unacked message queue size (as visible in metrics) is drained to 0 when acks are being sent.
     */
    @Test
    public void unackedQueueDrainShouldBeVisibleInMetrics() throws Exception {
        acksSendingEnabled = true;

        IgniteEx firstNode = startGrid("first");
        IgniteEx secondNode = startGrid("second");

        provokeMessageSends(firstNode, secondNode);

        assertTrue(
            waitForCondition(
                () -> firstNode.cluster().metrics().getUnacknowledgedMessagesQueueSize() == 0,
                SECONDS.toMillis(10)
            )
        );
    }

    private static void provokeMessageSends(IgniteEx firstNode, IgniteEx secondNode) {
        firstNode.getOrCreateCache(CACHE_NAME);
        IgniteCache<Object, Object> cache = secondNode.cache(CACHE_NAME);

        cache.put("key", "value");
    }
}
