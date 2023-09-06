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

package org.apache.ignite.internal;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cluster.ClusterGroupEmptyException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_IGNITE_INSTANCE_NAME;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * This tests that network messages sent using {@link TcpCommunicationSpi} get acknowledged according to configuration.
 * <p>
 * To test this, the following strategy is employed. We disable all mechanisms that send acks, and only enable
 * a specific mechanism (like by count threshold, by size threshold, etc) that we want to test. Then we start
 * two Ignite instances, one local and one remote (with amount of heap that is not sufficient to hold all the messages
 * we are going to send, but enough to hold a couple of them). Then we send a few puts through the remote Ignite
 * (that sends them to the local Ignite). If the tested mechanism-to-send-acks does not work, the messages will not
 * be acked, so they will pile up in the heap, and the remote node will soon blow up with an OOM.
 */
public class CommunicationMessageAcknowledgeTest extends GridCommonAbstractTest {
    /** Size of a single put in bytes. */
    private static final int SINGLE_PUT_SIZE = 100 * 1024 * 1024;

    /** Number of puts, so they are 500Mb in total. */
    private static final int PUT_COUNT = 5;

    /**
     * Just 512Mb of heap, which is not enough to hold the whole batch defined by {@link #SINGLE_PUT_SIZE}
     * and {@link #PUT_COUNT}.
     */
    private static final int REMOTE_HEAP_SIZE_MB = 512;

    /**
     * A short but noticeable delay between puts. Needed to make sure that, if we enable timeout-based mechanisms
     * to send acks, we can set a lower duration (like 10ms) as their timeouts, and they will ack
     * messages in a timely fashion.
     */
    private static final int MILLIS_BETWEEN_PUTS = 100;

    /** Name of the node started in the same JVM as the test (the test framework requires us to have at least
     * one such node to start a remote node).
     */
    private static final String LOCAL_NODE_NAME = "local";

    /** Name of a node started in a separate JVM. */
    private static final String REMOTE_NODE_NAME = "remote";

    private static final String CACHE_NAME = "cache1";

    /** Used by individual tests to enable individual mechanisms to send acks. */
    private Consumer<TcpCommunicationSpi> communicationSpiCustomizer;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpCommunicationSpi communicationSpi = (TcpCommunicationSpi) cfg.getCommunicationSpi();

        disableAcksForTestDuration(communicationSpi);

        // Now give each test a possibility to enable sending acks using a setting the test is testing.
        communicationSpiCustomizer.accept(communicationSpi);

        CacheConfiguration<?, ?> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        ccfg.setName(CACHE_NAME);
        ccfg.setCacheMode(PARTITIONED);
        ccfg.setBackups(1);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /***/
    private static void disableAcksForTestDuration(TcpCommunicationSpi communicationSpi) {
        long oneYearInMillis = TimeUnit.DAYS.toMillis(365);

        communicationSpi.setIdleConnectionTimeout(oneYearInMillis);

        // We have much less than 1k messages between the nodes during the test, so 1k is like 'infinity'.
        // We cannot just set a very large number here because the code preallocates an array of this size
        // multiplied by 128.
        communicationSpi.setAckSendThreshold(1_000);

        communicationSpi.setAckSendThresholdBytes(Long.MAX_VALUE);
        communicationSpi.setAckSendThresholdMillis(oneYearInMillis);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        communicationSpiCustomizer = null;

        stopAllGrids();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected boolean isMultiJvm() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected List<String> additionalRemoteJvmArgs() {
        return Arrays.asList(
            "-Xmx" + REMOTE_HEAP_SIZE_MB + "m",
            "-Xms" + REMOTE_HEAP_SIZE_MB + "m",
            "-XX:+CrashOnOutOfMemoryError"
        );
    }

    /** {@inheritDoc} */
    @Override protected boolean isRemoteJvm(String igniteInstanceName) {
        return igniteInstanceName.startsWith(REMOTE_NODE_NAME);
    }

    /**
     * Makes sure no acks are sent when all mechanisms that trigger ack sending are disabled.
     * This is mostly needed to make sure the test as a whole is sane and capable to notice unacked messages
     * piling up.
     */
    @Test
    public void noAcksShouldBeSentWhenAllDisabled() throws Exception {
        testMessagesAckingExpectingFailure(spi -> {});
    }

    /**
     * Makes sure that acks are sent when hitting the configured count threshold.
     */
    @Test
    public void acksShouldBeSentOnCountThreshold() throws Exception {
        testMessagesAckingExpectingSuccess(spi -> spi.setAckSendThreshold(1));
    }

    /**
     * Makes sure that acks are sent when a connection becomes idle (due to no message gets sent before reaching
     * the corresponding timeout).
     */
    @Test
    public void acksShouldBeSentOnIdleConnectionTimeout() throws Exception {
        testMessagesAckingExpectingSuccess(spi -> spi.setIdleConnectionTimeout(10));
    }

    /**
     * Makes sure that acks are sent when hitting the configured accrued message size threshold.
     */
    @Test
    public void acksShouldBeSentOnAccruedSizeThreshold() throws Exception {
        testMessagesAckingExpectingSuccess(spi -> spi.setAckSendThresholdBytes(1024 * 1024));
    }

    /**
     * Makes sure that acks are sent when hitting the configured accrued message size threshold.
     */
    @Test
    public void acksShouldBeSentOnTimeout() throws Exception {
        testMessagesAckingExpectingSuccess(spi -> spi.setAckSendThresholdMillis(10));
    }

    /***/
    private void testMessagesAckingExpectingSuccess(Consumer<TcpCommunicationSpi> communicationSpiCustomizer)
        throws Exception {

        testMessagesAcking(communicationSpiCustomizer, false);
    }

    /***/
    private void testMessagesAckingExpectingFailure(Consumer<TcpCommunicationSpi> communicationSpiCustomizer)
        throws Exception {

        testMessagesAcking(communicationSpiCustomizer, true);
    }

    /**
     * Runs the test by first applying the {@link #communicationSpiCustomizer} and the running the scenario
     * described in the class javadoc and making sure that the node either fails or does not fail, depending
     * on the expectFailure parameter.
     *
     * @param communicationSpiCustomizer    Customizer to customize the configuration before starting nodes.
     * @param expectFailure                 Whether it is expected that the remote node will fail.
     * @throws Exception If something goes wrong.
     */
    @SuppressWarnings({"resource", "BusyWait"})
    private void testMessagesAcking(Consumer<TcpCommunicationSpi> communicationSpiCustomizer, boolean expectFailure)
        throws Exception {

        this.communicationSpiCustomizer = communicationSpiCustomizer;

        IgniteEx localNode = startGrid(LOCAL_NODE_NAME);
        IgniteEx remoteNode = startGrid(REMOTE_NODE_NAME);

        AtomicBoolean remoteNodeFailed = listenForRemoteNodeFailure(localNode);

        localNode.getOrCreateCache(CACHE_NAME);
        IgniteCache<Object, Object> cache = remoteNode.cache(CACHE_NAME);

        Random random = new Random();
        byte[] bytes = new byte[SINGLE_PUT_SIZE];

        for (int i = 0; i < PUT_COUNT && !remoteNodeFailed.get(); i++) {
            random.nextBytes(bytes);

            try {
                cache.put(i, bytes);

                // Remove to reduce memory footprint of the test.
                cache.remove(i);
            } catch (ClusterGroupEmptyException e) {
                if (expectFailure) {
                    // The node has failed, which is expected, so some put is expected to fail.
                    break;
                } else {
                    if ("Topology projection is empty.".equals(e.getMessage()))
                        throw new AssertionError("Remote node seems to have died", e);

                    throw e;
                }
            }

            // Give timeout-based ack sending mechanisms some time to ack the message.
            Thread.sleep(MILLIS_BETWEEN_PUTS);
        }

        if (expectFailure) {
            assertTrue(
                "Remote node did not fail",
                waitForCondition(remoteNodeFailed::get, SECONDS.toMillis(10))
            );
        } else {
            assertFalse("Remote node failed, probably an OOM", remoteNodeFailed.get());
        }
    }

    /**
     * Listen for a remote node failure event and return an {@link AtomicBoolean} that will be set to {@code true}
     * when the remote node fails.
     *
     * @param localNode Node on which to listen.
     * @return An {@link AtomicBoolean} that will receive {@code true} when the remote node fails.
     */
    private static AtomicBoolean listenForRemoteNodeFailure(IgniteEx localNode) {
        AtomicBoolean remoteNodeFailed = new AtomicBoolean(false);

        localNode.events().localListen(event -> {
            DiscoveryEvent discoveryEvent = (DiscoveryEvent) event;

            if (REMOTE_NODE_NAME.equals(discoveryEvent.eventNode().attribute(ATTR_IGNITE_INSTANCE_NAME))) {
                remoteNodeFailed.set(true);
            }

            return false;
        }, EVT_NODE_FAILED);

        return remoteNodeFailed;
    }
}
