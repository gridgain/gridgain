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
package org.apache.ignite.testframework.discovery;

import java.util.concurrent.CountDownLatch;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.hamcrest.Matcher;

import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.junit.Assert.assertTrue;

/**
 * Test discovery SPI which provide ability to block discovery message by condition and collect some messages via {@link
 * TestMessageCollectStatistics}. Also it is possible to trigger execution fails({@link #failIfBlocked()}) when blocking
 * happens.
 *
 * Usage:
 *
 * <p>* {@link #blockIf(Matcher)} for  registration of  condition for blocking.</p>
 *
 * <p>* {@link #awaitBlocking} for awaiting of event of blocking.</p>
 *
 * <p>* {@link #release()} for continue the execution.</p>
 *
 * <p>* {@link #failIfBlocked()} for run failure if blocking happens.</p>
 */
public class BlockedDiscoverySpi extends TcpDiscoverySpi {
    /** Condition for blocking message. */
    private volatile Matcher<TcpDiscoveryAbstractMessage> predicate;

    /** Latch for blocking execution. */
    private volatile CountDownLatch latch;

    /** A sign of a run failure after blocking. */
    private volatile boolean fail = false;

    /** */
    public BlockedDiscoverySpi() {
        super(new TestMessageCollectStatistics());
    }

    /** {@inheritDoc} */
    @Override protected void startMessageProcess(TcpDiscoveryAbstractMessage msg) {
        Matcher<TcpDiscoveryAbstractMessage> predicate = this.predicate;

        if (predicate != null) {
            if (predicate.matches(msg)) {
                this.predicate = null;

                try {
                    latch.await();
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }

                if (fail) {
                    this.fail = false;

                    throw new RuntimeException();
                }
            }
        }
    }

    /**
     * Register condition by which execution of message would be blocked.
     *
     * @param predicate Condition for blocking.
     * @throws IgniteInterruptedCheckedException If fail.
     */
    public void blockIf(Matcher<TcpDiscoveryAbstractMessage> predicate) throws IgniteInterruptedCheckedException {
        assertTrue(latch == null || latch.getCount() == 0);

        latch = new CountDownLatch(1);

        this.predicate = predicate;
    }

    /**
     * Awaiting the event of blocking. It will fail if blocking doesn't happen for some time.
     *
     * @throws IgniteInterruptedCheckedException If fail.
     */
    public void awaitBlocking() throws IgniteInterruptedCheckedException {
        assertTrue(waitForCondition(() -> this.predicate == null, 10_000));
    }

    /**
     * Trigger to continue to handle the message.
     */
    public void release() {
        latch.countDown();
    }

    /**
     * Message execution will be failed if it is blocked by the registered condition.
     */
    public void failIfBlocked() {
        this.fail = true;

        release();
    }

    /**
     * @return All messages processing of which was started. Processing means only started of message handle but not a
     * call of listeners.
     */
    public Iterable<TcpDiscoveryAbstractMessage> startProcessingMessages() {
        return stat().startProcessingMessages;
    }

    /**
     * @return All messages which would send to next node.
     */
    public Iterable<TcpDiscoveryAbstractMessage> sentMessages() {
        return stat().sentMessages;
    }

    /**
     * @return All messages which were read from the socket from the previous node.
     */
    public Iterable<TcpDiscoveryAbstractMessage> receivedMessages() {
        return stat().receivedMessages;
    }

    /** */
    TestMessageCollectStatistics stat() {
        return (TestMessageCollectStatistics)stats;
    }

}
