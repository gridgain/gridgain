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

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryStatistics;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;

/**
 * Implementation of {@link TcpDiscoveryStatistics} for test purpose to collect some messages for further usage.
 */
public class TestMessageCollectStatistics extends TcpDiscoveryStatistics {
    /**
     * All messages processing of which was started. Processing means only started of message handle but not a call of
     * listeners.
     */
    final Queue<TcpDiscoveryAbstractMessage> startProcessingMessages = new ConcurrentLinkedQueue<>();

    /** All messages which would send to next node. */
    final Queue<TcpDiscoveryAbstractMessage> sentMessages = new ConcurrentLinkedQueue<>();

    /** All messages which were read from the socket from the previous node. */
    final Queue<TcpDiscoveryAbstractMessage> receivedMessages = new ConcurrentLinkedQueue<>();

    /** {@inheritDoc} */
    @Override public synchronized void onMessageProcessingStarted(TcpDiscoveryAbstractMessage msg) {
        super.onMessageProcessingStarted(msg);

        startProcessingMessages.add(msg);
    }

    /** {@inheritDoc} */
    @Override public synchronized void onMessageSent(TcpDiscoveryAbstractMessage msg, long time) {
        super.onMessageSent(msg, time);

        sentMessages.add(msg);
    }

    /** {@inheritDoc} */
    @Override public synchronized void onMessageReceived(TcpDiscoveryAbstractMessage msg) {
        super.onMessageReceived(msg);

        receivedMessages.add(msg);
    }
}
