/*
 * Copyright 2021 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.spi.communication.tcp;

import java.util.Collections;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.IgniteSpiException;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of communication spi that can block certain messages.
 */
public class BlockTcpCommunicationSpi extends TcpCommunicationSpi {
    /** Set of message classes that are being blocked */
    Set<Class> msgClasses = Collections.newSetFromMap(new ConcurrentHashMap<>());

    /** Queue of blocked messages that will be sent when block is cancalled.*/
    private final Queue<T3<ClusterNode, Message, IgniteInClosure>> queue = new ConcurrentLinkedQueue<>();

    /** */
    @LoggerResource
    private IgniteLogger log;

    /** {@inheritDoc} */
    @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC)
        throws IgniteSpiException {

        if (msg instanceof GridIoMessage && msgClasses.contains(((GridIoMessage)msg).message().getClass())) {
            log.info("Block message: " + msg);

            queue.add(new T3<>(node, msg, ackC));

            return;
        }

        super.sendMessage(node, msg, ackC);
    }

    /**
     * @param clazz Class of messages which will be block.
     */
    public void blockMessage(Class clazz) {
        msgClasses.add(clazz);
    }

    /**
     * Unlock message.
     */
    public void unblockMessage(Class clazz) {
        msgClasses.remove(clazz);
    }

    /**
     * Unlock all messages.
     */
    public void unblockAllMessages() {
        msgClasses.clear();

        for (T3<ClusterNode, Message, IgniteInClosure> msg : queue)
            super.sendMessage(msg.get1(), msg.get2(), msg.get3());

        queue.clear();
    }

    /**
     * @param ignite Server node
     * @return BlockTcpCommunicationSpi of the server node or null if other implementation of CommunicationSpi is used.
     */
    @Nullable public static BlockTcpCommunicationSpi commSpi(Ignite ignite) {
        return ((BlockTcpCommunicationSpi)ignite.configuration().getCommunicationSpi());
    }
}
