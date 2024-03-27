/*
 * Copyright 2024 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.spi.communication.tcp.messages;

import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Heartbeat message for TCP communication SPI.
 */
public class HeartbeatMessage implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** Message timestamp counter for debug. */
    private static final AtomicLong counter = new AtomicLong(1); // TODO remove

    /** Message body size in bytes. */
    static final int MESSAGE_SIZE = 8;

    /** Full message size (with message type) in bytes. */
    private static final int MESSAGE_FULL_SIZE = MESSAGE_SIZE + DIRECT_TYPE_SIZE;

    /** Message timestamp. */
    private long timestamp; // TODO public for debug; remove

    /** */
    public HeartbeatMessage() {
        this.timestamp = counter.getAndIncrement();
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        if (buf.remaining() < MESSAGE_FULL_SIZE)
            return false;

        TcpCommunicationSpi.writeMessageType(buf, directType());

        buf.putLong(timestamp);

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        if (buf.remaining() < MESSAGE_SIZE)
            return false;

        timestamp = buf.getLong();

        return true;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TcpCommunicationSpi.HEARTBEAT_MSG_TYPE;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override public String toString() {
        return S.toString(HeartbeatMessage.class, this);
    }
}
