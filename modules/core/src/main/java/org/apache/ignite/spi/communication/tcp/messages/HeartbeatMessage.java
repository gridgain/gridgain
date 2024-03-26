package org.apache.ignite.spi.communication.tcp.messages;

import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

public class HeartbeatMessage implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** Message body size in bytes. */
    static final int MESSAGE_SIZE = 8;

    /** Full message size (with message type) in bytes. */
    private static final int MESSAGE_FULL_SIZE = MESSAGE_SIZE + DIRECT_TYPE_SIZE;

    // TODO public for debug; remove
    public long timestamp;

    // TODO remove
    private static AtomicLong counter = new AtomicLong(1);

    public HeartbeatMessage() {
        this.timestamp = counter.getAndIncrement();
    }

    /** {@inheritDoc} */
    @Override
    public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        if (buf.remaining() < MESSAGE_FULL_SIZE)
            return false;

        TcpCommunicationSpi.writeMessageType(buf, directType());

        buf.putLong(timestamp);

        return true;
    }

    /** {@inheritDoc} */
    @Override
    public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        if (buf.remaining() < MESSAGE_SIZE)
            return false;

        timestamp = buf.getLong();

        return true;
    }

    /** {@inheritDoc} */
    @Override
    public short directType() {
        return TcpCommunicationSpi.HEARTBEAT_MSG_TYPE;
    }

    /** {@inheritDoc} */
    @Override
    public byte fieldsCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override
    public void onAckReceived() {
        // No-op.
    }

    @Override
    public String toString() {
        return "HeartBeatMessage{" +
                "timestamp=" + timestamp +
                '}';
    }
}
