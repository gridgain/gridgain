package org.apache.ignite.spi.communication.tcp.messages;

import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;

import java.nio.ByteBuffer;

public class HeartbeatAckMessage implements Message {
    private static final long serialVersionUID = 0L;

    /** Message body size in bytes. */
    static final int MESSAGE_SIZE = 8;

    /** Full message size (with message type) in bytes. */
    private static final int MESSAGE_FULL_SIZE = MESSAGE_SIZE + DIRECT_TYPE_SIZE;

    private long timestamp;

    public HeartbeatAckMessage() {
    }

    public HeartbeatAckMessage(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        if (buf.remaining() < MESSAGE_FULL_SIZE)
            return false;

        TcpCommunicationSpi.writeMessageType(buf, directType());

        buf.putLong(timestamp);

        return true;
    }

    @Override
    public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        if (buf.remaining() < MESSAGE_SIZE)
            return false;

        timestamp = buf.getLong();

        return true;
    }

    @Override
    public short directType() {
        return TcpCommunicationSpi.HEARTBEAT_ACK_MSG_TYPE;
    }

    @Override
    public byte fieldsCount() {
        return 1;
    }

    @Override
    public void onAckReceived() {
        // No-op.
    }

    @Override
    public String toString() {
        return "HeartbeatAckMessage{" +
                "timestamp=" + timestamp +
                '}';
    }
}
