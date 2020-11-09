package org.apache.ignite.internal.processors.query.h2.opt.statistics.messages;

import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

import java.io.Externalizable;
import java.nio.ByteBuffer;
import java.util.List;

public class StatsCollectionRequestMessage implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    public static final short TYPE_CODE = 179;

    /** Request id. */
    private long reqId;

    @GridDirectCollection(StatsKey.class)
    private List<StatsKey> keys;

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    @Override
    public byte policy() {
        return GridIoPolicy.QUERY_POOL;
    }

    /**
     * {@link Externalizable} support.
     */
    public StatsCollectionRequestMessage() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param reqId id of request.
     * @param keys keys what statistics should be collected.
     */
    public StatsCollectionRequestMessage(long reqId, List<StatsKey> keys) {
        this.reqId = reqId;
        this.keys = keys;
    }

    /**
     * @return request id.
     */
    public long reqId() {
        return reqId;
    }

    /**
     * @return List of keys to collect statistics by.
     */
    public List<StatsKey> keys() {
        return keys;
    }

    @Override
    public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeCollection("keys", keys, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeLong("reqId", reqId))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    @Override
    public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        if (!reader.beforeMessageRead())
            return false;

        switch (reader.state()) {
            case 0:
                keys = reader.readCollection("keys", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                reqId = reader.readLong("reqId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(StatsCollectionRequestMessage.class);
    }

    @Override
    public short directType() {
        return TYPE_CODE;
    }

    @Override
    public byte fieldsCount() {
        return 2;
    }
}
