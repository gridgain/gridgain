package org.apache.ignite.internal.processors.query.h2.opt.statistics.messages;

import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

import java.nio.ByteBuffer;
import java.util.List;

public class StatsRequestMessage implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    public static final short TYPE_CODE = 182;

    /** Request id. */
    private long reqId;

    /** List of key to collect statistics by. */
    @GridDirectCollection(StatsKey.class)
    private List<StatsKey> keys;

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param reqId
     * @param keys
     */
    public StatsRequestMessage(long reqId, List<StatsKey> keys) {
        this.reqId = reqId;
        this.keys = keys;
    }

    /**
     * @return
     */
    public long reqId() {
        return reqId;
    }

    /**
     * @return
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

        return reader.afterMessageRead(StatsRequestMessage.class);
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
