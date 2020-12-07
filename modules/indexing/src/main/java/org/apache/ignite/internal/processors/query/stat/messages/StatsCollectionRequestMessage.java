/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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
package org.apache.ignite.internal.processors.query.stat.messages;

import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

import java.io.Externalizable;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;

/**
 * Request to collect statistics message.
 */
public class StatsCollectionRequestMessage implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    public static final short TYPE_CODE = 179;

    /** Request id. */
    private UUID reqId;

    /** Is request is local (need to collect only local statistics) or global (need to start cluster wide process). */
    private boolean loc;

    /** Keys to collect statistics by. */
    @GridDirectCollection(StatsKeyMessage.class)
    private List<StatsKeyMessage> keys;

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
     * @param loc if {@code true} - need to collect statistics only by local primary partitions,
     *              otherwise need starts cluster wide statistics collection.
     * @param keys keys what statistics should be collected.
     */
    public StatsCollectionRequestMessage(UUID reqId, boolean loc, List<StatsKeyMessage> keys) {
        this.reqId = reqId;
        this.loc = loc;
        this.keys = keys;
    }

    /**
     * @return request id.
     */
    public UUID reqId() {
        return reqId;
    }

    /**
     * @return local collection flag.
     */
    public boolean local() {
        return loc;
    }

    /**
     * @return List of keys to collect statistics by.
     */
    public List<StatsKeyMessage> keys() {
        return keys;
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
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
                if (!writer.writeBoolean("local", loc))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeUuid("reqId", reqId))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
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
                loc = reader.readBoolean("local");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                reqId = reader.readUuid("reqId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(StatsCollectionRequestMessage.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public byte policy() {
        return GridIoPolicy.QUERY_POOL;
    }
}
