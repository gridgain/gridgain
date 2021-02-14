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
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

import java.io.Externalizable;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.UUID;

/**
 * Message to send statistics.
 */
public class StatisticsGatheringResponse implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    public static final short TYPE_CODE = 186;

    /** Collection id. */
    private UUID gatId;

    /** Request id. */
    private UUID reqId;

    /** Set of collected local object statistics. */
    @GridDirectCollection(StatisticsObjectData.class)
    private Collection<StatisticsObjectData> data;

    /** Array of included partitions. */
    private int[] parts;

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /**
     * {@link Externalizable} support.
     */
    public StatisticsGatheringResponse() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param gatId Gathering id.
     * @param reqId Request id.
     * @param data Set of objects statistics.
     * @param parts Array of included partitions.
     */
    public StatisticsGatheringResponse(UUID gatId, UUID reqId, Collection<StatisticsObjectData> data, int[] parts) {
        this.gatId = gatId;
        this.reqId = reqId;
        this.data = data;
        this.parts = parts;
    }

    /**
     * @return Gathering id.
     */
    public UUID gatId() {
        return gatId;
    }

    /**
     * @return Request id.
     */
    public UUID reqId() {
        return reqId;
    }

    /**
     * @return Set of object statistics.
     */
    public Collection<StatisticsObjectData> data() {
        return data;
    }

    /**
     * @return Array of included partitions.
     */
    public int[] parts() {
        return parts;
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
                if (!writer.writeCollection("data", data, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeUuid("gatId", gatId))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeIntArray("parts", parts))
                    return false;

                writer.incrementState();

            case 3:
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
                data = reader.readCollection("data", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                gatId = reader.readUuid("gatId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                parts = reader.readIntArray("parts");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 3:
                reqId = reader.readUuid("reqId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(StatisticsGatheringResponse.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE_CODE;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 4;
    }
}
