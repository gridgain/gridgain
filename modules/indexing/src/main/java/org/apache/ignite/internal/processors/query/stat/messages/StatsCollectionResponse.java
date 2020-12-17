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
import org.apache.ignite.internal.GridDirectMap;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

import java.io.Externalizable;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Message to send statistics.
 */
public class StatsCollectionResponse implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    public static final short TYPE_CODE = 186;

    /** Collection id. */
    private UUID colId;

    /** Request id. */
    private UUID reqId;

    /** Map of collected local object statistics with array of included partitions. */
    @GridDirectMap(keyType = StatsObjectData.class, valueType = int[].class)
    private Map<StatsObjectData, int[]> data;

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /**
     * {@link Externalizable} support.
     */
    public StatsCollectionResponse() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param colId Collection id.
     * @param reqId Request id.
     * @param data Map of objects statistics with array of included partitions.
     */
    public StatsCollectionResponse(UUID colId, UUID reqId, Map<StatsObjectData, int[]> data) {
        this.colId = colId;
        this.reqId = reqId;
        this.data = data;
    }

    /**
     * @return Collection id.
     */
    public UUID colId() {
        return colId;
    }

    /**
     * @return Request id.
     */
    public UUID reqId() {
        return reqId;
    }

    /**
     * @return Map of object statistics with array of included partitions.
     */
    public Map<StatsObjectData, int[]> data() {
        return data;
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
                if (!writer.writeUuid("colId", colId))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeMap("data", data, MessageCollectionItemType.MSG, MessageCollectionItemType.INT_ARR))
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
                colId = reader.readUuid("colId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                data = reader.readMap("data", MessageCollectionItemType.MSG, MessageCollectionItemType.INT_ARR, false);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                reqId = reader.readUuid("reqId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(StatsCollectionResponse.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE_CODE;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 3;
    }
}
