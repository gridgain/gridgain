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

import org.apache.ignite.internal.GridDirectMap;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
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
 * Request to collect statistics message.
 */
public class StatsCollectionRequest implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    public static final short TYPE_CODE = 179;

    /** Collection id. */
    private UUID colId;

    /** Request id. */
    private UUID reqId;

    /** Keys to partitions to collect statistics by. */
    @GridDirectMap(keyType = StatsKeyMessage.class, valueType = int[].class)
    private Map<StatsKeyMessage, int[]> keys;

    /**
     * {@link Externalizable} support.
     */
    public StatsCollectionRequest() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param colId Collection id.
     * @param reqId Request id.
     * @param keys Keys to partitions to collect statistics by.
     */
    public StatsCollectionRequest(UUID colId, UUID reqId, Map<StatsKeyMessage, int[]> keys) {
        this.colId = colId;
        this.reqId = reqId;
        this.keys = keys;
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
     * @return Map of keys to partitions to collect statistics by.
     */
    public Map<StatsKeyMessage, int[]> keys() {
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
                if (!writer.writeUuid("colId", colId))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeMap("keys", keys, MessageCollectionItemType.MSG, MessageCollectionItemType.INT_ARR))
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
                keys = reader.readMap("keys", MessageCollectionItemType.MSG, MessageCollectionItemType.INT_ARR, false);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                reqId = reader.readUuid("reqId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(StatsCollectionRequest.class);
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
