/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.metric.export;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import java.util.function.BiConsumer;

import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.util.GridUnsafe.BYTE_ARR_OFF;
import static org.apache.ignite.internal.util.GridUnsafe.copyMemory;
import static org.apache.ignite.internal.util.GridUnsafe.putInt;
import static org.apache.ignite.internal.util.GridUnsafe.putLong;
import static org.apache.ignite.internal.util.GridUnsafe.putShort;

/**
 * Compact data structure.
 *
 * Message format: header | [schema] | [data]
 *
 * Header:
 *  0 - int - message size in bytes.
 *  4 - short - version of protocol.
 *  6 - int - version of schema.
 * 10 - int - offset of schema frame (0xFFFFFFFF if no schema frame in message).
 * 14 - int - size of schema frame in bytes (0 if no schema frame in message).
 * 18 - int - offset of data frame (0xFFFFFFFF if no data farame in message).
 * 22 - int - size of data frame in bytes (0 if no data in message).
 * 26 - UUID - cluster ID (two long values: most significant bits, then least significant bits).
 * 42 - int - size of user tag in bytes (0 if user tag isn't defined).
 * 46 - byte[] - user tag.
 * 46 + user tag size - int - size of consistent ID in bytes.
 * 46 + user tag size + 4 - byte[] - consistent ID ()
 *
 * Schema:
 * 0 - byte - particular metirc type.
 * 1 - int - size of particular metric name in bytes.
 * 5 - bytes - particular metric name
 *
 * Data:
 * Accordingly to schema.
 *
 */
public class MetricResponse implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** Protocol version 1. */
    private static final short PROTO_VER_1 = 1;

    /** Header size without user tag bytes. */
    static final int BASE_HEADER_SIZE = 46;

    /** Message size field offset. */
    private static final int MSG_SIZE_OFF = 0;

    /** Protocol version field offset. */
    private static final int PROTO_VER_OFF = 4;

    /** Schema version field offset. */
    private static final int SCHEMA_VER_OFF = 6;

    /** Schema offset field offset. */
    private static final int SCHEMA_OFF_OFF = 10;

    /** Schema size field offset. */
    private static final int SCHEMA_SIZE_OFF = 14;

    /** Data offset field offset. */
    private static final int DATA_OFF_OFF = 18;

    /** Data size field offset. */
    private static final int DATA_SIZE_OFF = 22;

    /** Cluster ID field offset. */
    private static final int CLUSTER_ID_OFF = 26;

    /** User tag size field offset. */
    private static final int USER_TAG_SIZE_OFF = 42;

    /** User tag field offset. */
    private static final int USER_TAG_OFF = 46;

    /** Constant for indication of absent some block of data. */
    private static final int NO_OFF = -1;

    /** Message body. */
    byte[] body;

    /**
     * Default constructor.
     */
    public MetricResponse() {
        // No-op.
    }

    public MetricResponse(
            int schemaVer,
            UUID clusterId,
            @Nullable String userTag,
            String consistentId,
            int schemaSize,
            int dataSize,
            BiConsumer<byte[], Integer> schemaWriter,
            BiConsumer<byte[], Integer> dataWriter
    ) {
        byte[] userTagBytes = null;

        int userTagLen = 0;

        if (userTag != null && !userTag.isEmpty()) {
            userTagBytes = userTag.getBytes(Schema.UTF_8);

            userTagLen = userTagBytes.length;
        }

        byte[] consistentIdBytes = consistentId.getBytes(Schema.UTF_8);

        int len = BASE_HEADER_SIZE + userTagLen + Integer.BYTES + consistentIdBytes.length + schemaSize + dataSize;

        int schemaOff = BASE_HEADER_SIZE + userTagLen + Integer.BYTES + consistentIdBytes.length;

        int dataOff = schemaOff + schemaSize;

        body = new byte[len];

        header(schemaVer, clusterId, userTagBytes, consistentIdBytes, schemaOff, schemaSize, dataOff, dataSize);

        if (schemaOff > -1)
            schemaWriter.accept(body, schemaOff);

        if (dataOff > -1)
            dataWriter.accept(body, dataOff);
    }

    public int size() {
        return GridUnsafe.getInt(body, BYTE_ARR_OFF + MSG_SIZE_OFF);
    }

    public short protocolVersion() {
        return GridUnsafe.getShort(body, BYTE_ARR_OFF + PROTO_VER_OFF);
    }

    public int schemaVersion() {
        return GridUnsafe.getInt(body, BYTE_ARR_OFF + SCHEMA_VER_OFF);
    }

    //TODO: could be null?
    public UUID clusterId() {
        long mostSigBits = GridUnsafe.getLong(body, BYTE_ARR_OFF + CLUSTER_ID_OFF);

        long leastSigBits = GridUnsafe.getLong(body, BYTE_ARR_OFF + CLUSTER_ID_OFF + Long.BYTES);

        return new UUID(mostSigBits, leastSigBits);
    }

    @Nullable public String userTag() {
        int len = userTagSize();

        if (len == 0)
            return null;

        return new String(body, USER_TAG_OFF, len, Schema.UTF_8);
    }

    private int userTagSize() {
        return GridUnsafe.getInt(body, BYTE_ARR_OFF + USER_TAG_SIZE_OFF);
    }

    public String consistentId() {
        int consistentIdSizeOff = BASE_HEADER_SIZE + userTagSize();

        int len = GridUnsafe.getInt(body, BYTE_ARR_OFF + consistentIdSizeOff);

        int off = consistentIdSizeOff + Integer.BYTES;

        return new String(body, off, len, Schema.UTF_8);
    }

    @Nullable public Schema schema() {
        int off = schemaOffset();

        if (off == NO_OFF)
            return null;

        return Schema.fromBytes(body, off, schemaSize());
    }

    public void processData(Schema schema, MetricValueConsumer consumer) {
        int off = dataOffset();

        if (off == NO_OFF)
            return;

        int lim = off + dataSize();

        List<SchemaItem> items = schema.items();

        for (SchemaItem item : items) {
            assert off < lim;

            String name = item.name();

            byte type = item.type();

            switch (type) {
                case 0:
                    consumer.onBoolean(name, GridUnsafe.getBoolean(body, BYTE_ARR_OFF + off));

                    off += 1;

                    break;

                case 1:
                    consumer.onInt(name, GridUnsafe.getInt(body, BYTE_ARR_OFF + off));

                    off += Integer.BYTES;

                    break;

                case 2:
                    consumer.onLong(name, GridUnsafe.getLong(body, BYTE_ARR_OFF + off));

                    off += Long.BYTES;

                    break;

                case 3:
                    consumer.onDouble(name, GridUnsafe.getDouble(body, BYTE_ARR_OFF + off));

                    off += Double.BYTES;

                    break;

                default:
                    throw new IllegalArgumentException("Unknown metric type: " + type);
            }
        }
    }

    public int schemaOffset() {
        return GridUnsafe.getInt(body, BYTE_ARR_OFF + SCHEMA_OFF_OFF);
    }

    public int schemaSize() {
        return GridUnsafe.getInt(body, BYTE_ARR_OFF + SCHEMA_SIZE_OFF);
    }

    public int dataOffset() {
        return GridUnsafe.getInt(body, BYTE_ARR_OFF + DATA_OFF_OFF);
    }

    public int dataSize() {
        return GridUnsafe.getInt(body, BYTE_ARR_OFF + DATA_SIZE_OFF);
    }

    private void header(
            int schemaVer,
            UUID clusterId,
            byte[] userTagBytes,
            byte[] consistentIdBytes,
            int schemaOff,
            int schemaSize,
            int dataOff,
            int dataSize
    ) {
        putInt(body, BYTE_ARR_OFF + MSG_SIZE_OFF, body.length);

        putShort(body, BYTE_ARR_OFF + PROTO_VER_OFF, PROTO_VER_1);

        putInt(body, BYTE_ARR_OFF + SCHEMA_VER_OFF, schemaVer);

        putInt(body, BYTE_ARR_OFF + SCHEMA_OFF_OFF, schemaOff);

        putInt(body, BYTE_ARR_OFF + SCHEMA_SIZE_OFF, schemaSize);

        putInt(body, BYTE_ARR_OFF + DATA_OFF_OFF, dataOff);

        putInt(body, BYTE_ARR_OFF + DATA_SIZE_OFF, dataSize);

        putLong(body, BYTE_ARR_OFF + CLUSTER_ID_OFF, clusterId.getMostSignificantBits());

        putLong(body, BYTE_ARR_OFF + CLUSTER_ID_OFF + Long.BYTES, clusterId.getLeastSignificantBits());

        int off = BASE_HEADER_SIZE;

        if (userTagBytes != null) {
            putInt(body, BYTE_ARR_OFF + USER_TAG_SIZE_OFF, userTagBytes.length);

            copyMemory(userTagBytes, BYTE_ARR_OFF, body, BYTE_ARR_OFF + off, userTagBytes.length);

            off += userTagBytes.length;
        }
        else
            putInt(body, BYTE_ARR_OFF + USER_TAG_SIZE_OFF, 0);

        putInt(body, BYTE_ARR_OFF + off, consistentIdBytes.length);

        off += Integer.BYTES;

        copyMemory(consistentIdBytes, BYTE_ARR_OFF, body, BYTE_ARR_OFF + off, consistentIdBytes.length);
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
                if (!writer.writeByteArray("body", body))
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
                body = reader.readByteArray("body");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(MetricResponse.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -62;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /**
     * @return Raw bytes.
     */
    public byte[] body() {
        return body;
    }
}
