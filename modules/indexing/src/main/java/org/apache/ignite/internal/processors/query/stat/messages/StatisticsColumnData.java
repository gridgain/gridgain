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

import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2ValueMessage;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

import java.nio.ByteBuffer;

/**
 * Statistics by column (or by set of columns, if they collected together)
 */
public class StatisticsColumnData implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    public static final short TYPE_CODE = 185;

    /** Min value in column. */
    private GridH2ValueMessage min;

    /** Max value in column. */
    private GridH2ValueMessage max;

    /** Percent of null values in column. */
    private int nulls;

    /** Percent of distinct values in column (except nulls). */
    private int cardinality;

    /** Total vals in column. */
    private long total;

    /** Average size, for variable size values (in bytes). */
    private int size;

    /** Raw data. */
    private byte[] rawData;

    /**
     * Default constructor.
     */
    public StatisticsColumnData() {
    }

    /**
     * Constructor.
     *
     * @param min Min value in column.
     * @param max Max value in column.
     * @param nulls Percent of null values in column.
     * @param cardinality Percent of distinct values in column.
     * @param total Total values in column.
     * @param size Average size, for variable size types (in bytes).
     * @param rawData Raw data to make statistics aggregate.
     */
    public StatisticsColumnData(
        GridH2ValueMessage min,
        GridH2ValueMessage max,
        int nulls,
        int cardinality,
        long total,
        int size,
        byte[] rawData
    ) {
        this.min = min;
        this.max = max;
        this.nulls = nulls;
        this.cardinality = cardinality;
        this.total = total;
        this.size = size;
        this.rawData = rawData;
    }

    /**
     * @return Min value in column.
     */
    public GridH2ValueMessage min() {
        return min;
    }

    /**
     * @return Max value in column.
     */
    public GridH2ValueMessage max() {
        return max;
    }

    /**
     * @return Percent of null values in column.
     */
    public int nulls() {
        return nulls;
    }

    /**
     * @return Percent of distinct values in column.
     */
    public int cardinality() {
        return cardinality;
    }

    /**
     * @return Total values in column.
     */
    public long total() {
        return total;
    }

    /**
     * @return Average size, for variable size types (in bytes).
     */
    public int size() {
        return size;
    }

    /**
     * @return Raw data.
     */
    public byte[] rawData() {
        return rawData;
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
                if (!writer.writeInt("cardinality", cardinality))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeMessage("max", max))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeMessage("min", min))
                    return false;

                writer.incrementState();

            case 3:
                if (!writer.writeInt("nulls", nulls))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeByteArray("rawData", rawData))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeInt("size", size))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeLong("total", total))
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
                cardinality = reader.readInt("cardinality");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                max = reader.readMessage("max");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                min = reader.readMessage("min");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 3:
                nulls = reader.readInt("nulls");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                rawData = reader.readByteArray("rawData");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                size = reader.readInt("size");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                total = reader.readLong("total");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(StatisticsColumnData.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE_CODE;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 7;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {

    }
}
