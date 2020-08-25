package org.apache.ignite.internal.processors.query.h2.opt.statistics.messages;

import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2ValueMessage;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

import java.io.Externalizable;
import java.nio.ByteBuffer;


// TODO: due to GridDirectCollection limitation it possibly to remove StatsColumnData at all and use StatsColumnRawData
public class StatsColumnData implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    public static final short TYPE_CODE = 177;

    /** Min value in column. */
    private GridH2ValueMessage min;

    /** Max value in column. */
    private GridH2ValueMessage max;

    /** Percent of null values in column. */
    private int nulls;

    /** Percent of distinct values in column (except nulls). */
    private int cardinality;

    /** TBD */
    private byte[] rawData;

    /**
     * {@link Externalizable} support.
     */
    public StatsColumnData() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param min min value in column.
     * @param max max value in column.
     * @param nulls percent of null values in column.
     * @param cardinality percent of distinct values in column.
     * @param rawData raw data to make statistics agпregadate.
     */
    public StatsColumnData(GridH2ValueMessage min, GridH2ValueMessage max, int nulls, int cardinality, byte[] rawData) {
        this.min = min;
        this.max = max;
        this.nulls = nulls;
        this.cardinality = cardinality;
        this.rawData = rawData;
    }

    /**
     * @return
     */
    public GridH2ValueMessage min() {
        return min;
    }

    /**
     * @return
     */
    public GridH2ValueMessage max() {
        return max;
    }

    /**
     * @return
     */
    public int nulls() {
        return nulls;
    }

    /**
     * @return
     */
    public int cardinality() {
        return cardinality;
    }


    /**
     * @return
     */
    public byte[] rawData() {
        return rawData;
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
                if (!writer.writeMessage("max", max))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeMessage("min", min))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeInt("nulls", nulls))
                    return false;

                writer.incrementState();

            case 3:
                if (!writer.writeByteArray("rawData", rawData))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeInt("selectivity", cardinality))
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
                max = reader.readMessage("max");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                min = reader.readMessage("min");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                nulls = reader.readInt("nulls");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 3:
                rawData = reader.readByteArray("rawData");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                cardinality = reader.readInt("selectivity");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(StatsColumnData.class);
    }

    @Override
    public short directType() {
        return TYPE_CODE;
    }

    @Override
    public byte fieldsCount() {
        return 5;
    }

    @Override
    public void onAckReceived() {

    }
}
