package org.apache.ignite.internal.processors.query.stat.messages;

import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

import java.io.Externalizable;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Key, describing the object of statistics. For example: table with some columns.
 */
public class StatisticsKeyMessage implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    public static final short TYPE_CODE = 183;

    /** Object schema. */
    private String schema;

    /** Object name. */
    private String obj;

    /** Optional list of columns to collect statistics by.
     * Each string can contain list of comma separated columns to represent multicolumn stats. */
    @GridDirectCollection(String.class)
    private List<String> colNames;

    /**
     * {@link Externalizable} support.
     */
    public StatisticsKeyMessage() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param schema Schema name.
     * @param obj Object name.
     * @param colNames Column names.
     */
    public StatisticsKeyMessage(String schema, String obj, List<String> colNames) {
        this.schema = schema;
        this.obj = obj;
        this.colNames = colNames;
    }

    /**
     * @return Schema name.
     */
    public String schema() {
        return schema;
    }

    /**
     * @return Object name.
     */
    public String obj() {
        return obj;
    }

    /**
     * @return Column names.
     */
    public List<String> colNames() {
        return colNames;
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
                if (!writer.writeCollection("colNames", colNames, MessageCollectionItemType.STRING))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeString("obj", obj))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeString("schema", schema))
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
                colNames = reader.readCollection("colNames", MessageCollectionItemType.STRING);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                obj = reader.readString("obj");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                schema = reader.readString("schema");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(StatisticsKeyMessage.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE_CODE;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {

    }
}
