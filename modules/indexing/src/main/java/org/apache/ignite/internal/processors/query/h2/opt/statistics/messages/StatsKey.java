package org.apache.ignite.internal.processors.query.h2.opt.statistics.messages;

import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

import java.io.Externalizable;
import java.nio.ByteBuffer;
import java.util.List;

public class StatsKey implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    public static final short TYPE_CODE = 176;

    /** Schema name. */
    private String schemaName;

    /** Object (table or index) name. */
    private String objectName;

    /** Optional list of columns to collect statistics by.
     * Each string can contain list of comma separated columns to represent multicolumns stats. */
    @GridDirectCollection(String.class)
    private List<String> colNames;

    /**
     * {@link Externalizable} support.
     */
    public StatsKey() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param schemaName
     * @param objectName
     * @param colNames
     */
    public StatsKey(String schemaName, String objectName, List<String> colNames) {
        this.schemaName = schemaName;
        this.objectName = objectName;
        this.colNames = colNames;
    }

    public String schemaName() {
        return schemaName;
    }

    public String objectName() {
        return objectName;
    }

    public List<String> colNames() {
        return colNames;
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
                if (!writer.writeCollection("colNames", colNames, MessageCollectionItemType.STRING))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeString("objectName", objectName))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeString("schemaName", schemaName))
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
                colNames = reader.readCollection("colNames", MessageCollectionItemType.STRING);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                objectName = reader.readString("objectName");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                schemaName = reader.readString("schemaName");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(StatsKey.class);
    }

    @Override
    public short directType() {
        return TYPE_CODE;
    }

    @Override
    public byte fieldsCount() {
        return 3;
    }

    @Override
    public void onAckReceived() {

    }
}
