package org.apache.ignite.internal.processors.query.h2.opt.statistics.messages;

import org.apache.ignite.internal.processors.query.h2.opt.statistics.StatsType;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2ValueMessage;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

import java.io.Externalizable;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Statistics for some object (index or table) in database,
 */
public class StatsObjectData implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    public static final short TYPE_CODE = 178;

    /** Name of objects schema. */
    public String schemaName;

    /** Name of object. */
    public String objectName;

    /** Total row count in current object. */
    public long rowsCnt;

    /** Type of statistics. */
    public StatsType type;

    /** Partition id if statistics was collected by partition. */
    public int partId;

    /** Update counter if statistics was collected by partition. */
    public long updateCounter;

    /** Columns key to statistic map. */
    public Map<String, StatsColumnData> data;

    /**
     * {@link Externalizable} support.
     */
    public StatsObjectData() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param schemaName schema name
     * @param objectName object (table or index) name
     * @param rowsCnt
     * @param data map of column name to
     */
    public StatsObjectData(String schemaName, String objectName, long rowsCnt, Map<String, StatsColumnData> data) {
        this.schemaName = schemaName;
        this.objectName = objectName;
        this.data = data;
    }

    /**
     * Constructor.
     *
     * @param schemaName
     * @param objectName
     * @param rowsCnt
     * @param type
     * @param partId
     * @param updateCounter
     * @param data
     */
    public StatsObjectData(String schemaName,
                           String objectName,
                           long rowsCnt,
                           StatsType type,
                           int partId,
                           long updateCounter,
                           Map<String, StatsColumnData> data) {
        this.schemaName = schemaName;
        this.objectName = objectName;
        this.rowsCnt = rowsCnt;
        this.type = type;
        this.partId = partId;
        this.updateCounter = updateCounter;
        this.data = data;
    }

    @Override
    public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        return false;
    }

    @Override
    public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        return false;
    }

    @Override
    public short directType() {
        return TYPE_CODE;
    }

    @Override
    public byte fieldsCount() {
        return 0;
    }

    @Override
    public void onAckReceived() {

    }
}
