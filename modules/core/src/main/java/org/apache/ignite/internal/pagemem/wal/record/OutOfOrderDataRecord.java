package org.apache.ignite.internal.pagemem.wal.record;

import java.util.Collections;
import java.util.List;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

public class OutOfOrderDataRecord extends DataRecord {
    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.OUT_OF_ORDER_UPDATE;
    }

    /**
     * @param writeEntry Write entry.
     */
    public OutOfOrderDataRecord(DataEntry writeEntry) {
        super(writeEntry, U.currentTimeMillis());
    }

//    /**
//     * @param writeEntries Write entries.
//     */
//    public MvccDataRecord(List<DataEntry> writeEntries) {
//        this(writeEntries, U.currentTimeMillis());
//    }

    /**
     * @param writeEntry Write entry.
     */
    public OutOfOrderDataRecord(MvccDataEntry writeEntry, long timestamp) {
        super(Collections.singletonList(writeEntry), timestamp);
    }

    /**
     * @param writeEntries Write entries.
     * @param timestamp TimeStamp.
     */
    public OutOfOrderDataRecord(List<DataEntry> writeEntries, long timestamp) {
        super(writeEntries, timestamp);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(OutOfOrderDataRecord.class, this, "super", super.toString());
    }
}
