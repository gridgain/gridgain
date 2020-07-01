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

package org.apache.ignite.internal.pagemem.wal.record;

import java.util.Collections;
import java.util.List;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Represents out-of-order update of atomic cache.
 * Atomic cache protocol allows applying updates in a different order on backup nodes,
 * i.e. the update with greater update counter can be logged in WAL before the update with smaller update counter.
 * In this case the second update can be just ignored.
 */
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

    /**
     * @param writeEntries Write entries.
     */
    public OutOfOrderDataRecord(List<DataEntry> writeEntries) {
        this(writeEntries, U.currentTimeMillis());
    }

    /**
     * @param writeEntry Write entry.
     */
    public OutOfOrderDataRecord(MvccDataEntry writeEntry, long ts) {
        super(Collections.singletonList(writeEntry), ts);
    }

    /**
     * @param writeEntries Write entries.
     * @param ts TimeStamp.
     */
    public OutOfOrderDataRecord(List<DataEntry> writeEntries, long ts) {
        super(writeEntries, ts);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(OutOfOrderDataRecord.class, this, "super", super.toString());
    }
}
