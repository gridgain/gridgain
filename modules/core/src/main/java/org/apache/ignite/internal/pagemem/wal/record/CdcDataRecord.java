/*
 * Copyright 2023 GridGain Systems, Inc. and Contributors.
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

import java.util.List;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * The record to forcefully resend cache data to the CDC application.
 */
public class CdcDataRecord extends DataRecord {
    /** */
    public CdcDataRecord(DataEntry writeEntry) {
        super(writeEntry);
    }

    /** */
    public CdcDataRecord(List<DataEntry> writeEntries) {
        super(writeEntries);
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.CDC_DATA_RECORD;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CdcDataRecord.class, this, "super", super.toString());
    }
}
