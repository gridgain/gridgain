/*
 * Copyright 2024 GridGain Systems, Inc. and Contributors.
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

import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * This record represents a pointer to a consistent cut fuzzy border or actual cut border with a timestamp.
 */
public class TimeStampedConsistentCutRecord extends TimeStampRecord {
    /** Represents a placeholder for a snapshot identifier when the record is created outside of snapshot scope. */
    public static final long UNDEFINED_SNAPSHOT_ID = -1;

    /** Optional field that contains snapshot identifier in the case when this WAL record is created as a part of exchangeless snapshot. */
    private long snapshotId;

    /**
     * Creates a new instance of consistent cut record initializing the timestamp with the current time using {@link U#currentTimeMillis()}.
     * Snapshot identifier is set to {@link #UNDEFINED_SNAPSHOT_ID}.
     */
    public TimeStampedConsistentCutRecord() {
        this(UNDEFINED_SNAPSHOT_ID, U.currentTimeMillis());
    }

    /**
     * Creates a new instance of consistent cut record using the provided {@code snaspshotId} and timestamp set to the current time
     * using {@link U#currentTimeMillis()}.
     *
     * @param snapshotId Snapshot identifier.
     */
    public TimeStampedConsistentCutRecord(long snapshotId) {
        this(snapshotId, U.currentTimeMillis());
    }

    /**
     * Creates a new instance of consistent cut record using the provided {@code snapshotId} and {@code timestamp}.
     *
     * @param snapshotId Snapshot identifier.
     * @param timestamp Timestamp.
     */
    public TimeStampedConsistentCutRecord(long snapshotId, long timestamp) {
        super(timestamp);

        this.snapshotId = snapshotId;
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.TIME_STAMPED_CONSISTENT_CUT;
    }

    /**
     * Gets the snapshot identifier.
     *
     * @return Snapshot identifier.
     */
    public long snapshotId() {
        return snapshotId;
    }

    @Override public String toString() {
        return S.toString(TimeStampedConsistentCutRecord.class, this, "super", super.toString());
    }
}
