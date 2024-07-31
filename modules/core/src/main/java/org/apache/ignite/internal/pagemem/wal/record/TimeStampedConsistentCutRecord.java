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

import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * This record represents a pointer to a consistent cut fuzzy border or actual cut border with a timestamp.
 */
public class TimeStampedConsistentCutRecord extends TimeStampRecord {
    /**
     * Creates a new instance of consistent cut record initializing the timestamp with the current time using {@link U#currentTimeMillis()}.
     */
    public TimeStampedConsistentCutRecord() {
        super(U.currentTimeMillis());
    }

    /**
     * Creates a new instance of consistent cut record using the provided {@code timestamp}.
     */
    public TimeStampedConsistentCutRecord(long timestamp) {
        super(timestamp);
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.TIME_STAMPED_CONSISTENT_CUT;
    }
}
