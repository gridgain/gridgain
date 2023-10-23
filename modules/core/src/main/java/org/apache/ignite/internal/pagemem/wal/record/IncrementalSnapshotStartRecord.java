/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
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

import java.util.UUID;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * This record is written to WAL at the moment when incremental snapshot starts on a baseline node.
 */
public class IncrementalSnapshotStartRecord extends WALRecord {
    /** Incremental snapshot ID. */
    @GridToStringInclude
    private final UUID id;

    /** @param id Incremental snapshot ID. */
    public IncrementalSnapshotStartRecord(UUID id) {
        this.id = id;
    }

    /** @return Incremental snapshot ID. */
    public UUID id() {
        return id;
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.INCREMENTAL_SNAPSHOT_START_RECORD;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IncrementalSnapshotStartRecord.class, this);
    }
}
