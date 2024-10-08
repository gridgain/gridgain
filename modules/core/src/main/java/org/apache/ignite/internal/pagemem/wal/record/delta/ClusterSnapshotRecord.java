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

package org.apache.ignite.internal.pagemem.wal.record.delta;

import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * ClusterSnapshot record. It splits WAL on 2 areas: any data changes before this record are part of related ClusterSnapshot,
 * and vice versa. It's guaranteed with:
 * 1. The record is written after ClusterSnapshot acquires checkpoint writeLock.
 * 2. The writeLock is acquired on PME, after every transaction already committed and no active transactions anymore.
 */
public class ClusterSnapshotRecord extends WALRecord {
    /** ClusterSnapshot name. */
    @GridToStringInclude
    private final String snpName;

    /**
     * @param snpName ClusterSnapshot name.
     */
    public ClusterSnapshotRecord(String snpName) {
        this.snpName = snpName;
    }

    /** */
    public String clusterSnapshotName() {
        return snpName;
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.CLUSTER_SNAPSHOT;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ClusterSnapshotRecord.class, this);
    }
}
