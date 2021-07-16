/*
 * Copyright 2021 GridGain Systems, Inc. and Contributors.
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

/**
 * Partition clearing started record.
 */
public class PartitionClearingStarted extends WALRecord {
    /** */
    private int partId;

    /** */
    private int grpId;

    /**
     * @return Partition ID.
     */
    public int partId() {
        return partId;
    }

    /**
     * @param partId Partition ID.
     */
    public void partId(int partId) {
        this.partId = partId;
    }

    /**
     * @return Cache group ID.
     */
    public int grpId() {
        return grpId;
    }

    /**
     * @param grpId Cache group ID.
     */
    public void grpId(int grpId) {
        this.grpId = grpId;
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.PARTITION_CLEARING_STARTED;
    }

    /** */
    public PartitionClearingStarted(int partId, int grpId) {
        this.partId = partId;
        this.grpId = grpId;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(PartitionClearingStarted.class, this, "super", super.toString());
    }
}
