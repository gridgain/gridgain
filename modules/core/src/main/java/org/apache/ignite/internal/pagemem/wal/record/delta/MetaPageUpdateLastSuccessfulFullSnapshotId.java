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

package org.apache.ignite.internal.pagemem.wal.record.delta;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageMetaIO;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * @deprecated Will be removed at 3.0. See IGNITE-11139.
 */
@Deprecated
public class MetaPageUpdateLastSuccessfulFullSnapshotId extends PageDeltaRecord {
    /** */
    private final long lastSuccessfulFullSnapshotId;

    /**
     * @param pageId Meta page ID.
     */
    public MetaPageUpdateLastSuccessfulFullSnapshotId(int grpId, long pageId, long lastSuccessfulFullSnapshotId) {
        super(grpId, pageId);

        this.lastSuccessfulFullSnapshotId = lastSuccessfulFullSnapshotId;
    }

    /** {@inheritDoc} */
    @Override public void applyDelta(PageMemory pageMem, long pageAddr) throws IgniteCheckedException {
        PageMetaIO io = PageMetaIO.VERSIONS.forPage(pageAddr);

        io.setLastSuccessfulFullSnapshotId(pageAddr, lastSuccessfulFullSnapshotId);
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.META_PAGE_UPDATE_LAST_SUCCESSFUL_FULL_SNAPSHOT_ID;
    }

    /**
     * @return Root ID.
     */
    public long lastSuccessfulFullSnapshotId() {
        return lastSuccessfulFullSnapshotId;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(MetaPageUpdateLastSuccessfulFullSnapshotId.class, this, "super", super.toString());
    }
}

