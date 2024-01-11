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

package org.apache.ignite.internal.pagemem.wal.record.delta;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Physical WAL record that represents a fragment of an entry update.
 */
public class DataPageFragmentedUpdateRecord extends DataPageUpdateRecord {
    /** Link to the next entry fragment. */
    private final long linkToNextFragment;

    /**
     * @param grpId Cache group ID.
     * @param pageId Page ID.
     * @param itemId Item ID.
     * @param linkToNextFragment Link to the next entry fragment.
     * @param payload Record data.
     */
    public DataPageFragmentedUpdateRecord(
        int grpId,
        long pageId,
        int itemId,
        long linkToNextFragment,
        byte[] payload
    ) {
        super(grpId, pageId, itemId, payload);

        this.linkToNextFragment = linkToNextFragment;
    }

    /**
     * @return Link to the next entry fragment.
     */
    public long linkToNextFragment() {
        return linkToNextFragment;
    }

    /** {@inheritDoc} */
    @Override public void applyDelta(PageMemory pageMem, long pageAddr) throws IgniteCheckedException {
        assert payload() != null;

        DataPageIO io = PageIO.getPageIO(pageAddr);

        io.updateFragmentedData(pageAddr, itemId(), pageMem.realPageSize(groupId()), linkToNextFragment, payload());
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.DATA_PAGE_FRAGMENTED_UPDATE_RECORD;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DataPageFragmentedUpdateRecord.class, this, "super", super.toString());
    }
}
