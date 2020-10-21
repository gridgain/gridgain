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

package org.apache.ignite.internal.processors.cache.persistence.tree.io;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.util.GridStringBuilder;

/**
 * IO for partition metadata pages.
 * Add UpdateLogTree (update counter -> row link) for each partition.
 */
public class PagePartitionMetaIOV1GG extends PagePartitionMetaIOV2 {

    /** */
    private static final int UPDATE_LOG_TREE_ROOT_OFF = GAPS_LINK + 8;

    /**
     * @param ver Version.
     */
    public PagePartitionMetaIOV1GG(int ver) {
        super(ver);
    }

    /** {@inheritDoc} */
    @Override public void initNewPage(long pageAddr, long pageId, int pageSize) {
        super.initNewPage(pageAddr, pageId, pageSize);

        setUpdateTreeRoot(pageAddr, 0L);
    }

    /** {@inheritDoc} */
    @Override public long getUpdateTreeRoot(long pageAddr) {
        return PageUtils.getLong(pageAddr, UPDATE_LOG_TREE_ROOT_OFF);
    }

    /** {@inheritDoc} */
    @Override public void setUpdateTreeRoot(long pageAddr, long link) {
        PageUtils.putLong(pageAddr, UPDATE_LOG_TREE_ROOT_OFF, link);
    }

    /** {@inheritDoc} */
    @Override protected void printPage(long pageAddr, int pageSize, GridStringBuilder sb) throws IgniteCheckedException {
        byte state = getPartitionState(pageAddr);

        sb.a("PagePartitionMeta[\n\ttreeRoot=").a(getReuseListRoot(pageAddr));
        sb.a(",\n\tpendingTreeRoot=").a(getLastSuccessfulFullSnapshotId(pageAddr));
        sb.a(",\n\tlastSuccessfulFullSnapshotId=").a(getLastSuccessfulFullSnapshotId(pageAddr));
        sb.a(",\n\tlastSuccessfulSnapshotId=").a(getLastSuccessfulSnapshotId(pageAddr));
        sb.a(",\n\tnextSnapshotTag=").a(getNextSnapshotTag(pageAddr));
        sb.a(",\n\tlastSuccessfulSnapshotTag=").a(getLastSuccessfulSnapshotTag(pageAddr));
        sb.a(",\n\tlastAllocatedPageCount=").a(getLastAllocatedPageCount(pageAddr));
        sb.a(",\n\tcandidatePageCount=").a(getCandidatePageCount(pageAddr));
        sb.a(",\n\tsize=").a(getSize(pageAddr));
        sb.a(",\n\tupdateCounter=").a(getUpdateCounter(pageAddr));
        sb.a(",\n\tglobalRemoveId=").a(getGlobalRemoveId(pageAddr));
        sb.a(",\n\tpartitionState=").a(state).a("(").a(GridDhtPartitionState.fromOrdinal(state)).a(")");
        sb.a(",\n\tcountersPageId=").a(getCountersPageId(pageAddr));
        sb.a(",\n\tcntrUpdDataPageId=").a(getGapsLink(pageAddr));
        sb.a(",\n\tupdLogRootPageId=").a(getUpdateTreeRoot(pageAddr));
        sb.a("\n]");
    }

    /**
     * Upgrade page to PagePartitionMetaIOV2.
     *
     * @param pageAddr Page address.
     * @param from From version.
     */
    public void upgradePage(long pageAddr, int from) {
        assert PageIO.getType(pageAddr) == getType();
        assert PageIO.getVersion(pageAddr) < 3;

        PageIO.setVersion(pageAddr, getVersion());

        if (from < 2) {
            setPendingTreeRoot(pageAddr, 0);
            setPartitionMetaStoreReuseListRoot(pageAddr, 0);
            setGapsLink(pageAddr, 0);
        }

        if (from < 3)
            setUpdateTreeRoot(pageAddr, 0);
    }
}
