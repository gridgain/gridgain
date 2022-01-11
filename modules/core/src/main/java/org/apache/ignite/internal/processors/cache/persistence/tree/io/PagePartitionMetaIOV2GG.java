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

package org.apache.ignite.internal.processors.cache.persistence.tree.io;

import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMetrics;
import org.apache.ignite.internal.util.GridStringBuilder;

/**
 * GG meta page IO.
 * This page corresponds to the GridGain IO v2 meta page.
 */
public class PagePartitionMetaIOV2GG extends PagePartitionMetaIOV1GG {
    /**
     * Default field offset.
     */
    public static final int TOMBSTONES_COUNT_OFF = UPDATE_TREE_ROOT_OFF + 8;

    /**
     * It will need when IO extends in future.
     *
     * @param ver Version.
     */
    public PagePartitionMetaIOV2GG(int ver) {
        super(ver);
    }

    /** {@inheritDoc} */
    @Override public void initNewPage(long pageAddr, long pageId, int pageSize, PageMetrics metrics) {
        super.initNewPage(pageAddr, pageId, pageSize, metrics);

        setTombstonesCount(pageAddr, 0L);
    }

    /** {@inheritDoc} */
    @Override protected void printFields(long pageAddr, GridStringBuilder sb) {
        super.printFields(pageAddr, sb);

        sb.a(",\n\ttombstonesCount=").a(getTombstonesCount(pageAddr));
    }

    /** {@inheritDoc} */
    @Override public long getTombstonesCount(long pageAddr) {
        return PageUtils.getLong(pageAddr, TOMBSTONES_COUNT_OFF);
    }

    /** {@inheritDoc} */
    @Override public boolean setTombstonesCount(long pageAddr, long tombstonesCnt) {
        assertPageType(pageAddr);

        if (getTombstonesCount(pageAddr) == tombstonesCnt)
            return false;

        PageUtils.putLong(pageAddr, TOMBSTONES_COUNT_OFF, tombstonesCnt);

        return true;
    }

    /** {@inheritDoc} */
    @Override public void upgradePage(long pageAddr) {
        assertPageType(pageAddr);

        // Will be overrided by super call, need to store.
        int from = PageIO.getVersion(pageAddr);
        PagePartitionMetaIO fromIo = PagePartitionMetaIO.VERSIONS.forVersion(from);

        long tombstonesCnt = 0;
        long updLogRootPageId = 0;

        try {
            tombstonesCnt = fromIo.getTombstonesCount(pageAddr);
        }
        catch (Throwable e) {
            // No-op.
        }

        try {
            updLogRootPageId = fromIo.getUpdateTreeRoot(pageAddr);
        }
        catch (Throwable e) {
            // No-op.
        }

        super.upgradePage(pageAddr);

        if (from < getVersion()) {
            setUpdateTreeRoot(pageAddr, updLogRootPageId);
            setTombstonesCount(pageAddr, tombstonesCnt);
        }
    }
}
