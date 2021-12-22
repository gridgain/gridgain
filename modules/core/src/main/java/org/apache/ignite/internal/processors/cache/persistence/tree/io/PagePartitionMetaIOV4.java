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
 * IO for partition metadata pages.
 */
public class PagePartitionMetaIOV4 extends PagePartitionMetaIOV3 {
    /** */
    protected static final int TOMBSTONES_COUNT_OFF = ENCRYPT_PAGE_MAX_OFF + 4;

    /**
     * @param ver Version.
     */
    public PagePartitionMetaIOV4(int ver) {
        super(ver);
    }

    /** {@inheritDoc} */
    @Override public void initNewPage(long pageAddr, long pageId, int pageSize, PageMetrics metrics) {
        super.initNewPage(pageAddr, pageId, pageSize, metrics);

        setTombstonesCount(pageAddr, 0);
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
    @Override protected void printFields(long pageAddr, GridStringBuilder sb) {
        super.printFields(pageAddr, sb);

        sb.a(",\n\ttombstonesCount=").a(getTombstonesCount(pageAddr));
    }
}
