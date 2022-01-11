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

import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMetrics;
import org.apache.ignite.internal.util.GridStringBuilder;

/**
 * IO for partition metadata pages.
 * Add UpdateLogTree (update counter -> row link) for each partition.
 */
public class PagePartitionMetaIOV1GG extends PagePartitionMetaIOV3 implements PagePartitionMetaIOGG {
    /**
     * Default field offset.
     */
    protected static final int UPDATE_TREE_ROOT_OFF = ENCRYPT_PAGE_MAX_OFF + 4;

    /**
     * @param ver Version.
     */
    public PagePartitionMetaIOV1GG(int ver) {
        super(ver);
    }

    /**
     * {@inheritDoc}
     */
    @Override public void initNewPage(long pageAddr, long pageId, int pageSize, PageMetrics metrics) {
        super.initNewPage(pageAddr, pageId, pageSize, metrics);

        setUpdateTreeRoot(pageAddr, 0L);
    }

    /**
     * {@inheritDoc}
     */
    @Override public long getUpdateTreeRoot(long pageAddr) {
        return PageUtils.getLong(pageAddr, UPDATE_TREE_ROOT_OFF);
    }

    /**
     * {@inheritDoc}
     */
    @Override public boolean setUpdateTreeRoot(long pageAddr, long link) {
        assertPageType(pageAddr);

        if (getUpdateTreeRoot(pageAddr) == link)
            return false;

        PageUtils.putLong(pageAddr, UPDATE_TREE_ROOT_OFF, link);

        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void printFields(long pageAddr, GridStringBuilder sb) {
        super.printFields(pageAddr, sb);

        sb.a(",\n\tupdLogRootPageId=").a(getUpdateTreeRoot(pageAddr));
    }

    /**
     * {@inheritDoc}
     */
    @Override public void upgradePage(long pageAddr) {
        assertPageType(pageAddr);

        int from = PageIO.getVersion(pageAddr);

        PageIO.setVersion(pageAddr, getVersion());

        // GG v1 supports upgrading only from AI versions.
        if (from < 2) {
            setPendingTreeRoot(pageAddr, 0);
            setPartitionMetaStoreReuseListRoot(pageAddr, 0);
            setGapsLink(pageAddr, 0);
        }

        if (from < 3) {
            setEncryptedPageIndex(pageAddr, 0);
            setEncryptedPageCount(pageAddr, 0);
        }

        setUpdateTreeRoot(pageAddr, 0);
    }
}
