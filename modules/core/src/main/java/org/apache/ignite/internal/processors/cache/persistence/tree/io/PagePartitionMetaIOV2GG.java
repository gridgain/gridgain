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

import org.apache.ignite.internal.util.GridStringBuilder;

/**
 * GG meta page IO.
 * This page corresponds to the GridGain IO v1 meta page.
 */
public class PagePartitionMetaIOV2GG extends PagePartitionMetaIOV3 implements PagePartitionMetaIOGG {
    /** GridGain meta page IO delegate. */
    private final PagePartitionMetaIOV1GG delegate;

    /**
     * @param ver Version.
     */
    public PagePartitionMetaIOV2GG(int ver) {
        super(ver);

        delegate = new PagePartitionMetaIOV1GG(ver, ENCRYPT_PAGE_MAX_OFF + 4);
    }

    /** {@inheritDoc} */
    @Override public void initNewPage(long pageAddr, long pageId, int pageSize) {
        super.initNewPage(pageAddr, pageId, pageSize);

        initSpecificFields(pageAddr, pageId, pageSize);
    }

    /** {@inheritDoc} */
    @Override public void initSpecificFields(long pageAddr, long pageId, int pageSize) {
        delegate.initSpecificFields(pageAddr, pageId, pageSize);
    }

    /** {@inheritDoc} */
    @Override public long getUpdateTreeRoot(long pageAddr) {
        return delegate.getUpdateTreeRoot(pageAddr);
    }

    /** {@inheritDoc} */
    @Override public void setUpdateTreeRoot(long pageAddr, long link) {
        delegate.setUpdateTreeRoot(pageAddr, link);
    }

    /** {@inheritDoc} */
    @Override protected void printFields(long pageAddr, GridStringBuilder sb) {
        super.printFields(pageAddr, sb);

        specificFields(pageAddr, sb);
    }

    /** {@inheritDoc} */
    @Override public void specificFields(long pageAddr, GridStringBuilder sb) {
        delegate.specificFields(pageAddr, sb);

        sb.a(",\n\tencryptedPageIndex=").a(getEncryptedPageIndex(pageAddr))
            .a(",\n\tencryptedPageCount=").a(getEncryptedPageCount(pageAddr));
    }

    /** {@inheritDoc} */
    public void upgradePage(long pageAddr) {
        assert PageIO.getType(pageAddr) == getType();

        int from = PageIO.getVersion(pageAddr);

        assert from < 4 || from > getVersion() : "Unexpected page IO version [ver=" + from + ']';

        if (from < 3)
            delegate.upgradePage(pageAddr);

        setEncryptedPageIndex(pageAddr, 0);
        setEncryptedPageCount(pageAddr, 0);
    }
}
