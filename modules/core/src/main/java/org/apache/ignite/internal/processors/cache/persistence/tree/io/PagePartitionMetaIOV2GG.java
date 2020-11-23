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
    /**
     * Registered version.
     * This parameter depends of that the version registered in {@see VERSIONS}.
     */
    private static final int REGISTERED_VERSION = Short.toUnsignedInt((short)-2);

    /** GridGain meta page IO delegate. */
    private final PagePartitionMetaIOV1GG delegate;

    /**
     * Default constructor.
     */
    public PagePartitionMetaIOV2GG() {
        this(REGISTERED_VERSION, ENCRYPT_PAGE_MAX_OFF + 4);
    }

    /**
     * It will need when IO extends in future.
     *
     * @param ver Version.
     * @param fieldOffset Offset after which the page fields are written.
     */
    public PagePartitionMetaIOV2GG(int ver, int fieldOffset) {
        super(ver);

        delegate = new PagePartitionMetaIOV1GG(ver, fieldOffset);
    }

    /** {@inheritDoc} */
    @Override public void initNewPage(long pageAddr, long pageId, int pageSize) {
        if (REGISTERED_VERSION == getVersion())
            super.initNewPage(pageAddr, pageId, pageSize);

        delegate.initNewPage(pageAddr, pageId, pageSize);
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
        if (REGISTERED_VERSION == getVersion())
            super.printFields(pageAddr, sb);

        delegate.printFields(pageAddr, sb);
    }

    /** {@inheritDoc} */
    @Override public void upgradePage(long pageAddr) {
        assert PageIO.getType(pageAddr) == getType();

        int from = PageIO.getVersion(pageAddr);

        delegate.upgradePage(pageAddr);

        if (from < 3 || from > REGISTERED_VERSION) {
            setEncryptedPageIndex(pageAddr, 0);
            setEncryptedPageCount(pageAddr, 0);
        }
    }
}
