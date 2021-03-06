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
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMetrics;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageMetaIO;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class MetaPageInitRecord extends InitNewPageRecord {
    /** */
    private long treeRoot;

    /** */
    private long reuseListRoot;

    /** */
    private int ioType;

    /**
     * @param grpId Cache group ID.
     * @param pageId Page ID.
     * @param ioType IO type.
     * @param ioVer Io version.
     * @param treeRoot Tree root.
     * @param reuseListRoot Reuse list root.
     */
    public MetaPageInitRecord(int grpId, long pageId, int ioType, int ioVer, long treeRoot, long reuseListRoot) {
        this(grpId, pageId, ioType, ioVer, treeRoot, reuseListRoot, null);
    }

    /**
     * @param grpId Cache group ID.
     * @param pageId Page ID.
     * @param ioType IO type.
     * @param treeRoot Tree root.
     * @param reuseListRoot Reuse list root.
     * @param log Logger for case data is invalid. Can be {@code null}, but is needed when processing existing storage.
     */
    public MetaPageInitRecord(
        int grpId,
        long pageId,
        int ioType,
        int ioVer,
        long treeRoot,
        long reuseListRoot,
        @Nullable IgniteLogger log
    ) {
        super(grpId, pageId, ioType, ioVer, pageId, log);

        this.treeRoot = treeRoot;
        this.reuseListRoot = reuseListRoot;
        this.ioType = ioType;
    }

    /**
     * @return Tree root.
     */
    public long treeRoot() {
        return treeRoot;
    }

    /**
     * @return Reuse list root.
     */
    public long reuseListRoot() {
        return reuseListRoot;
    }

    /** {@inheritDoc} */
    @Override public int ioType() {
        return ioType;
    }

    /** {@inheritDoc} */
    @Override public void applyDelta(PageMemory pageMem, long pageAddr) throws IgniteCheckedException {
        PageMetaIO io = PageMetaIO.getPageIO(ioType, ioVer);

        PageMetrics metrics = pageMem.metrics().cacheGrpPageMetrics(groupId());

        io.initNewPage(pageAddr, newPageId, pageMem.realPageSize(groupId()), metrics);

        io.setTreeRoot(pageAddr, treeRoot);
        io.setReuseListRoot(pageAddr, reuseListRoot);
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.META_PAGE_INIT;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(MetaPageInitRecord.class, this, "super", super.toString());
    }
}
