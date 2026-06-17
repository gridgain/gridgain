/*
 * Copyright 2026 GridGain Systems, Inc. and Contributors.
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
package org.apache.ignite.internal.processors.cache.index;

import java.util.Collection;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusMetaIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.query.h2.H2TableDescriptor;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.database.H2Tree;
import org.apache.ignite.internal.processors.query.h2.database.H2TreeIndex;
import org.apache.ignite.internal.processors.query.h2.database.io.H2LeafIO;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Base class for tests that corrupt an H2 secondary index's B+ tree directly in page memory.
 */
public abstract class AbstractIndexCorruptionTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();

        GridQueryProcessor.idxCls = null;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        GridQueryProcessor.idxCls = null;

        super.afterTest();
    }

    /**
     * Corrupts the first leaf page of every segment of the given index by zeroing its row-link bytes.
     *
     * @param srv Node holding the index.
     * @param indexing H2 indexing.
     * @param cacheName Cache name.
     * @param idxName Index name.
     * @throws IgniteCheckedException If page memory access failed.
     */
    protected void corruptIndex(IgniteEx srv, IgniteH2Indexing indexing, String cacheName, String idxName)
        throws IgniteCheckedException {
        PageMemoryEx mem = (PageMemoryEx)srv.context().cache().context()
            .cacheContext(CU.cacheId(cacheName)).dataRegion().pageMemory();

        Collection<H2TableDescriptor> tables = indexing.schemaManager().tablesForCache(cacheName);

        for (H2TableDescriptor descriptor : tables) {
            H2TreeIndex idx = (H2TreeIndex)descriptor.table().getIndex(idxName);
            int segments = idx.segmentsCount();

            for (int segment = 0; segment < segments; segment++) {
                H2Tree tree = idx.treeForRead(segment);

                GridCacheDatabaseSharedManager mgr = dbMgr(srv);

                mgr.checkpointReadLock();

                try {
                    corruptTreeRoot(mem, tree.groupId(), tree.getMetaPageId());
                }
                finally {
                    mgr.checkpointReadUnlock();
                }
            }
        }
    }

    /** Zeroes the row-link bytes of every item in the index's first leaf page. */
    private void corruptTreeRoot(PageMemoryEx pageMem, int grpId, long metaPageId)
        throws IgniteCheckedException {
        long leafId = findFirstLeafId(grpId, metaPageId, pageMem);

        if (leafId == 0L)
            return;

        long leafPage = pageMem.acquirePage(grpId, leafId);

        try {
            long leafAddr = pageMem.writeLock(grpId, leafId, leafPage);

            try {
                H2LeafIO io = PageIO.getBPlusIO(leafAddr);

                for (int i = 0; i < io.getCount(leafAddr); i++)
                    PageUtils.putLong(leafAddr, io.offset(i) + io.getPayloadSize(), 0);
            }
            finally {
                pageMem.writeUnlock(grpId, leafId, leafPage, true, true);
            }
        }
        finally {
            pageMem.releasePage(grpId, leafId, leafPage);
        }
    }

    /** Returns the id of the index's first leaf page, or {@code 0} if the tree is empty. */
    private long findFirstLeafId(int grpId, long metaPageId, PageMemoryEx pageMem)
        throws IgniteCheckedException {
        long metaPage = pageMem.acquirePage(grpId, metaPageId);

        try {
            long metaAddr = pageMem.readLock(grpId, metaPageId, metaPage);

            try {
                BPlusMetaIO metaIo = PageIO.getPageIO(metaAddr);

                return metaIo.getFirstPageId(metaAddr, 0);
            }
            finally {
                pageMem.readUnlock(grpId, metaPageId, metaPage);
            }
        }
        finally {
            pageMem.releasePage(grpId, metaPageId, metaPage);
        }
    }
}
