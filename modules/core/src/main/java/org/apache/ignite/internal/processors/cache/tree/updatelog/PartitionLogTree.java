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

package org.apache.ignite.internal.processors.cache.tree.updatelog;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageLockListener;
import org.apache.ignite.internal.util.typedef.internal.CU;

/**
 *
 */
public class PartitionLogTree extends BPlusTree<UpdateLogRow, UpdateLogRow> {
    /** */
    public static final Object FULL_ROW = new Object();

    /** */
    private final CacheGroupContext grp;

    /**
     * @param grp Cache group.
     * @param name Tree name.
     * @param pageMem Page memory.
     * @param metaPageId Meta page ID.
     * @param reuseList Reuse list.
     * @param initNew Initialize new index.
     * @throws IgniteCheckedException If failed.
     */
    public PartitionLogTree(
        CacheGroupContext grp,
        String name,
        PageMemory pageMem,
        long metaPageId,
        ReuseList reuseList,
        boolean initNew,
        PageLockListener lockLsnr
    ) throws IgniteCheckedException {
        super(
            name,
            grp.groupId(),
            grp.name(),
            pageMem,
            grp.dataRegion().config().isPersistenceEnabled() ? grp.shared().wal() : null,
            grp.offheap().globalRemoveId(),
            metaPageId,
            reuseList,
            grp.sharedGroup() ? CacheIdAwareUpdateLogInnerIO.VERSIONS : UpdateLogInnerIO.VERSIONS,
            grp.sharedGroup() ? CacheIdAwareUpdateLogLeafIO.VERSIONS : UpdateLogLeafIO.VERSIONS,
            grp.shared().kernalContext().failure(),
            lockLsnr
        );

        this.grp = grp;

        assert !grp.dataRegion().config().isPersistenceEnabled() || grp.shared().database().checkpointLockIsHeldByThread();

        initTree(initNew);
    }

    /** {@inheritDoc} */
    @Override protected int compare(BPlusIO<UpdateLogRow> iox, long pageAddr, int idx, UpdateLogRow row) {
        UpdateLogRowIO io = (UpdateLogRowIO)iox;

        int cmp;

        if (grp.sharedGroup()) {
            assert row.cacheId != CU.UNDEFINED_CACHE_ID : "Cache ID is not provided!";
            assert io.getCacheId(pageAddr, idx) != CU.UNDEFINED_CACHE_ID : "Cache ID is not stored!";

            cmp = Integer.compare(io.getCacheId(pageAddr, idx), row.cacheId);

            if (cmp != 0)
                return cmp;

            if (row.updCntr == 0 && row.link == 0) {
                // A search row with a cache ID only is used as a cache bound.
                // The found position will be shifted until the exact cache bound is found;
                // See for details:
                // o.a.i.i.p.c.database.tree.BPlusTree.ForwardCursor.findLowerBound()
                // o.a.i.i.p.c.database.tree.BPlusTree.ForwardCursor.findUpperBound()
                return cmp;
            }
        }

        long updCntr = io.getUpdateCounter(pageAddr, idx);

        cmp = Long.compare(updCntr, row.updCntr);

        assert cmp != 0 || row.link == 0 /* search insertion poin */ || io.getLink(pageAddr, idx) == row.link /* remove row */;

        return cmp;
    }

    /** {@inheritDoc} */
    @Override public UpdateLogRow getRow(BPlusIO<UpdateLogRow> io, long pageAddr, int idx, Object flag)
        throws IgniteCheckedException {
        UpdateLogRow row = io.getLookupRow(this, pageAddr, idx);

        return flag == FULL_ROW ? row.initRow(grp) : row;
    }
}
