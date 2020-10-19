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
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusInnerIO;
import org.apache.ignite.internal.util.typedef.internal.CU;

/**
 *
 */
public abstract class AbstractUpdateLogInnerIO extends BPlusInnerIO<UpdateLogRow> implements UpdateLogRowIO {
    /**
     * @param type      Page type.
     * @param ver       Page format version.
     * @param canGetRow If we can get full row from this page.
     * @param itemSize  Single item size on page.
     */
    AbstractUpdateLogInnerIO(int type, int ver, boolean canGetRow, int itemSize) {
        super(type, ver, canGetRow, itemSize);
    }

    /** {@inheritDoc} */
    @Override public void storeByOffset(long pageAddr, int off, UpdateLogRow row) {
        assert row.link != 0;
        assert row.updCntr > 0;

        PageUtils.putLong(pageAddr, off, row.updCntr);
        PageUtils.putLong(pageAddr, off + 8, row.link);

        if (storeCacheId()) {
            assert row.cacheId != CU.UNDEFINED_CACHE_ID;

            PageUtils.putInt(pageAddr, off + 16, row.cacheId);
        }
    }

    /** {@inheritDoc} */
    @Override public void store(
        long dstPageAddr,
        int dstIdx,
        BPlusIO<UpdateLogRow> srcIo,
        long srcPageAddr,
        int srcIdx
    ) throws IgniteCheckedException {
        int dstOff = offset(dstIdx);

        long link = ((UpdateLogRowIO)srcIo).getLink(srcPageAddr, srcIdx);
        long updCntr = ((UpdateLogRowIO)srcIo).getUpdateCounter(srcPageAddr, srcIdx);

        PageUtils.putLong(dstPageAddr, dstOff, updCntr);
        PageUtils.putLong(dstPageAddr, dstOff + 8, link);

        if (storeCacheId()) {
            int cacheId = ((UpdateLogRowIO)srcIo).getCacheId(srcPageAddr, srcIdx);

            assert cacheId != CU.UNDEFINED_CACHE_ID;

            PageUtils.putInt(dstPageAddr, dstOff + 16, cacheId);
        }
    }

    /** {@inheritDoc} */
    @Override public UpdateLogRow getLookupRow(BPlusTree<UpdateLogRow, ?> tree, long pageAddr, int idx) {
        return new UpdateLogRow(getCacheId(pageAddr, idx), getUpdateCounter(pageAddr, idx), getLink(pageAddr, idx));
    }

    /** {@inheritDoc} */
    @Override public long getUpdateCounter(long pageAddr, int idx) {
        return PageUtils.getLong(pageAddr, offset(idx));
    }

    /** {@inheritDoc} */
    @Override public long getLink(long pageAddr, int idx) {
        return PageUtils.getLong(pageAddr, offset(idx) + 8);
    }

    /**
     * @return {@code True} if cache ID has to be stored.
     */
    protected abstract boolean storeCacheId();
}
