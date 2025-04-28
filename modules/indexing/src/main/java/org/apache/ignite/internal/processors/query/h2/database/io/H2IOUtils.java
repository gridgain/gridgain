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

package org.apache.ignite.internal.processors.query.h2.database.io;

import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.cache.mvcc.MvccUtils;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.query.h2.opt.H2CacheRow;
import org.apache.ignite.internal.processors.query.h2.opt.H2Row;

/**
 *
 */
public class H2IOUtils {
    private static final int ROW_LINK_SIZE = Long.BYTES;

    private static final int MVCC_CRD_VER_SIZE = Long.BYTES;

    private static final int MVCC_CNTR_SIZE = Long.BYTES;

    private static final int MVCC_OP_CNTR_SIZE = Integer.BYTES;

    private static final int MVCC_OVERHEAD_SIZE = MVCC_CRD_VER_SIZE + MVCC_CNTR_SIZE + MVCC_OP_CNTR_SIZE;

    private static final int MVCC_CRD_VER_OFFSET = ROW_LINK_SIZE;

    private static final int MVCC_CNTR_OFFSET = MVCC_CRD_VER_OFFSET + MVCC_CRD_VER_SIZE;

    private static final int MVCC_OP_CNTR_OFFSET = MVCC_CNTR_OFFSET + MVCC_CNTR_SIZE;

    /**
     *
     */
    private H2IOUtils() {}

    /**
     * @param row Row.
     * @param pageAddr Page address.
     * @param off Offset.
     * @param storeMvcc {@code True} to store mvcc data.
     */
    public static void storeRow(H2CacheRow row, long pageAddr, int off, boolean storeMvcc) {
        assert row.link() != 0;

        PageUtils.putLong(pageAddr, off, row.link());

        if (storeMvcc) {
            long mvccCrdVer = row.mvccCoordinatorVersion();
            long mvccCntr = row.mvccCounter();
            int mvccOpCntr = row.mvccOperationCounter();

            assert MvccUtils.mvccVersionIsValid(mvccCrdVer, mvccCntr, mvccOpCntr);

            PageUtils.putLong(pageAddr, off + MVCC_CRD_VER_OFFSET, mvccCrdVer);
            PageUtils.putLong(pageAddr, off + MVCC_CNTR_OFFSET, mvccCntr);
            PageUtils.putInt(pageAddr, off + MVCC_OP_CNTR_OFFSET, mvccOpCntr);
        }
    }

    /**
     * @param dstPageAddr Destination page address.
     * @param dstOff Destination page offset.
     * @param srcIo Source IO.
     * @param srcPageAddr Source page address.
     * @param srcIdx Source index.
     * @param storeMvcc {@code True} to store mvcc data.
     */
    static void store(long dstPageAddr,
        int dstOff,
        BPlusIO<H2Row> srcIo,
        long srcPageAddr,
        int srcIdx,
        boolean storeMvcc)
    {
        H2RowLinkIO rowIo = (H2RowLinkIO)srcIo;

        long link = rowIo.getLink(srcPageAddr, srcIdx);

        PageUtils.putLong(dstPageAddr, dstOff, link);

        if (storeMvcc) {
            long mvccCrdVer = rowIo.getMvccCoordinatorVersion(srcPageAddr, srcIdx);
            long mvccCntr = rowIo.getMvccCounter(srcPageAddr, srcIdx);
            int mvccOpCntr = rowIo.getMvccOperationCounter(srcPageAddr, srcIdx);

            assert MvccUtils.mvccVersionIsValid(mvccCrdVer, mvccCntr, mvccOpCntr);

            PageUtils.putLong(dstPageAddr, dstOff + MVCC_CRD_VER_OFFSET, mvccCrdVer);
            PageUtils.putLong(dstPageAddr, dstOff + MVCC_CNTR_OFFSET, mvccCntr);
            PageUtils.putInt(dstPageAddr, dstOff + MVCC_OP_CNTR_OFFSET, mvccOpCntr);
        }
    }

    /** Size of the meta information. */
    public static int itemOverhead(boolean mvccEnabled) {
        return ROW_LINK_SIZE + (mvccEnabled ? MVCC_OVERHEAD_SIZE : 0);
    }
}
