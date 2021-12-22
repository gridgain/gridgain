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

package org.apache.ignite.internal.processors.cache.persistence.metastorage;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusLeafIO;

/**
 *
 */
public class MetastorageLeafIO extends BPlusLeafIO<MetastorageRow> implements MetastorageBPlusIO {
    /**
     * @param ver Page format version.
     */
    MetastorageLeafIO(int ver) {
        super(T_DATA_REF_METASTORAGE_LEAF, ver, 10 + MetastorageTree.MAX_KEY_LEN);
    }

    /** {@inheritDoc} */
    @Override public void storeByOffset(
        long pageAddr,
        int off,
        MetastorageRow row
    ) {
        assertPageType(pageAddr);

        setVersion(pageAddr, 2);

        MetastoragePageIOUtils.storeByOffset(this, pageAddr, off, row);
    }

    /** {@inheritDoc} */
    @Override public void store(long dstPageAddr, int dstIdx, BPlusIO<MetastorageRow> srcIo, long srcPageAddr, int srcIdx) {
        assertPageType(dstPageAddr);

        setVersion(dstPageAddr, 2);

        MetastoragePageIOUtils.store(this, dstPageAddr, dstIdx, srcIo, srcPageAddr, srcIdx);
    }

    /** {@inheritDoc} */
    @Override public MetastorageRow getLookupRow(BPlusTree<MetastorageRow, ?> tree, long pageAddr,
        int idx
    ) throws IgniteCheckedException {
        assert tree instanceof MetastorageTree;

        return getDataRow(pageAddr, idx, ((MetastorageTree)tree).rowStore());
    }

    /** {@inheritDoc} */
    @Override public long getLink(long pageAddr, int idx) {
        return MetastoragePageIOUtils.getLink(this, pageAddr, idx);
    }

    /** {@inheritDoc} */
    @Override public short getKeySize(long pageAddr, int idx) {
        return MetastoragePageIOUtils.getKeySize(this, pageAddr, idx);
    }

    /** {@inheritDoc} */
    @Override public String getKey(long pageAddr, int idx, MetastorageRowStore rowStore) throws IgniteCheckedException {
        return MetastoragePageIOUtils.getKey(this, pageAddr, idx, rowStore);
    }

    /** {@inheritDoc} */
    @Override public MetastorageDataRow getDataRow(long pageAddr, int idx, MetastorageRowStore rowStore) throws IgniteCheckedException {
        return MetastoragePageIOUtils.getDataRow(this, pageAddr, idx, rowStore);
    }
}
