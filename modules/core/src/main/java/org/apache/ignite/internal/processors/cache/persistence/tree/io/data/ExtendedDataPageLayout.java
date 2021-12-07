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

package org.apache.ignite.internal.processors.cache.persistence.tree.io.data;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.PageUtils;

public class ExtendedDataPageLayout extends DataPageLayout {

    public ExtendedDataPageLayout() {
        super(
            4,
            2,
            2,
            4,
            4,
            4,
            0b10000000_00000000_00000000_00000000
        );
    }

    /** {@inheritDoc} */
    @Override public int getItem(long pageAddr, int idx) {
        return PageUtils.getInt(pageAddr, itemOffset(idx));
    }


    /** {@inheritDoc} */
    @Override public int getDirectCount(long pageAddr) {
        return PageUtils.getShort(pageAddr, directCntOff) & maxIndex;
    }

    /** {@inheritDoc} */
    @Override public int getIndirectCount(long pageAddr) {
        return PageUtils.getShort(pageAddr, indirectCntOff) & maxIndex;
    }

    /** {@inheritDoc} */
    @Override public int getRealFreeSpace(long pageAddr) {
        return PageUtils.getInt(pageAddr, FREE_SPACE_OFF);
    }

    @Override public void setRealFreeSpace(long pageAddr, int freeSpace) {
        PageUtils.putInt(pageAddr, FREE_SPACE_OFF, freeSpace);
    }

    @Override public int directItemIndex(int indirectItem) {
        return indirectItem & 0xFFFF;
    }

    @Override public int directItemToOffset(int directItem) {
        return directItem;
    }

    @Override public int directItemFromOffset(int dataOff) {
        assert dataOff >= itemsOff + itemSize && dataOff < Integer.MAX_VALUE : dataOff;

        return dataOff;
    }

    @Override public void writeRowData(long pageAddr, int dataOff, byte[] payload) {
        PageUtils.putInt(pageAddr, dataOff, payload.length);
        dataOff += payloadLenSize;

        PageUtils.putBytes(pageAddr, dataOff, payload);
    }

    @Override public ByteBuffer createFragment(PageMemory pageMem, long pageAddr, int dataOff, int payloadSize, long lastLink) {
        ByteBuffer buf = pageMem.pageBuffer(pageAddr);

        buf.position(dataOff);

        int p = payloadSize | fragmentedFlag;

        buf.putInt(p);
        buf.putLong(lastLink);

        return buf;
    }

    @Override public void putFragment(long pageAddr, int dataOff, int payloadSize, byte[] payload, long lastLink) {
        PageUtils.putInt(pageAddr, dataOff, payloadSize | fragmentedFlag);

        PageUtils.putLong(pageAddr, dataOff + 4, lastLink);

        PageUtils.putBytes(pageAddr, dataOff + 12, payload);
    }

    @Override public void setItem(long pageAddr, int idx, int item) {
        PageUtils.putInt(pageAddr, itemOffset(idx), item);
    }

    @Override public int indirectItem(int itemId, int directItemIdx) {
        assert checkIndex(itemId) : itemId;
        assert checkIndex(directItemIdx) : directItemIdx;

        return ((itemId << 16) | directItemIdx);
    }

    @Override public long getNextFragmentLink(long pageAddr, int dataOff) {
        assert isFragmented(pageAddr, dataOff);

        return PageUtils.getLong(pageAddr, dataOff + payloadLenSize);
    }

    @Override public boolean isFragmented(long pageAddr, int dataOff) {
        return (PageUtils.getInt(pageAddr, dataOff) & fragmentedFlag) != 0;
    }

    @Override public void setDirectCount(long pageAddr, int cnt) {
        assert checkCount(cnt) : cnt;

        PageUtils.putShort(pageAddr, directCntOff, (short) cnt);
    }

    @Override public void setIndirectCount(long pageAddr, int cnt) {
        assert checkCount(cnt) : cnt;

        PageUtils.putShort(pageAddr, indirectCntOff, (short) cnt);
    }

    @Override public void setFirstEntryOffset(long pageAddr, int dataOff, long pageSize) {
        assert dataOff >= itemsOff + itemSize && dataOff <= pageSize : dataOff;

        PageUtils.putInt(pageAddr, firstEntryOff, dataOff);
    }

    @Override public int itemId(int indirectItem) {
        return (indirectItem) >>> 16;
    }

    @Override public int getPageEntrySize(long pageAddr, int dataOff, int show) {
        int payloadLen = PageUtils.getInt(pageAddr, dataOff);

        if ((payloadLen & fragmentedFlag) != 0)
            payloadLen &= ~fragmentedFlag; // We are fragmented and have a link.
        else
            show &= ~SHOW_LINK; // We are not fragmented, never have a link.

        return getPageEntrySize(payloadLen, show);
    }

    @Override public int getFirstEntryOffset(long pageAddr) {
        return PageUtils.getInt(pageAddr, firstEntryOff);
    }

    @Override public void putPayloadSize(long addr, int offset, int payloadSize) {
        PageUtils.putInt(addr, 0, payloadSize);
    }
}
