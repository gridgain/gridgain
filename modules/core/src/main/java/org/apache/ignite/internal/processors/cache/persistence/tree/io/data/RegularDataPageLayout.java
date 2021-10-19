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

public class RegularDataPageLayout extends DataPageLayout {

    public RegularDataPageLayout() {
        super(
            2,
            1,
            1,
            2,
            2,
            2,
            0b10000000_00000000
        );
    }

    /** {@inheritDoc} */
    @Override public int getItem(long pageAddr, int idx) {
        return PageUtils.getShort(pageAddr, itemOffset(idx));
    }

    @Override public int getDirectCount(long pageAddr) {
        return PageUtils.getByte(pageAddr, directCntOff) & maxIndex;
    }

    @Override public int getIndirectCount(long pageAddr) {
        return PageUtils.getByte(pageAddr, indirectCntOff) & maxIndex;
    }

    @Override public int getRealFreeSpace(long pageAddr) {
        return PageUtils.getShort(pageAddr, FREE_SPACE_OFF);
    }

    @Override public int getFirstEntryOffset(long pageAddr) {
        return PageUtils.getShort(pageAddr, firstEntryOff) & 0xFFFF;
    }

    @Override public void setFirstEntryOffset(long pageAddr, int dataOff, long pageSize) {
        assert dataOff >= itemsOff + itemSize && dataOff <= pageSize : dataOff;

        PageUtils.putShort(pageAddr, firstEntryOff, (short)dataOff);
    }

    @Override public int itemId(int indirectItem) {
        return (indirectItem & 0xFFFF) >>> 8;
    }

    @Override public int getPageEntrySize(long pageAddr, int dataOff, int show) {
        int payloadLen = PageUtils.getShort(pageAddr, dataOff) & 0xFFFF;

        if ((payloadLen & fragmentedFlag) != 0)
            payloadLen &= ~fragmentedFlag; // We are fragmented and have a link.
        else
            show &= ~SHOW_LINK; // We are not fragmented, never have a link.

        return getPageEntrySize(payloadLen, show);
    }

    @Override public void setRealFreeSpace(long pageAddr, int freeSpace) {
        PageUtils.putShort(pageAddr, FREE_SPACE_OFF, (short)freeSpace);
    }

    @Override public int directItemIndex(int indirectItem) {
        return indirectItem & 0xFF;
    }

    @Override public int directItemToOffset(int directItem) {
        return directItem & 0xFFFF;
    }

    @Override public int directItemFromOffset(int dataOff) {
        assert dataOff >= itemsOff + itemSize && dataOff < Short.MAX_VALUE : dataOff;

        return (short)dataOff;
    }

    @Override public void writeRowData(long pageAddr, int dataOff, byte[] payload) {
        PageUtils.putShort(pageAddr, dataOff, (short)payload.length);
        dataOff += payloadLenSize;

        PageUtils.putBytes(pageAddr, dataOff, payload);
    }

    @Override public ByteBuffer createFragment(PageMemory pageMem, long pageAddr, int dataOff, int payloadSize, long lastLink) {
        ByteBuffer buf = pageMem.pageBuffer(pageAddr);

        buf.position(dataOff);

        short p = (short)(payloadSize | fragmentedFlag);

        buf.putShort(p);
        buf.putLong(lastLink);

        return buf;
    }

    @Override public void putFragment(long pageAddr, int dataOff, int payloadSize, byte[] payload, long lastLink) {
        PageUtils.putShort(pageAddr, dataOff, (short)(payloadSize | fragmentedFlag));

        PageUtils.putLong(pageAddr, dataOff + 2, lastLink);

        PageUtils.putBytes(pageAddr, dataOff + 10, payload);
    }

    @Override public void setItem(long pageAddr, int idx, int item) {
        PageUtils.putShort(pageAddr, itemOffset(idx), (short) item);
    }

    @Override public int indirectItem(int itemId, int directItemIdx) {
        assert checkIndex(itemId) : itemId;
        assert checkIndex(directItemIdx) : directItemIdx;

        return (short)((itemId << 8) | directItemIdx);
    }

    @Override public long getNextFragmentLink(long pageAddr, int dataOff) {
        assert isFragmented(pageAddr, dataOff);

        return PageUtils.getLong(pageAddr, dataOff + payloadLenSize);
    }

    @Override public boolean isFragmented(long pageAddr, int dataOff) {
        return (PageUtils.getShort(pageAddr, dataOff) & fragmentedFlag) != 0;
    }

    @Override public void setDirectCount(long pageAddr, int cnt) {
        assert checkCount(cnt) : cnt;

        PageUtils.putByte(pageAddr, directCntOff, (byte)cnt);
    }

    @Override public void setIndirectCount(long pageAddr, int cnt) {
        assert checkCount(cnt) : cnt;

        PageUtils.putByte(pageAddr, indirectCntOff, (byte)cnt);
    }

    @Override public void putPayloadSize(long addr, int offset, int payloadSize) {
        PageUtils.putShort(addr, 0, (short)payloadSize);
    }
}
