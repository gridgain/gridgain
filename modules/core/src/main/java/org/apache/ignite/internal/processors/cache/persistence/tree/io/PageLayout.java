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

import java.nio.ByteBuffer;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.PageUtils;

import static org.apache.ignite.internal.processors.cache.persistence.tree.io.AbstractDataPageIO.LINK_SIZE;
import static org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO.COMMON_HEADER_END;

public class PageLayout {
    private final PageLayoutHelper helper;

    public PageLayout(boolean bigPages) {
        helper = bigPages ? BigPageLayout.create() : SmallPageLayout.create();
    }

    /**
     * Free space refers to a "max row size (without any data page specific overhead) which is guaranteed to fit into
     * this data page".
     *
     * @param pageAddr Page address.
     * @return Free space.
     */
    public int getFreeSpace(long pageAddr) {
        return helper.getFreeSpace(pageAddr);
    }

    /**
     * @param pageAddr Page address.
     * @return Direct count.
     */
    public int getDirectCount(long pageAddr) {
        return helper.getDirectCount(pageAddr);
    }

    /**
     * @param pageAddr Page address.
     * @return Indirect count.
     */
    public int getIndirectCount(long pageAddr) {
        return helper.getIndirectCount(pageAddr);
    }

    /**
     * @param idx Index of the item.
     * @return Offset in buffer.
     */
    public int itemOffset(int idx) {
        assert helper.checkIndex(idx) : idx;

        return helper.itemOffset(idx);
    }

    /**
     * @param pageAddr Page address.
     * @param idx Item index.
     * @return Item.
     */
    public int getItem(long pageAddr, int idx) {
        return helper.getItem(pageAddr, idx);
    }

    /**
     * Reads saved value of free space.
     *
     * @param pageAddr Page address.
     * @return Free space.
     */
    public int getRealFreeSpace(long pageAddr) {
        return helper.getRealFreeSpace(pageAddr);
    }

    public int directItemIndex(int indirectItem) {
        return helper.directItemIndex(indirectItem);
    }

    /**
     * @param dataOff Data offset.
     * @return Direct item.
     */
    public int directItemFromOffset(int dataOff) {
        return helper.directItemFromOffset(dataOff);
    }

    /**
     * @param pageAddr Page address.
     * @param cnt Direct count.
     */
    public void setDirectCount(long pageAddr, int cnt) {
        helper.setDirectCount(pageAddr, cnt);
    }

    /**
     * @param pageAddr Page address.
     * @param cnt Indirect count.
     */
    public void setIndirectCount(long pageAddr, int cnt) {
        helper.setIndirectCount(pageAddr, cnt);
    }

    public int getFirstEntryOffset(long pageAddr) {
        return helper.getFirstEntryOffset(pageAddr);
    }

    public void setFirstEntryOffset(long pageAddr, int dataOff, int pageSize) {
        helper.setFirstEntryOffset(pageAddr, dataOff, pageSize);
    }

    public void setRealFreeSpace(long pageAddr, int freeSpace) {
        helper.setRealFreeSpace(pageAddr, freeSpace);
    }

    public int directItemToOffset(int directItem) {
        return helper.directItemToOffset(directItem);
    }

    /**
     * @param pageAddr Page address.
     * @param dataOff Data offset.
     * @param show What elements of data page entry to show in the result.
     * @return Data page entry size.
     */
    public int getPageEntrySize(long pageAddr, int dataOff, int show) {
        return helper.getPageEntrySize(pageAddr, dataOff, show);
    }

    /**
     * @param pageAddr Page address.
     * @param dataOff Data offset.
     * @param payload Payload
     */
    protected void writeRowData(long pageAddr, int dataOff, byte[] payload) {
        helper.writeRowData(pageAddr, dataOff, payload);
    }

    /**
     * @param payloadLen Length of the payload, may be a full data row or a row fragment length.
     * @param show What elements of data page entry to show in the result.
     * @return Data page entry size.
     */
    public int getPageEntrySize(int payloadLen, int show) {
        return helper.getPageEntrySize(payloadLen, show);
    }

    public int itemId(int indirectItem) {
        return helper.itemId(indirectItem);
    }

    public int offsetSize() {
        return helper.offsetSize();
    }

    public int offsetMask() {
        return helper.offsetMask();
    }

    public int itemSize() {
        return helper.itemSize();
    }

    public int itemsOffset() {
        return helper.itemsOffset();
    }

    public int payloadLenSize() {
        return helper.payloadLenSize();
    }

    /**
     * @param newEntryFullSize New entry full size (with item, length and link).
     * @param firstEntryOff First entry data offset.
     * @param directCnt Direct items count.
     * @param indirectCnt Indirect items count.
     * @return {@code true} If there is enough space for the entry.
     */
    public boolean isEnoughSpace(int newEntryFullSize, int firstEntryOff, int directCnt, int indirectCnt) {
        return helper.isEnoughSpace(newEntryFullSize, firstEntryOff, directCnt, indirectCnt);
    }

    public ByteBuffer createFragment(PageMemory pageMem, long pageAddr, int dataOff, int payloadSize, long lastLink) {
        return helper.createFragment(pageMem, pageAddr, dataOff, payloadSize, lastLink);
    }

    public void putFragment(long pageAddr, int dataOff, int payloadSize, byte[] payload, long lastLink) {
        helper.putFragment(pageAddr, dataOff, payloadSize, payload, lastLink);
    }

    /**
     * @param pageAddr Page address.
     * @param idx Item index.
     * @param item Item.
     */
    public void setItem(long pageAddr, int idx, int item) {
        helper.setItem(pageAddr, idx, item);
    }

    /**
     * @param itemId Fixed item ID (the index used for referencing an entry from the outside).
     * @param directItemIdx Index of corresponding direct item.
     * @return Indirect item.
     */
    public int indirectItem(int itemId, int directItemIdx) {
        return helper.indirectItem(itemId, directItemIdx);
    }

    /**
     * @param pageAddr Page address.
     * @param dataOff Points to the entry start.
     * @return Link to the next entry fragment or 0 if no fragments left or if entry is not fragmented.
     */
    public long getNextFragmentLink(long pageAddr, int dataOff) {
        return helper.getNextFragmentLink(pageAddr, dataOff);
    }

    /**
     * @param pageAddr Page address.
     * @param dataOff Data offset.
     * @return {@code true} If the data row is fragmented across multiple pages.
     */
    protected boolean isFragmented(long pageAddr, int dataOff) {
        return helper.isFragmented(pageAddr, dataOff);
    }

    public boolean checkIndex(int itemId) {
        return helper.checkIndex(itemId);
    }

    public boolean checkCount(int cnt) {
        return helper.checkCount(cnt);
    }

    public int minDataPageOverhead() {
        return helper.minDataPageOverhead();
    }

    public void putPayloadSize(long addr, int offset, int payloadSize) {
        helper.putPayloadSize(addr, offset, payloadSize);
    }

    private abstract static class PageLayoutHelper {
        /** */
        protected static final int SHOW_ITEM = 0b0001;

        /** */
        protected static final int SHOW_PAYLOAD_LEN = 0b0010;

        /** */
        protected static final int SHOW_LINK = 0b0100;

        /** */
        protected static final int FREE_LIST_PAGE_ID_OFF = COMMON_HEADER_END;

        /** */
        protected static final int FREE_SPACE_OFF = FREE_LIST_PAGE_ID_OFF + 8;

        protected final int directCntSize;

        protected final int maxIndex;

        /** */
        protected final int directCntOff;

        /** */
        protected final int indirectCntOff;

        /** */
        protected final int firstEntryOff;

        /** */
        protected final int itemsOff;

        /** */
        protected final int itemSize;

        /** */
        protected final int payloadLenSize;

        /** */
        protected final int fragmentedFlag;

        /** */
        protected final int minDataPageOverhead;

        protected PageLayoutHelper(
            int freeSpaceSize,
            int directCntSize,
            int indirectCntSize,
            int firstEntrySize,
            int itemSize,
            int payloadLenSize,
            int fragmentedFlag
        ) {
            this.directCntSize = directCntSize;
            this.maxIndex = ~(-1 << directCntSize * Byte.SIZE);
            this.directCntOff = PageLayoutHelper.FREE_SPACE_OFF + freeSpaceSize;
            this.indirectCntOff = directCntOff + directCntSize;
            this.firstEntryOff = indirectCntOff + indirectCntSize;
            this.itemsOff = firstEntryOff + firstEntrySize;
            this.itemSize = itemSize;
            this.payloadLenSize = payloadLenSize;
            this.fragmentedFlag = fragmentedFlag;
            this.minDataPageOverhead = itemsOff + itemSize + payloadLenSize + LINK_SIZE;
        }

        /** @see PageLayout#getItem(long, int) */
        abstract int getItem(long pageAddr, int idx);

        /**
         * @param idx Index of the item.
         * @return Offset in buffer.
         */
        public int itemOffset(int idx) {
            assert checkIndex(idx) : idx;

            return itemsOff + idx * itemSize;
        }

        /**
         * @param idx Index.
         * @return {@code true} If the index is valid.
         */
        public boolean checkIndex(int idx) {
            return idx >= 0 && idx < maxIndex;
        }

        /**
         * @param cnt Counter value.
         * @return {@code true} If the counter fits 1 byte.
         */
        public boolean checkCount(int cnt) {
            return cnt >= 0 && cnt <= maxIndex;
        }

        /** @see PageLayout#getDirectCount(long) */
        abstract int getDirectCount(long pageAddr);

        /** @see PageLayout#getIndirectCount(long) */
        public abstract int getIndirectCount(long pageAddr);

        public int getFreeSpace(long pageAddr) {
            if (getFreeItemSlots(pageAddr) == 0)
                return 0;

            int freeSpace = getRealFreeSpace(pageAddr);

            // We reserve size here because of getFreeSpace() method semantics (see method javadoc).
            // It means that we must be able to accommodate a row of size which is equal to getFreeSpace(),
            // plus we will have data page overhead: header of the page as well as item, payload length and
            // possibly a link to the next row fragment.
            freeSpace -= itemSize + payloadLenSize + LINK_SIZE;

            return freeSpace < 0 ? 0 : freeSpace;
        }

        private int getFreeItemSlots(long pageAddr) {
            return maxIndex - getDirectCount(pageAddr);
        }

        /** @see PageLayout#getFirstEntryOffset(long) */
        public abstract int getFirstEntryOffset(long pageAddr);

        /** @see PageLayout#getRealFreeSpace(long) */
        public abstract int getRealFreeSpace(long pageAddr);

        /** @see PageLayout#setRealFreeSpace(long, int) */
        public abstract void setRealFreeSpace(long pageAddr, int freeSpace);

        /** @see PageLayout#directItemIndex(int) */
        public abstract int directItemIndex(int indirectItem);

        /** @see PageLayout#directItemToOffset(int) */
        public abstract int directItemToOffset(int directItem);

        /** @see PageLayout#directItemFromOffset(int) */
        public abstract int directItemFromOffset(int dataOff);

        /** @see PageLayout#writeRowData(long, int, byte[]) */
        public abstract void writeRowData(long pageAddr, int dataOff, byte[] payload);

        /** @see PageLayout#offsetSize() */
        public int offsetSize() {
            return directCntSize * 8;
        }

        /** @see PageLayout#offsetMask() */
        public int offsetMask() {
            return maxIndex;
        }

        /** @see PageLayout#itemSize() */
        public int itemSize() {
            return itemSize;
        }

        /** @see PageLayout#itemsOffset() */
        public int itemsOffset() {
            return itemsOff;
        }

        /** @see PageLayout#payloadLenSize() */
        public int payloadLenSize() {
            return payloadLenSize;
        }

        public boolean isEnoughSpace(int newEntryFullSize, int firstEntryOff, int directCnt, int indirectCnt) {
            return itemsOff + itemSize * (directCnt + indirectCnt) <= firstEntryOff - newEntryFullSize;
        }

        public abstract ByteBuffer createFragment(PageMemory pageMem, long pageAddr, int dataOff, int payloadSize, long lastLink);

        public abstract void putFragment(long pageAddr, int dataOff, int payloadSize, byte[] payload, long lastLink);

        public abstract void setItem(long pageAddr, int idx, int item);

        public abstract int indirectItem(int itemId, int directItemIdx);

        public abstract long getNextFragmentLink(long pageAddr, int dataOff);

        public abstract boolean isFragmented(long pageAddr, int dataOff);

        /**
         * @param pageAddr Page address.
         * @param cnt Direct count.
         */
        public abstract void setDirectCount(long pageAddr, int cnt);

        /**
         * @param pageAddr Page address.
         * @param cnt Indirect count.
         */
        public abstract void setIndirectCount(long pageAddr, int cnt);

        public abstract void setFirstEntryOffset(long pageAddr, int dataOff, long pageSize);

        public abstract int itemId(int indirectItem);

        public abstract int getPageEntrySize(long pageAddr, int dataOff, int show);

        public int getPageEntrySize(int payloadLen, int show) {
            assert payloadLen > 0 : payloadLen;

            int res = payloadLen;

            if ((show & SHOW_LINK) != 0)
                res += LINK_SIZE;

            if ((show & SHOW_ITEM) != 0)
                res += itemSize;

            if ((show & SHOW_PAYLOAD_LEN) != 0)
                res += payloadLenSize;

            return res;
        }

        public int minDataPageOverhead() {
            return minDataPageOverhead;
        }

        public abstract void putPayloadSize(long addr, int offset, int payloadSize);
    }

    private static class BigPageLayout extends PageLayoutHelper {

        private BigPageLayout(
            int freeSpaceSize,
            int directCntSize,
            int indirectCntSize,
            int firstEntrySize,
            int itemSize,
            int payloadLenSize,
            int fragmentedFlag
        ) {
            super(
                freeSpaceSize,
                directCntSize,
                indirectCntSize,
                firstEntrySize,
                itemSize,
                payloadLenSize,
                fragmentedFlag
            );
        }

        public static BigPageLayout create() {
            final int itemSize = 4;

            final int payloadLenSize = 4;

            final int fragmentedFlag = 0b10000000_00000000_00000000_00000000;

            return new BigPageLayout(
                4,
                2,
                2,
                4,
                itemSize,
                payloadLenSize,
                fragmentedFlag
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

    private static class SmallPageLayout extends PageLayoutHelper {

        private SmallPageLayout(
            int freeSpaceSize,
            int directCntSize,
            int indirectCntSize,
            int firstEntrySize,
            int itemSize,
            int payloadLenSize,
            int fragmentedFlag
        ) {
            super(
                freeSpaceSize,
                directCntSize,
                indirectCntSize,
                firstEntrySize,
                itemSize,
                payloadLenSize,
                fragmentedFlag
            );
        }

        public static SmallPageLayout create() {
            final int itemSize = 2;

            final int payloadLenSize = 2;

            final int fragmentedFlag = 0b10000000_00000000;

            return new SmallPageLayout(
                2,
                1,
                1,
                2,
                itemSize,
                payloadLenSize,
                fragmentedFlag
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
}
