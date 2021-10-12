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

import static org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO.COMMON_HEADER_END;

public class PageLayout {

    private final int pageSize;

    private final PageLayoutHelper helper;

    public PageLayout(int size) {
        pageSize = size;
//        helper = new BigPageLayout(size);
        helper = new SmallPageLayout(size);
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

    public void setFirstEntryOffset(long pageAddr, int dataOff) {
        helper.setFirstEntryOffset(pageAddr, dataOff);
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

    public int pageSize() {
        return pageSize;
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

    private interface PageLayoutHelper {
        /**
         * @param pageAddr Page address.
         * @param idx Item index.
         * @return Item.
         */
        int getItem(long pageAddr, int idx);

        int itemOffset(int idx);

        boolean checkIndex(int idx);

        /** @see PageLayout#getDirectCount(long) */
        int getDirectCount(long pageAddr);

        /** @see PageLayout#getIndirectCount(long) */
        int getIndirectCount(long pageAddr);

        /** @see PageLayout#getFreeSpace(long) */
        int getFreeSpace(long pageAddr);

        int getFirstEntryOffset(long pageAddr);

        int getRealFreeSpace(long pageAddr);

        void setRealFreeSpace(long pageAddr, int freeSpace);

        int directItemIndex(int indirectItem);

        int directItemToOffset(int directItem);

        int directItemFromOffset(int dataOff);

        void writeRowData(long pageAddr, int dataOff, byte[] payload);

        public int offsetSize();

        public int offsetMask();

        public int itemSize();

        public int itemsOffset();

        public int payloadLenSize();

        public boolean isEnoughSpace(int newEntryFullSize, int firstEntryOff, int directCnt, int indirectCnt);

        ByteBuffer createFragment(PageMemory pageMem, long pageAddr, int dataOff, int payloadSize, long lastLink);

        void putFragment(long pageAddr, int dataOff, int payloadSize, byte[] payload, long lastLink);

        void setItem(long pageAddr, int idx, int item);

        int indirectItem(int itemId, int directItemIdx);

        long getNextFragmentLink(long pageAddr, int dataOff);

        boolean isFragmented(long pageAddr, int dataOff);

        /**
         * @param pageAddr Page address.
         * @param cnt Direct count.
         */
        public void setDirectCount(long pageAddr, int cnt);

        /**
         * @param pageAddr Page address.
         * @param cnt Indirect count.
         */
        public void setIndirectCount(long pageAddr, int cnt);

        void setFirstEntryOffset(long pageAddr, int dataOff);

        int itemId(int indirectItem);

        int getPageEntrySize(long pageAddr, int dataOff, int show);

        int getPageEntrySize(int payloadLen, int show);

        boolean checkCount(int cnt);

        int minDataPageOverhead();

        void putPayloadSize(long addr, int offset, int payloadSize);
    }

    private static class BigPageLayout implements PageLayoutHelper {
        /** */
        private static final int SHOW_ITEM = 0b0001;

        /** */
        private static final int SHOW_PAYLOAD_LEN = 0b0010;

        /** */
        private static final int SHOW_LINK = 0b0100;

        /** */
        private static final int FREE_LIST_PAGE_ID_OFF = COMMON_HEADER_END;

        /** */
        private static final int FREE_SPACE_OFF = FREE_LIST_PAGE_ID_OFF + 8;

        /** */
        private static final int DIRECT_CNT_OFF = FREE_SPACE_OFF + 4;

        /** */
        private static final int INDIRECT_CNT_OFF = DIRECT_CNT_OFF + 2;

        /** */
        private static final int FIRST_ENTRY_OFF = INDIRECT_CNT_OFF + 2;

        /** */
        public static final int ITEMS_OFF = FIRST_ENTRY_OFF + 4;

        /** */
        private static final int ITEM_SIZE = 4;

        /** */
        protected static final int PAYLOAD_LEN_SIZE = 4;

        /** */
        private static final int LINK_SIZE = 8;

        /** */
        private static final int FRAGMENTED_FLAG = 0b10000000_00000000_00000000_00000000;

        /** */
        public static final int MIN_DATA_PAGE_OVERHEAD = ITEMS_OFF + ITEM_SIZE + PAYLOAD_LEN_SIZE + LINK_SIZE;

        private final int pageSize;

        private BigPageLayout(int size) {
            pageSize = size;
        }

        /** {@inheritDoc} */
        @Override public int getItem(long pageAddr, int idx) {
            return PageUtils.getInt(pageAddr, itemOffset(idx));
        }

        /**
         * @param idx Index of the item.
         * @return Offset in buffer.
         */
        @Override public int itemOffset(int idx) {
            assert checkIndex(idx) : idx;

            return ITEMS_OFF + idx * ITEM_SIZE;
        }

        /** {@inheritDoc} */
        @Override public int getDirectCount(long pageAddr) {
            return PageUtils.getShort(pageAddr, DIRECT_CNT_OFF) & 0xFFFF;
        }

        /** {@inheritDoc} */
        @Override public int getIndirectCount(long pageAddr) {
            return PageUtils.getShort(pageAddr, INDIRECT_CNT_OFF) & 0xFFFF;
        }

        /** {@inheritDoc} */
        @Override public int getRealFreeSpace(long pageAddr) {
            return PageUtils.getInt(pageAddr, FREE_SPACE_OFF);
        }

        /**
         * Free space refers to a "max row size (without any data page specific overhead) which is guaranteed to fit into
         * this data page".
         *
         * @param pageAddr Page address.
         * @return Free space.
         */
        @Override public int getFreeSpace(long pageAddr) {
            if (getFreeItemSlots(pageAddr) == 0)
                return 0;

            int freeSpace = getRealFreeSpace(pageAddr);

            // We reserve size here because of getFreeSpace() method semantics (see method javadoc).
            // It means that we must be able to accommodate a row of size which is equal to getFreeSpace(),
            // plus we will have data page overhead: header of the page as well as item, payload length and
            // possibly a link to the next row fragment.
            freeSpace -= ITEM_SIZE + PAYLOAD_LEN_SIZE + LINK_SIZE;

            return freeSpace < 0 ? 0 : freeSpace;
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
            assert dataOff >= ITEMS_OFF + ITEM_SIZE && dataOff < Integer.MAX_VALUE : dataOff;

            return dataOff;
        }

        @Override public void writeRowData(long pageAddr, int dataOff, byte[] payload) {
            PageUtils.putInt(pageAddr, dataOff, payload.length);
            dataOff += PAYLOAD_LEN_SIZE;

            PageUtils.putBytes(pageAddr, dataOff, payload);
        }

        @Override public int offsetSize() {
            return 16;
        }

        @Override public int offsetMask() {
            return ~(-1 << offsetSize());
        }

        @Override public int itemSize() {
            return ITEM_SIZE;
        }

        @Override public int itemsOffset() {
            return ITEMS_OFF;
        }

        @Override public int payloadLenSize() {
            return PAYLOAD_LEN_SIZE;
        }

        @Override public boolean isEnoughSpace(int newEntryFullSize, int firstEntryOff, int directCnt, int indirectCnt) {
            return ITEMS_OFF + ITEM_SIZE * (directCnt + indirectCnt) <= firstEntryOff - newEntryFullSize;
        }

        @Override public ByteBuffer createFragment(PageMemory pageMem, long pageAddr, int dataOff, int payloadSize, long lastLink) {
            ByteBuffer buf = pageMem.pageBuffer(pageAddr);

            buf.position(dataOff);

            int p = (int)(payloadSize | FRAGMENTED_FLAG);

            buf.putInt(p);
            buf.putLong(lastLink);

            return buf;
        }

        @Override public void putFragment(long pageAddr, int dataOff, int payloadSize, byte[] payload, long lastLink) {
            PageUtils.putInt(pageAddr, dataOff, (int)(payloadSize | FRAGMENTED_FLAG));

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

            return PageUtils.getLong(pageAddr, dataOff + PAYLOAD_LEN_SIZE);
        }

        @Override public boolean isFragmented(long pageAddr, int dataOff) {
            return (PageUtils.getInt(pageAddr, dataOff) & FRAGMENTED_FLAG) != 0;
        }

        @Override public void setDirectCount(long pageAddr, int cnt) {
            assert checkCount(cnt) : cnt;

            PageUtils.putShort(pageAddr, DIRECT_CNT_OFF, (short) cnt);
        }

        @Override public void setIndirectCount(long pageAddr, int cnt) {
            assert checkCount(cnt) : cnt;

            PageUtils.putShort(pageAddr, INDIRECT_CNT_OFF, (short) cnt);
        }

        @Override public void setFirstEntryOffset(long pageAddr, int dataOff) {
            assert dataOff >= ITEMS_OFF + ITEM_SIZE && dataOff <= pageSize : dataOff;

            PageUtils.putInt(pageAddr, FIRST_ENTRY_OFF, dataOff);
        }

        @Override public int itemId(int indirectItem) {
            return (indirectItem) >>> 16;
        }

        @Override public int getPageEntrySize(long pageAddr, int dataOff, int show) {
            int payloadLen = PageUtils.getInt(pageAddr, dataOff);

            if ((payloadLen & FRAGMENTED_FLAG) != 0)
                payloadLen &= ~FRAGMENTED_FLAG; // We are fragmented and have a link.
            else
                show &= ~SHOW_LINK; // We are not fragmented, never have a link.

            return getPageEntrySize(payloadLen, show);
        }

        @Override public int getPageEntrySize(int payloadLen, int show) {
            assert payloadLen > 0 : payloadLen;

            int res = payloadLen;

            if ((show & SHOW_LINK) != 0)
                res += LINK_SIZE;

            if ((show & SHOW_ITEM) != 0)
                res += ITEM_SIZE;

            if ((show & SHOW_PAYLOAD_LEN) != 0)
                res += PAYLOAD_LEN_SIZE;

            return res;
        }

        /**
         * @param pageAddr Page address.
         * @return Number of free entry slots.
         */
        private int getFreeItemSlots(long pageAddr) {
            return 0xFFFF - getDirectCount(pageAddr);
        }

        @Override public int getFirstEntryOffset(long pageAddr) {
            return PageUtils.getInt(pageAddr, FIRST_ENTRY_OFF);
        }

        /**
         * @param idx Index.
         * @return {@code true} If the index is valid.
         */
        @Override public boolean checkIndex(int idx) {
            return idx >= 0 && idx < 0xFFFF;
        }

        /**
         * @param cnt Counter value.
         * @return {@code true} If the counter fits 1 byte.
         */
        @Override public boolean checkCount(int cnt) {
            return cnt >= 0 && cnt <= 0xFFFF;
        }

        @Override public int minDataPageOverhead() {
            return MIN_DATA_PAGE_OVERHEAD;
        }

        @Override public void putPayloadSize(long addr, int offset, int payloadSize) {
            PageUtils.putInt(addr, 0, payloadSize);
        }
    }

    private static class SmallPageLayout implements PageLayoutHelper {

        /** */
        private static final int SHOW_ITEM = 0b0001;

        /** */
        private static final int SHOW_PAYLOAD_LEN = 0b0010;

        /** */
        private static final int SHOW_LINK = 0b0100;

        /** */
        private static final int FREE_LIST_PAGE_ID_OFF = COMMON_HEADER_END;

        /** */
        private static final int FREE_SPACE_OFF = FREE_LIST_PAGE_ID_OFF + 8;

        /** */
        private static final int DIRECT_CNT_OFF = FREE_SPACE_OFF + 2;

        /** */
        private static final int INDIRECT_CNT_OFF = DIRECT_CNT_OFF + 1;

        /** */
        private static final int FIRST_ENTRY_OFF = INDIRECT_CNT_OFF + 1;

        /** */
        public static final int ITEMS_OFF = FIRST_ENTRY_OFF + 2;

        /** */
        private static final int ITEM_SIZE = 2;

        /** */
        private static final int PAYLOAD_LEN_SIZE = 2;

        /** */
        private static final int LINK_SIZE = 8;

        /** */
        private static final int FRAGMENTED_FLAG = 0b10000000_00000000;

        private final int pageSize;

        private SmallPageLayout(int size) {
            pageSize = size;
        }

        /** */
        public static final int MIN_DATA_PAGE_OVERHEAD = ITEMS_OFF + ITEM_SIZE + PAYLOAD_LEN_SIZE + LINK_SIZE;

        /** {@inheritDoc} */
        @Override public int getItem(long pageAddr, int idx) {
            return PageUtils.getShort(pageAddr, itemOffset(idx));
        }

        @Override public int itemOffset(int idx) {
            assert checkIndex(idx) : idx;

            return ITEMS_OFF + idx * ITEM_SIZE;
        }

        /**
         * @param idx Index.
         * @return {@code true} If the index is valid.
         */
        @Override public boolean checkIndex(int idx) {
            return idx >= 0 && idx < 0xFF;
        }

        /**
         * @param cnt Counter value.
         * @return {@code true} If the counter fits 1 byte.
         */
        @Override public boolean checkCount(int cnt) {
            return cnt >= 0 && cnt <= 0xFF;
        }

        @Override public int getDirectCount(long pageAddr) {
            return PageUtils.getByte(pageAddr, DIRECT_CNT_OFF) & 0xFF;
        }

        @Override public int getIndirectCount(long pageAddr) {
            return PageUtils.getByte(pageAddr, INDIRECT_CNT_OFF) & 0xFF;
        }

        @Override public int getRealFreeSpace(long pageAddr) {
            return PageUtils.getShort(pageAddr, FREE_SPACE_OFF);
        }

        @Override public int getFreeSpace(long pageAddr) {
            if (getFreeItemSlots(pageAddr) == 0)
                return 0;

            int freeSpace = getRealFreeSpace(pageAddr);

            // We reserve size here because of getFreeSpace() method semantics (see method javadoc).
            // It means that we must be able to accommodate a row of size which is equal to getFreeSpace(),
            // plus we will have data page overhead: header of the page as well as item, payload length and
            // possibly a link to the next row fragment.
            freeSpace -= ITEM_SIZE + PAYLOAD_LEN_SIZE + LINK_SIZE;

            return freeSpace < 0 ? 0 : freeSpace;
        }

        private int getFreeItemSlots(long pageAddr) {
            return 0xFF - getDirectCount(pageAddr);
        }

        @Override public int getFirstEntryOffset(long pageAddr) {
            return PageUtils.getShort(pageAddr, FIRST_ENTRY_OFF) & 0xFFFF;
        }

        @Override public void setFirstEntryOffset(long pageAddr, int dataOff) {
            assert dataOff >= ITEMS_OFF + ITEM_SIZE && dataOff <= pageSize : dataOff;

            PageUtils.putShort(pageAddr, FIRST_ENTRY_OFF, (short)dataOff);
        }

        @Override public int itemId(int indirectItem) {
            return (indirectItem & 0xFFFF) >>> 8;
        }

        @Override public int getPageEntrySize(long pageAddr, int dataOff, int show) {
            int payloadLen = PageUtils.getShort(pageAddr, dataOff);

            if ((payloadLen & FRAGMENTED_FLAG) != 0)
                payloadLen &= ~FRAGMENTED_FLAG; // We are fragmented and have a link.
            else
                show &= ~SHOW_LINK; // We are not fragmented, never have a link.

            return getPageEntrySize(payloadLen, show);
        }

        @Override public int getPageEntrySize(int payloadLen, int show) {
            assert payloadLen > 0 : payloadLen;

            int res = payloadLen;

            if ((show & SHOW_LINK) != 0)
                res += LINK_SIZE;

            if ((show & SHOW_ITEM) != 0)
                res += ITEM_SIZE;

            if ((show & SHOW_PAYLOAD_LEN) != 0)
                res += PAYLOAD_LEN_SIZE;

            return res;
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
            assert dataOff >= ITEMS_OFF + ITEM_SIZE && dataOff < Integer.MAX_VALUE : dataOff;

            return dataOff;
        }

        @Override public void writeRowData(long pageAddr, int dataOff, byte[] payload) {
            PageUtils.putShort(pageAddr, dataOff, (short)payload.length);
            dataOff += PAYLOAD_LEN_SIZE;

            PageUtils.putBytes(pageAddr, dataOff, payload);
        }

        @Override public int offsetSize() {
            return 8;
        }

        @Override public int offsetMask() {
            return ~(-1 << offsetSize());
        }

        @Override public int itemSize() {
            return ITEM_SIZE;
        }

        @Override public int itemsOffset() {
            return ITEMS_OFF;
        }

        @Override public int payloadLenSize() {
            return PAYLOAD_LEN_SIZE;
        }

        @Override public boolean isEnoughSpace(int newEntryFullSize, int firstEntryOff, int directCnt, int indirectCnt) {
            return ITEMS_OFF + ITEM_SIZE * (directCnt + indirectCnt) <= firstEntryOff - newEntryFullSize;
        }

        @Override public ByteBuffer createFragment(PageMemory pageMem, long pageAddr, int dataOff, int payloadSize, long lastLink) {
            ByteBuffer buf = pageMem.pageBuffer(pageAddr);

            buf.position(dataOff);

            short p = (short)(payloadSize | FRAGMENTED_FLAG);

            buf.putShort(p);
            buf.putLong(lastLink);

            return buf;
        }

        @Override public void putFragment(long pageAddr, int dataOff, int payloadSize, byte[] payload, long lastLink) {
            PageUtils.putShort(pageAddr, dataOff, (short)(payloadSize | FRAGMENTED_FLAG));

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

            return PageUtils.getLong(pageAddr, dataOff + PAYLOAD_LEN_SIZE);
        }

        @Override public boolean isFragmented(long pageAddr, int dataOff) {
            return (PageUtils.getShort(pageAddr, dataOff) & FRAGMENTED_FLAG) != 0;
        }

        @Override public void setDirectCount(long pageAddr, int cnt) {
            assert checkCount(cnt) : cnt;

            PageUtils.putByte(pageAddr, DIRECT_CNT_OFF, (byte) cnt);
        }

        @Override public void setIndirectCount(long pageAddr, int cnt) {
            assert checkCount(cnt) : cnt;

            PageUtils.putByte(pageAddr, INDIRECT_CNT_OFF, (byte) cnt);
        }

        @Override public int minDataPageOverhead() {
            return MIN_DATA_PAGE_OVERHEAD;
        }

        @Override public void putPayloadSize(long addr, int offset, int payloadSize) {
            PageUtils.putShort(addr, 0, (short) payloadSize);
        }
    }
}
