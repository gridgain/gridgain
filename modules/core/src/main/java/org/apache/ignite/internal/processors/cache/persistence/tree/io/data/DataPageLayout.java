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

import static org.apache.ignite.internal.processors.cache.persistence.tree.io.AbstractDataPageIO.LINK_SIZE;
import static org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO.COMMON_HEADER_END;

public abstract class DataPageLayout {
    /**
     *
     */
    protected static final int SHOW_ITEM = 0b0001;

    /**
     *
     */
    protected static final int SHOW_PAYLOAD_LEN = 0b0010;

    /**
     *
     */
    protected static final int SHOW_LINK = 0b0100;

    /**
     *
     */
    protected static final int FREE_LIST_PAGE_ID_OFF = COMMON_HEADER_END;

    /**
     *
     */
    protected static final int FREE_SPACE_OFF = FREE_LIST_PAGE_ID_OFF + 8;

    protected final int directCntSize;

    protected final int maxIndex;

    /**
     *
     */
    protected final int directCntOff;

    /**
     *
     */
    protected final int indirectCntOff;

    /**
     *
     */
    protected final int firstEntryOff;

    /**
     *
     */
    protected final int itemsOff;

    /**
     *
     */
    protected final int itemSize;

    /**
     *
     */
    protected final int payloadLenSize;

    /**
     *
     */
    protected final int fragmentedFlag;

    /**
     *
     */
    protected final int minDataPageOverhead;

    protected DataPageLayout(
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
        this.directCntOff = DataPageLayout.FREE_SPACE_OFF + freeSpaceSize;
        this.indirectCntOff = directCntOff + directCntSize;
        this.firstEntryOff = indirectCntOff + indirectCntSize;
        this.itemsOff = firstEntryOff + firstEntrySize;
        this.itemSize = itemSize;
        this.payloadLenSize = payloadLenSize;
        this.fragmentedFlag = fragmentedFlag;
        this.minDataPageOverhead = itemsOff + itemSize + payloadLenSize + LINK_SIZE;
    }

    /**
     * @param pageAddr Page address.
     * @param idx Item index.
     * @return Item.
     */
    public abstract int getItem(long pageAddr, int idx);

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

    /**
     * @param pageAddr Page address.
     * @return Direct count.
     */
    public abstract int getDirectCount(long pageAddr);

    /**
     * @param pageAddr Page address.
     * @return Indirect count.
     */
    public abstract int getIndirectCount(long pageAddr);

    /**
     * Free space refers to a "max row size (without any data page specific overhead) which is guaranteed to fit into
     * this data page".
     *
     * @param pageAddr Page address.
     * @return Free space.
     */
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

    /**
     * @see PageLayout#getFirstEntryOffset(long)
     */
    public abstract int getFirstEntryOffset(long pageAddr);

    /**
     * Reads saved value of free space.
     *
     * @param pageAddr Page address.
     * @return Free space.
     */
    public abstract int getRealFreeSpace(long pageAddr);

    /**
     * @see PageLayout#setRealFreeSpace(long, int)
     */
    public abstract void setRealFreeSpace(long pageAddr, int freeSpace);

    /**
     * @see PageLayout#directItemIndex(int)
     */
    public abstract int directItemIndex(int indirectItem);

    /**
     * @see PageLayout#directItemToOffset(int)
     */
    public abstract int directItemToOffset(int directItem);

    /**
     * @param dataOff Data offset.
     * @return Direct item.
     */
    public abstract int directItemFromOffset(int dataOff);

    /**
     * @param pageAddr Page address.
     * @param dataOff Data offset.
     * @param payload Payload
     */
    public abstract void writeRowData(long pageAddr, int dataOff, byte[] payload);

    /**
     * @see PageLayout#offsetSize()
     */
    public int offsetSize() {
        return directCntSize * 8;
    }

    /**
     * @see PageLayout#offsetMask()
     */
    public int offsetMask() {
        return maxIndex;
    }

    /**
     * @see PageLayout#itemSize()
     */
    public int itemSize() {
        return itemSize;
    }

    /**
     * @see PageLayout#itemsOffset()
     */
    public int itemsOffset() {
        return itemsOff;
    }

    /**
     * @see PageLayout#payloadLenSize()
     */
    public int payloadLenSize() {
        return payloadLenSize;
    }

    /**
     * @param newEntryFullSize New entry full size (with item, length and link).
     * @param firstEntryOff First entry data offset.
     * @param directCnt Direct items count.
     * @param indirectCnt Indirect items count.
     * @return {@code true} If there is enough space for the entry.
     */
    public boolean isEnoughSpace(int newEntryFullSize, int firstEntryOff, int directCnt, int indirectCnt) {
        return itemsOff + itemSize * (directCnt + indirectCnt) <= firstEntryOff - newEntryFullSize;
    }

    public abstract ByteBuffer createFragment(PageMemory pageMem, long pageAddr, int dataOff, int payloadSize, long lastLink);

    public abstract void putFragment(long pageAddr, int dataOff, int payloadSize, byte[] payload, long lastLink);

    /**
     * @param pageAddr Page address.
     * @param idx Item index.
     * @param item Item.
     */
    public abstract void setItem(long pageAddr, int idx, int item);

    /**
     * @param itemId Fixed item ID (the index used for referencing an entry from the outside).
     * @param directItemIdx Index of corresponding direct item.
     * @return Indirect item.
     */
    public abstract int indirectItem(int itemId, int directItemIdx);

    /**
     * @param pageAddr Page address.
     * @param dataOff Points to the entry start.
     * @return Link to the next entry fragment or 0 if no fragments left or if entry is not fragmented.
     */
    public abstract long getNextFragmentLink(long pageAddr, int dataOff);

    /**
     * @param pageAddr Page address.
     * @param dataOff Data offset.
     * @return {@code true} If the data row is fragmented across multiple pages.
     */
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

    /**
     * @param pageAddr Page address.
     * @param dataOff Data offset.
     * @param show What elements of data page entry to show in the result.
     * @return Data page entry size.
     */
    public abstract int getPageEntrySize(long pageAddr, int dataOff, int show);

    /**
     * @param payloadLen Length of the payload, may be a full data row or a row fragment length.
     * @param show What elements of data page entry to show in the result.
     * @return Data page entry size.
     */
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

    public static boolean shouldBeExtended(int pageSize) {
        return pageSize > (16 * 1024);
    }
}
