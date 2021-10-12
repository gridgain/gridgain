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

package org.apache.ignite.internal.processors.cache.persistence.tree.io;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.cache.persistence.Storable;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMetrics;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageHandler;
import org.apache.ignite.internal.util.GridStringBuilder;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.util.GridUnsafe.bufferAddress;

/**
 * Data pages IO.
 *
 * Rows in a data page are organized into two arrays growing toward each other: items table and row data.
 * <p>
 * Items table contains direct or indirect items which locate a row within the page. Items table is stored at the
 * beginning of a page. Each item has an item ID which serves as an external item reference
 * (see {@link PageIdUtils#link(long, int)}) and can be either direct or indirect. The size of any item in the items
 * table is 2 bytes. ID of a direct item is always the same as its index in items table so it is not stored in the
 * item itself. ID of an indirect item may differ from its index (see example below) so it is stored it the item
 * along with ID (index) of direct item.
 * <p>
 * Direct and indirect items are always placed in the items table in such a way that direct items are stored first,
 * and indirect items are always stored after direct items. A data page explicitly stores both direct and indirect
 * items count (see {@link PageLayout#getDirectCount(long)} and {@link PageLayout#getIndirectCount(long)}), so that the item type can be
 * easily determined: items with indexes {@code [0, directCnt)} are always direct and items with indexes
 * {@code [directCnt, directCnt + indirectCnt)} are always indirect. Having both direct and indirect items in a
 * page allows page defragmentation without breaking external links. Total number of rows stored in a page is equal
 * to the number of direct items.
 * <p>
 * The row data is stored at the end of the page; newer rows are stored closer to the end of the items table.
 * <h3>Direct Items</h3>
 * Direct items refer a stored row directly by offset in the page:
 * <pre>
 *     +-----------------------------------------------------------------------------+
 *     | Direct Items             ..... (rows data)                                  |
 *     | (4000), (3800), (3600)   ..... row_2_cccc  row_1_bbbb   row_0_aaaa          |
 *     |  |       |       |_____________^           ^            ^                   |
 *     |  |       |_________________________________|            |                   |
 *     |  |______________________________________________________|                   |
 *     | directCnt: 3                                                                |
 *     | indirectCnt: 0                                                              |
 *     +-----------------------------------------------------------------------------+
 * </pre>
 * Direct item ID always matches it's index in the items table. The value of a direct item in the table is an
 * offset of the row data within the page.
 * <h3>Indirect Items</h3>
 * An indirect item explicitly stores the indirect item ID (1 byte) and the index of the direct item it refers to
 * (1 byte). The referred direct item (referrent) stores the actual offset of the row in the data page:
 * <pre>
 *     +-----------------------------------------------------------------------------+
 *     |  Direct Items .. Indirect items .... (rows data)                            |
 *     |   ____________________                                                      |
 *     |  ⌄                    |                                                     |
 *     | (3600), (3800), (3 -> 0)        ....   row_2_cccc  row_1_bbbb               |
 *     |  |       |_____________________________^___________^                        |
 *     |  |_____________________________________|                                    |
 *     | directCnt: 2                                                                |
 *     | indirectCount: 1                                                            |
 *     +-----------------------------------------------------------------------------+
 * </pre>
 * An indirect item can only be created as a result of row deletion. Note that indirect item ID does not
 * necessarily match the item index in the items table, however, indirect item IDs are always stored in sorted order
 * by construction. In the picture above, the page contains two rows which are referred by two items:
 * <ul>
 *     <li>{@code 1} is a direct item which is stored at index 1 in the items table</li>
 *     <li>{@code 3} is an indirect item which is stored at index 2 in the items table and refers to the direct
 *     item {@code 0}</li>
 * </ul>
 *
 * <h2>Items insertion and deletion</h2>
 * <p>
 * When rows are added to an empty page or a page with only direct items, the items table grows naturally:
 * <pre>
 *     +---------------------------------------------------------------+
 *     | Table index | 00 | 01 | 02 | 03 | 04 | 05 | 06 | 07 | 08 | 09 |
 *     +---------------------------------------------------------------+
 *     | Item        |  a |  b |  c |  d |  e |  f |  g |  h |  i |  j |
 *     +---------------------------------------------------------------+
 * </pre>
 *
 * When an item is removed, the items table is modified in such a way, that:
 * <ul>
 *     <li>Item of deleted row is modified to point to the row of the last direct item</li>
 *     <li>The last direct item is converted to an indirect item, pointing to the deleted item</li>
 * </ul>
 *
 * <pre>
 *     remove(itemId=07)
 *     +-----------------------------------------------------------------------+
 *     | Table index | 00 | 01 | 02 | 03 | 04 | 05 | 06 | 07 | 08 |         09 |
 *     +-----------------------------------------------------------------------+
 *     | Item        |  a |  b |  c |  d |  e |  f |  g |  j |  i | (09 -> 07) |
 *     +-----------------------------------------------------------------------+
 *
 *      remove(itemId=02)
 *     +--------------------------------------------------------------------------------+
 *     | Table index | 00 | 01 | 02 | 03 | 04 | 05 | 06 | 07 |          08 |         09 |
 *     +--------------------------------------------------------------------------------+
 *     | Item        |  a |  b |  i |  d |  e |  f |  g |  j |  (08 -> 02) | (09 -> 07) |
 *     +--------------------------------------------------------------------------------+
 * </pre>
 *
 * If the last direct item is already referred by an indirect item, that indirect item is updated and
 * all indirect items are shifted to the left by 1 effectively dropping last direct item:
 * <pre>
 *      remove(itemId=00)
 *     +--------------------------------------------------------------------------------+
 *     | Table index | 00 | 01 | 02 | 03 | 04 | 05 | 06 |          07 |         08 | 09 |
 *     +--------------------------------------------------------------------------------+
 *     | Item        |  j |  b |  i |  d |  e |  f |  g |  (08 -> 02) | (09 -> 00) |    |
 *     +--------------------------------------------------------------------------------+
 * </pre>
 *
 * When adding a row to a page with indirect items, the item is added to the end of direct items in the table and
 * indirect items are shifted to the right:
 * <pre>
 *      add(k)
 *     +-------------------------------------------------------------------------------+
 *     | Table index | 00 | 01 | 02 | 03 | 04 | 05 | 06 | 07 |         08 |         09 |
 *     +-------------------------------------------------------------------------------+
 *     | Item        |  j |  b |  i |  d |  e |  f |  g |  k | (08 -> 02) | (09 -> 00) |
 *     +-------------------------------------------------------------------------------+
 * </pre>
 *
 * If during an insert a newly added direct item ID matches with an existing indirect item ID, the corresponding
 * indirect item is converted to a direct item, and the row being inserted is represented by a direct item
 * with the referrent ID:
 * <pre>
 *      add(l)
 *     +-----------------------------------------------------------------------+
 *     | Table index | 00 | 01 | 02 | 03 | 04 | 05 | 06 | 07 | 08 |         09 |
 *     +-----------------------------------------------------------------------+
 *     | Item        |  j |  b |  l |  d |  e |  f |  g |  k |  i | (09 -> 00) |
 *     +-----------------------------------------------------------------------+
 * </pre>
 */
public abstract class AbstractDataPageIO<T extends Storable> extends PageIO implements CompactablePageIO {
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
    private static final int LINK_SIZE = 8;

    /**
     * @param type Page type.
     * @param ver Page format version.
     */
    protected AbstractDataPageIO(int type, int ver) {
        super(type, ver);
    }

    /** {@inheritDoc} */
    @Override public void initNewPage(long pageAddr, long pageId, PageLayout pageLayout, PageMetrics metrics) {
        super.initNewPage(pageAddr, pageId, pageLayout, metrics);

        setEmptyPage(pageAddr, pageLayout);
        setFreeListPageId(pageAddr, 0L);
    }

    /**
     * @param pageAddr Page address.
     * @param pageLayout Page layout.
     */
    private void setEmptyPage(long pageAddr, PageLayout pageLayout) {
        pageLayout.setDirectCount(pageAddr, 0);
        pageLayout.setIndirectCount(pageAddr, 0);

        int pageSize = pageLayout.pageSize();

        pageLayout.setFirstEntryOffset(pageAddr, pageSize);
        setRealFreeSpace(pageAddr, pageSize - pageLayout.itemsOffset(), pageLayout);
    }

    /**
     * @param pageAddr Page address.
     * @param freeListPageId Free list page ID.
     */
    public void setFreeListPageId(long pageAddr, long freeListPageId) {
        PageUtils.putLong(pageAddr, FREE_LIST_PAGE_ID_OFF, freeListPageId);
    }

    /**
     * @param pageAddr Page address.
     * @return Free list page ID.
     */
    public long getFreeListPageId(long pageAddr) {
        return PageUtils.getLong(pageAddr, FREE_LIST_PAGE_ID_OFF);
    }

    /**
     * @param pageAddr Page address.
     * @param freeSpace Free space.
     * @param pageLayout Page layout.
     */
    private void setRealFreeSpace(long pageAddr, int freeSpace, PageLayout pageLayout) {
        assert freeSpace == actualFreeSpace(pageAddr, pageLayout) : freeSpace + " != " + actualFreeSpace(pageAddr, pageLayout);

        pageLayout.setRealFreeSpace(pageAddr, freeSpace);
    }

    /**
     * @param pageLayout Page layout.
     * @param pageAddr Page address.
     * @return {@code true} If there is no useful data in this page.
     */
    public boolean isEmpty(PageLayout pageLayout, long pageAddr) {
        return pageLayout.getDirectCount(pageAddr) == 0;
    }

    /**
     * @param pageLayout Page layout.
     * @param pageAddr Page address.
     * @return Rows number in the given data page.
     */
    public int getRowsCount(PageLayout pageLayout, long pageAddr) {
        return pageLayout.getDirectCount(pageAddr);
    }

    /**
     * @param pageAddr Page address.
     * @param pageLayout Page layout.
     * @param c Closure.
     * @param <T> Closure return type.
     * @return Collection of closure results for all items in page.
     * @throws IgniteCheckedException In case of error in closure body.
     */
    public <T> List<T> forAllItems(long pageAddr, PageLayout pageLayout, CC<T> c) throws IgniteCheckedException {
        long pageId = getPageId(pageAddr);

        int cnt = pageLayout.getDirectCount(pageAddr);

        List<T> res = new ArrayList<>(cnt);

        for (int i = 0; i < cnt; i++) {
            long link = PageIdUtils.link(pageId, i);

            res.add(c.apply(link));
        }

        return res;
    }

    /**
     * @param pageAddr Page address.
     * @param itemId Fixed item ID (the index used for referencing an entry from the outside).
     * @param directCnt Direct items count.
     * @param indirectCnt Indirect items count.
     * @param pageLayout Page layout.
     * @return Found index of indirect item.
     */
    private int findIndirectItemIndex(long pageAddr, int itemId, int directCnt, int indirectCnt, PageLayout pageLayout) {
        int low = directCnt;
        int high = directCnt + indirectCnt - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;

            int cmp = Integer.compare(pageLayout.itemId(pageLayout.getItem(pageAddr, mid)), itemId);

            if (cmp < 0)
                low = mid + 1;
            else if (cmp > 0)
                high = mid - 1;
            else
                return mid; // found
        }

        throw new IllegalStateException("Item not found: " + itemId);
    }

    /**
     * @param pageAddr Page address.
     * @param pageLayout Page layout.
     * @return String representation.
     */
    private String printPageLayout(long pageAddr, PageLayout pageLayout) {
        SB b = new SB();

        printPageLayout(pageAddr, pageLayout, b);

        return b.toString();
    }

    /**
     * @param pageAddr Page address.
     * @param pageLayout Page layout.
     * @param b B.
     */
    protected void printPageLayout(long pageAddr, PageLayout pageLayout, GridStringBuilder b) {
        int directCnt = pageLayout.getDirectCount(pageAddr);
        int indirectCnt = pageLayout.getIndirectCount(pageAddr);
        int free = pageLayout.getRealFreeSpace(pageAddr);

        boolean valid = directCnt >= indirectCnt;

        b.appendHex(PageIO.getPageId(pageAddr)).a(" [");

        int entriesSize = 0;

        for (int i = 0; i < directCnt; i++) {
            if (i != 0)
                b.a(", ");

            int item = pageLayout.getItem(pageAddr, i);

            if (item < pageLayout.itemsOffset() || item >= pageLayout.pageSize())
                valid = false;

            entriesSize += pageLayout.getPageEntrySize(pageAddr, item, SHOW_PAYLOAD_LEN | SHOW_LINK);

            b.a(item);
        }

        b.a("][");

        Collection<Integer> set = new HashSet<>();

        for (int i = directCnt; i < directCnt + indirectCnt; i++) {
            if (i != directCnt)
                b.a(", ");

            int item = pageLayout.getItem(pageAddr, i);

            int itemId = pageLayout.itemId(item);
            int directIdx = pageLayout.directItemIndex(item);

            if (!set.add(directIdx) || !set.add(itemId))
                valid = false;

            assert pageLayout.indirectItem(itemId, directIdx) == item;

            if (itemId < directCnt || directIdx < 0 || directIdx >= directCnt)
                valid = false;

            if (i > directCnt && pageLayout.itemId(pageLayout.getItem(pageAddr, i - 1)) >= itemId)
                valid = false;

            b.a(itemId).a('^').a(directIdx);
        }

        b.a("][free=").a(free);

        int actualFree = pageLayout.pageSize() - pageLayout.itemsOffset()
            - (entriesSize + (directCnt + indirectCnt) * pageLayout.itemSize());

        if (free != actualFree) {
            b.a(", actualFree=").a(actualFree);

            valid = false;
        }
        else
            b.a("]");

        assert valid : b.toString();
    }

    /**
     * @param pageAddr Page address.
     * @param itemId Fixed item ID (the index used for referencing an entry from the outside).
     * @param pageLayout Page layout.
     * @return Data entry offset in bytes.
     */
    protected int getDataOffset(long pageAddr, int itemId, PageLayout pageLayout) {
        assert pageLayout.checkIndex(itemId) : itemId;

        int directCnt = pageLayout.getDirectCount(pageAddr);

        assert directCnt > 0 : "itemId=" + itemId + ", directCnt=" + directCnt + ", page=" + printPageLayout(pageAddr, pageLayout);

        if (itemId >= directCnt) { // Need to do indirect lookup.
            int indirectCnt = pageLayout.getIndirectCount(pageAddr);

            // Must have indirect items here.
            assert indirectCnt > 0 : "itemId=" + itemId + ", directCnt=" + directCnt + ", indirectCnt=" + indirectCnt +
                ", page=" + printPageLayout(pageAddr, pageLayout);

            int indirectItemIdx = findIndirectItemIndex(pageAddr, itemId, directCnt, indirectCnt, pageLayout);

            assert indirectItemIdx >= directCnt : indirectItemIdx + " " + directCnt;
            assert indirectItemIdx < directCnt + indirectCnt : indirectItemIdx + " " + directCnt + " " + indirectCnt;

            itemId = pageLayout.directItemIndex(pageLayout.getItem(pageAddr, indirectItemIdx));

            assert itemId >= 0 && itemId < directCnt : itemId + " " + directCnt + " " + indirectCnt; // Direct item.
        }

        return pageLayout.directItemToOffset(pageLayout.getItem(pageAddr, itemId));
    }

    /**
     * Sets position to start of actual fragment data and limit to it's end.
     *
     * @param pageAddr Page address.
     * @param itemId Item to position on.
     * @param pageLayout Page layout.
     * @return {@link DataPagePayload} object.
     */
    public DataPagePayload readPayload(final long pageAddr, final int itemId, final PageLayout pageLayout) {
        int dataOff = getDataOffset(pageAddr, itemId, pageLayout);

        boolean fragmented = pageLayout.isFragmented(pageAddr, dataOff);
        long nextLink = fragmented ? pageLayout.getNextFragmentLink(pageAddr, dataOff) : 0;
        int payloadSize = pageLayout.getPageEntrySize(pageAddr, dataOff, 0);

        return new DataPagePayload(
            dataOff + pageLayout.payloadLenSize() + (fragmented ? LINK_SIZE : 0),
            payloadSize,
            nextLink
        );
    }

    /**
     * @param pageAddr Page address.
     * @param itemId Item to position on.
     * @param pageLayout Page layout.
     * @param reqLen Required payload length.
     * @return Offset to start of actual fragment data.
     */
    public int getPayloadOffset(final long pageAddr, final int itemId, final PageLayout pageLayout, int reqLen) {
        int dataOff = getDataOffset(pageAddr, itemId, pageLayout);

        int payloadSize = pageLayout.getPageEntrySize(pageAddr, dataOff, 0);

        assert payloadSize >= reqLen : payloadSize;

        return dataOff + pageLayout.payloadLenSize() + (pageLayout.isFragmented(pageAddr, dataOff) ? LINK_SIZE : 0);
    }

    /**
     * Move the last direct item to the free slot and reference it with indirect item on the same place.
     *
     * @param pageAddr Page address.
     * @param freeDirectIdx Free slot.
     * @param directCnt Direct items count.
     * @param indirectCnt Indirect items count.
     * @return {@code true} If the last direct item already had corresponding indirect item.
     */
    private boolean moveLastItem(
        long pageAddr,
        int freeDirectIdx,
        int directCnt,
        int indirectCnt,
        PageLayout pageLayout
    ) {
        int lastIndirectId = findIndirectIndexForLastDirect(pageAddr, directCnt, indirectCnt, pageLayout);

        int lastItemId = directCnt - 1;

        assert lastItemId != freeDirectIdx;

        int indirectItem = pageLayout.indirectItem(lastItemId, freeDirectIdx);

        assert pageLayout.itemId(indirectItem) == lastItemId && pageLayout.directItemIndex(indirectItem) == freeDirectIdx;

        pageLayout.setItem(pageAddr, freeDirectIdx, pageLayout.getItem(pageAddr, lastItemId));
        pageLayout.setItem(pageAddr, lastItemId, indirectItem);

        assert pageLayout.getItem(pageAddr, lastItemId) == indirectItem;

        if (lastIndirectId != -1) { // Fix pointer to direct item.
            pageLayout.setItem(
                pageAddr,
                lastIndirectId,
                pageLayout.indirectItem(pageLayout.itemId(pageLayout.getItem(pageAddr, lastIndirectId)), freeDirectIdx)
            );

            return true;
        }

        return false;
    }

    /**
     * @param pageAddr Page address.
     * @param directCnt Direct items count.
     * @param indirectCnt Indirect items count.
     * @param pageLayout Page layout.
     * @return Index of indirect item for the last direct item.
     */
    private int findIndirectIndexForLastDirect(
        long pageAddr,
        int directCnt,
        int indirectCnt,
        PageLayout pageLayout
    ) {
        int lastDirectId = directCnt - 1;

        for (int i = directCnt, end = directCnt + indirectCnt; i < end; i++) {
            int item = pageLayout.getItem(pageAddr, i);

            if (pageLayout.directItemIndex(item) == lastDirectId)
                return i;
        }

        return -1;
    }

    /**
     * @param pageAddr Page address.
     * @param itemId Item ID.
     * @param pageLayout Page layout.
     * @param payload Row data.
     * @param row Row.
     * @param rowSize Row size.
     * @return {@code True} if entry is not fragmented.
     * @throws IgniteCheckedException If failed.
     */
    public boolean updateRow(
        final long pageAddr,
        int itemId,
        PageLayout pageLayout,
        @Nullable byte[] payload,
        @Nullable T row,
        final int rowSize
    ) throws IgniteCheckedException {
        assert pageLayout.checkIndex(itemId) : itemId;
        assert row != null ^ payload != null;

        final int dataOff = getDataOffset(pageAddr, itemId, pageLayout);

        if (pageLayout.isFragmented(pageAddr, dataOff))
            return false;

        if (row != null)
            writeRowData(pageLayout, pageAddr, dataOff, rowSize, row, false);
        else
            pageLayout.writeRowData(pageAddr, dataOff, payload);

        return true;
    }

    /**
     * @param pageAddr Page address.
     * @param itemId Fixed item ID (the index used for referencing an entry from the outside).
     * @param pageLayout Page layout.
     * @return Next link for fragmented entries or {@code 0} if none.
     * @throws IgniteCheckedException If failed.
     */
    public long removeRow(
        long pageAddr,
        int itemId,
        PageLayout pageLayout
    ) throws IgniteCheckedException {
        assert pageLayout.checkIndex(itemId) : itemId;

        final int dataOff = getDataOffset(pageAddr, itemId, pageLayout);
        final long nextLink = pageLayout.isFragmented(pageAddr, dataOff)
            ? pageLayout.getNextFragmentLink(pageAddr, dataOff) : 0;

        // Record original counts to calculate delta in free space in the end of remove.
        final int directCnt = pageLayout.getDirectCount(pageAddr);
        final int indirectCnt = pageLayout.getIndirectCount(pageAddr);

        int curIndirectCnt = indirectCnt;

        assert directCnt > 0 : directCnt; // Direct count always represents overall number of live items.

        // Remove the last item on the page.
        if (directCnt == 1) {
            assert (indirectCnt == 0 && itemId == 0) ||
                (indirectCnt == 1 && itemId == pageLayout.itemId(pageLayout.getItem(pageAddr, 1))) : itemId;

            setEmptyPage(pageAddr, pageLayout);
        }
        else {
            // Get the entry size before the actual remove.
            int rmvEntrySize = pageLayout.getPageEntrySize(pageAddr, dataOff, SHOW_PAYLOAD_LEN | SHOW_LINK);

            int indirectId = 0;

            if (itemId >= directCnt) { // Need to remove indirect item.
                assert indirectCnt > 0;

                indirectId = findIndirectItemIndex(pageAddr, itemId, directCnt, indirectCnt, pageLayout);

                assert indirectId >= directCnt;

                itemId = pageLayout.directItemIndex(pageLayout.getItem(pageAddr, indirectId));

                assert itemId < directCnt;
            }

            boolean dropLast = true;

            if (itemId + 1 < directCnt) // It is not the last direct item.
                dropLast = moveLastItem(pageAddr, itemId, directCnt, indirectCnt, pageLayout);

            if (indirectId == 0) { // For the last direct item with no indirect item.
                if (dropLast)
                    moveItems(pageAddr, directCnt, indirectCnt, -1, pageLayout);
                else
                    curIndirectCnt++;
            }
            else {
                if (dropLast)
                    moveItems(pageAddr, directCnt, indirectId - directCnt, -1, pageLayout);

                moveItems(pageAddr, indirectId + 1, directCnt + indirectCnt - indirectId - 1, dropLast ? -2 : -1, pageLayout);

                if (dropLast)
                    curIndirectCnt--;
            }

            pageLayout.setIndirectCount(pageAddr, curIndirectCnt);
            pageLayout.setDirectCount(pageAddr, directCnt - 1);

            assert pageLayout.getIndirectCount(pageAddr) <= pageLayout.getDirectCount(pageAddr);

            // Increase free space.
            int realFreeSpace = pageLayout.getRealFreeSpace(pageAddr);

            int items = directCnt - pageLayout.getDirectCount(pageAddr) + indirectCnt - pageLayout.getIndirectCount(pageAddr);

            setRealFreeSpace(pageAddr, realFreeSpace + rmvEntrySize + pageLayout.itemSize() * items, pageLayout);
        }

        return nextLink;
    }

    /**
     * @param pageAddr Page address.
     * @param idx Index.
     * @param cnt Count.
     * @param step Step.
     * @param pageLayout Page layout.
     */
    private void moveItems(long pageAddr, int idx, int cnt, int step, PageLayout pageLayout) {
        assert cnt >= 0 : cnt;

        if (cnt != 0) {
            moveBytes(
                pageAddr,
                pageLayout.itemOffset(idx),
                cnt * pageLayout.itemSize(),
                step * pageLayout.itemSize(),
                pageLayout.pageSize()
            );
        }
    }

    public int getFreeSpace(long pageAddr, PageLayout pageLayout) {
        return pageLayout.getFreeSpace(pageAddr);
    }

    /**
     * Adds row to this data page and sets respective link to the given row object.
     *
     * @param pageAddr Page address.
     * @param row Data row.
     * @param rowSize Row size.
     * @param pageLayout Page layout.
     * @throws IgniteCheckedException If failed.
     */
    public void addRow(
        final long pageId,
        final long pageAddr,
        T row,
        final int rowSize,
        final PageLayout pageLayout
    ) throws IgniteCheckedException {
        assert rowSize <= pageLayout.getFreeSpace(pageAddr) : "can't call addRow if not enough space for the whole row";

        int fullEntrySize = pageLayout.getPageEntrySize(rowSize, SHOW_PAYLOAD_LEN | SHOW_ITEM);

        int directCnt = pageLayout.getDirectCount(pageAddr);
        int indirectCnt = pageLayout.getIndirectCount(pageAddr);

        int dataOff = getDataOffsetForWrite(pageAddr, fullEntrySize, directCnt, indirectCnt, pageLayout);

        writeRowData(pageLayout, pageAddr, dataOff, rowSize, row, true);

        int itemId = addItem(pageAddr, fullEntrySize, directCnt, indirectCnt, dataOff, pageLayout);

        setLinkByPageId(row, pageId, itemId);
    }

    /**
     * Adds row to this data page and sets respective link to the given row object.
     *
     * @param pageAddr Page address.
     * @param payload Payload.
     * @param pageLayout Page layout.
     * @return Item ID.
     * @throws IgniteCheckedException If failed.
     */
    public int addRow(
        long pageAddr,
        byte[] payload,
        PageLayout pageLayout
    ) throws IgniteCheckedException {
        assert payload.length <= pageLayout.getFreeSpace(pageAddr) : "can't call addRow if not enough space for the whole row";

        int fullEntrySize = pageLayout.getPageEntrySize(payload.length, SHOW_PAYLOAD_LEN | SHOW_ITEM);

        int directCnt = pageLayout.getDirectCount(pageAddr);
        int indirectCnt = pageLayout.getIndirectCount(pageAddr);

        int dataOff = getDataOffsetForWrite(pageAddr, fullEntrySize, directCnt, indirectCnt, pageLayout);

        pageLayout.writeRowData(pageAddr, dataOff, payload);

        return addItem(pageAddr, fullEntrySize, directCnt, indirectCnt, dataOff, pageLayout);
    }

    /**
     * @param pageAddr Page address.
     * @param entryFullSize New entry full size (with item, length and link).
     * @param directCnt Direct items count.
     * @param indirectCnt Indirect items count.
     * @param dataOff First entry offset.
     * @param pageLayout Page layout.
     * @return First entry offset after compaction.
     */
    private int compactIfNeed(
        final long pageAddr,
        final int entryFullSize,
        final int directCnt,
        final int indirectCnt,
        int dataOff,
        PageLayout pageLayout
    ) {
        if (!pageLayout.isEnoughSpace(entryFullSize, dataOff, directCnt, indirectCnt)) {
            dataOff = compactDataEntries(pageAddr, directCnt, pageLayout);

            assert pageLayout.isEnoughSpace(entryFullSize, dataOff, directCnt, indirectCnt);
        }

        return dataOff;
    }

    /**
     * Put item reference on entry.
     *
     * @param pageAddr Page address.
     * @param fullEntrySize Full entry size (with link, payload size and item).
     * @param directCnt Direct items count.
     * @param indirectCnt Indirect items count.
     * @param dataOff Data offset.
     * @param pageLayout Page layout.
     * @return Item ID.
     */
    private int addItem(
        final long pageAddr,
        final int fullEntrySize,
        final int directCnt,
        final int indirectCnt,
        final int dataOff,
        final PageLayout pageLayout
    ) {
        pageLayout.setFirstEntryOffset(pageAddr, dataOff);

        int itemId = insertItem(pageAddr, dataOff, directCnt, indirectCnt, pageLayout);

        assert pageLayout.checkIndex(itemId) : itemId;
        assert pageLayout.getIndirectCount(pageAddr) <= pageLayout.getDirectCount(pageAddr);

        // Update free space. If number of indirect items changed, then we were able to reuse an item slot.
        int space = pageLayout.getRealFreeSpace(pageAddr) - fullEntrySize
            + (pageLayout.getIndirectCount(pageAddr) != indirectCnt ? pageLayout.itemSize() : 0);

        setRealFreeSpace(pageAddr, space, pageLayout);

        return itemId;
    }

    /**
     * @param pageAddr Page address.
     * @param fullEntrySize Full entry size.
     * @param directCnt Direct items count.
     * @param indirectCnt Indirect items count.
     * @param pageLayout Page layout.
     * @return Offset in the buffer where the entry must be written.
     */
    private int getDataOffsetForWrite(
        long pageAddr,
        int fullEntrySize,
        int directCnt,
        int indirectCnt,
        PageLayout pageLayout
    ) {
        int dataOff = pageLayout.getFirstEntryOffset(pageAddr);

        // Compact if we do not have enough space for entry.
        dataOff = compactIfNeed(pageAddr, fullEntrySize, directCnt, indirectCnt, dataOff, pageLayout);

        // We will write data right before the first entry.
        dataOff -= fullEntrySize - pageLayout.itemSize();

        return dataOff;
    }

    /**
     * Adds maximum possible fragment of the given row to this data page and sets respective link to the row.
     *
     * @param pageMem Page memory.
     * @param pageId Page ID to use to construct a link.
     * @param pageAddr Page address.
     * @param row Data row.
     * @param written Number of bytes of row size that was already written.
     * @param rowSize Row size.
     * @param pageLayout Page layout.
     * @return Written payload size.
     * @throws IgniteCheckedException If failed.
     */
    public int addRowFragment(
        PageMemory pageMem,
        long pageId,
        long pageAddr,
        T row,
        int written,
        int rowSize,
        PageLayout pageLayout
    ) throws IgniteCheckedException {
        return addRowFragment(pageMem, pageId, pageAddr, written, rowSize, row.link(), row, null, pageLayout);
    }

    /**
     * Adds this payload as a fragment to this data page.
     *
     * @param pageId Page ID to use to construct a link.
     * @param pageAddr Page address.
     * @param payload Payload bytes.
     * @param lastLink Link to the previous written fragment (link to the tail).
     * @param pageLayout Page layout.
     * @throws IgniteCheckedException If failed.
     */
    public void addRowFragment(
        long pageId,
        long pageAddr,
        byte[] payload,
        long lastLink,
        PageLayout pageLayout
    ) throws IgniteCheckedException {
        addRowFragment(null, pageId, pageAddr, 0, 0, lastLink, null, payload, pageLayout);
    }

    /**
     * Adds maximum possible fragment of the given row to this data page and sets respective link to the row.
     *
     * @param pageMem Page memory.
     * @param pageId Page ID to use to construct a link.
     * @param pageAddr Page address.
     * @param written Number of bytes of row size that was already written.
     * @param rowSize Row size.
     * @param lastLink Link to the previous written fragment (link to the tail).
     * @param row Row.
     * @param payload Payload bytes.
     * @param pageLayout Page layout.
     * @return Written payload size.
     * @throws IgniteCheckedException If failed.
     */
    private int addRowFragment(
        PageMemory pageMem,
        long pageId,
        long pageAddr,
        int written,
        int rowSize,
        long lastLink,
        T row,
        byte[] payload,
        PageLayout pageLayout
    ) throws IgniteCheckedException {
        assert payload == null ^ row == null;

        int directCnt = pageLayout.getDirectCount(pageAddr);
        int indirectCnt = pageLayout.getIndirectCount(pageAddr);

        int payloadSize = payload != null ?
            payload.length : Math.min(rowSize - written, pageLayout.getFreeSpace(pageAddr));

        if (row != null) {
            int remain = rowSize - written - payloadSize;
            int hdrSize = row.headerSize();

            // We need page header (i.e. MVCC info) is located entirely on the very first page in chain.
            // So we force moving it to the next page if it could not fit entirely on this page.
            if (remain > 0 && remain < hdrSize)
                payloadSize -= hdrSize - remain;
        }

        int fullEntrySize = pageLayout.getPageEntrySize(payloadSize, SHOW_PAYLOAD_LEN | SHOW_LINK | SHOW_ITEM);
        int dataOff = getDataOffsetForWrite(pageAddr, fullEntrySize, directCnt, indirectCnt, pageLayout);

        if (payload == null) {
            ByteBuffer buf = pageLayout.createFragment(pageMem, pageAddr, dataOff, payloadSize, lastLink);

            int rowOff = rowSize - written - payloadSize;

            writeFragmentData(row, buf, rowOff, payloadSize);
        }
        else
            pageLayout.putFragment(pageAddr, dataOff, payloadSize, payload, lastLink);

        int itemId = addItem(pageAddr, fullEntrySize, directCnt, indirectCnt, dataOff, pageLayout);

        if (row != null)
            setLinkByPageId(row, pageId, itemId);

        return payloadSize;
    }

    /**
     * @param row Row to set link to.
     * @param pageId Page ID.
     * @param itemId Item ID.
     */
    private void setLinkByPageId(T row, long pageId, int itemId) {
        row.link(PageIdUtils.link(pageId, itemId));
    }

    /**
     * Write row data fragment.
     *
     * @param row Row.
     * @param buf Byte buffer.
     * @param rowOff Offset in row data bytes.
     * @param payloadSize Data length that should be written in a fragment.
     * @throws IgniteCheckedException If failed.
     */
    protected abstract void writeFragmentData(
        final T row,
        final ByteBuffer buf,
        final int rowOff,
        final int payloadSize
    ) throws IgniteCheckedException;

    /**
     * @param pageAddr Page address.
     * @param dataOff Data offset.
     * @param directCnt Direct items count.
     * @param indirectCnt Indirect items count.
     * @param pageLayout Page layout.
     * @return Item ID (insertion index).
     */
    private int insertItem(long pageAddr, int dataOff, int directCnt, int indirectCnt, PageLayout pageLayout) {
        if (indirectCnt > 0) {
            // If the first indirect item is on correct place to become the last direct item, do the transition
            // and insert the new item into the free slot which was referenced by this first indirect item.
            int item = pageLayout.getItem(pageAddr, directCnt);

            if (pageLayout.itemId(item) == directCnt) {
                int directItemIdx = pageLayout.directItemIndex(item);

                pageLayout.setItem(pageAddr, directCnt, pageLayout.getItem(pageAddr, directItemIdx));
                pageLayout.setItem(pageAddr, directItemIdx, pageLayout.directItemFromOffset(dataOff));

                pageLayout.setDirectCount(pageAddr, directCnt + 1);
                pageLayout.setIndirectCount(pageAddr, indirectCnt - 1);

                return directItemIdx;
            }
        }

        // Move all the indirect items forward to make a free slot and insert new item at the end of direct items.
        moveItems(pageAddr, directCnt, indirectCnt, +1, pageLayout);

        pageLayout.setItem(pageAddr, directCnt, pageLayout.directItemFromOffset(dataOff));

        pageLayout.setDirectCount(pageAddr, directCnt + 1);
        assert pageLayout.getDirectCount(pageAddr) == directCnt + 1;

        return directCnt; // Previous directCnt will be our itemId.
    }

    /** {@inheritDoc} */
    @Override public void compactPage(ByteBuffer page, ByteBuffer out, PageLayout pageLayout) {
        // TODO May we compactDataEntries in-place and then copy compacted data to out?
        copyPage(page, out, pageLayout.pageSize());

        long pageAddr = bufferAddress(out);

        int freeSpace = pageLayout.getRealFreeSpace(pageAddr);

        if (freeSpace == 0)
            return; // No garbage: nothing to compact here.

        int directCnt = pageLayout.getDirectCount(pageAddr);

        int pageSize = pageLayout.pageSize();

        if (directCnt != 0) {
            int firstOff = pageLayout.getFirstEntryOffset(pageAddr);

            if (firstOff - freeSpace != getHeaderSizeWithItems(pageAddr, directCnt, pageLayout)) {
                firstOff = compactDataEntries(pageAddr, directCnt, pageLayout);
                pageLayout.setFirstEntryOffset(pageAddr, firstOff);
            }

            // Move all the data entries from page end to the page header to close the gap.
            moveBytes(pageAddr, firstOff, pageSize - firstOff, -freeSpace, pageLayout.pageSize());
        }

        out.limit(pageSize - freeSpace); // Here we have only meaningful data of this page.
    }

    /** {@inheritDoc} */
    @Override public void restorePage(ByteBuffer page, PageLayout pageLayout) {
        int pageSize = pageLayout.pageSize();

        assert page.isDirect();
        assert page.position() == 0;
        assert page.limit() <= pageSize;

        long pageAddr = bufferAddress(page);

        int freeSpace = pageLayout.getRealFreeSpace(pageAddr);

        if (freeSpace != 0) {
            int firstOff = pageLayout.getFirstEntryOffset(pageAddr);
            int cnt = pageSize - firstOff;

            if (cnt != 0) {
                int off = page.limit() - cnt;

                assert off > PageIO.COMMON_HEADER_END : off;
                assert cnt > 0 : cnt;

                moveBytes(pageAddr, off, cnt, freeSpace, pageSize);
            }
        }

        page.limit(pageSize);
    }

    /**
     * @param pageAddr Page address.
     * @param directCnt Direct items count.
     * @param pageLayout Page layout.
     * @return New first entry offset.
     */
    private int compactDataEntries(long pageAddr, int directCnt, PageLayout pageLayout) {
        assert pageLayout.checkCount(directCnt) : directCnt;

        long[] offs = new long[directCnt];

        int offsetSize = pageLayout.offsetSize();

        for (int i = 0; i < directCnt; i++) {
            long off = pageLayout.directItemToOffset(pageLayout.getItem(pageAddr, i));

            // This way we'll be able to sort by offset using Arrays.sort(...).
            offs[i] = (off << offsetSize) | i;
        }

        Arrays.sort(offs);

        // Move right all of the entries if possible to make the page as compact as possible to its tail.
        int prevOff = pageLayout.pageSize();

        final int start = directCnt - 1;
        int curOff = (int) (offs[start] >>> offsetSize);
        int curEntrySize = pageLayout.getPageEntrySize(pageAddr, curOff, SHOW_PAYLOAD_LEN | SHOW_LINK);

        for (int i = start; i >= 0; i--) {
            assert curOff < prevOff : curOff;

            int delta = prevOff - (curOff + curEntrySize);

            int off = curOff;
            int entrySize = curEntrySize;

            if (delta != 0) { // Move right.
                assert delta > 0 : delta;

                int itemId = (int) (offs[i] & pageLayout.offsetMask());

                pageLayout.setItem(pageAddr, itemId, pageLayout.directItemFromOffset(curOff + delta));

                for (int j = i - 1; j >= 0; j--) {
                    int offNext = (int) (offs[j] >>> offsetSize);
                    int nextSize = pageLayout.getPageEntrySize(pageAddr, offNext, SHOW_PAYLOAD_LEN | SHOW_LINK);

                    if (offNext + nextSize == off) {
                        i--;

                        off = offNext;
                        entrySize += nextSize;

                        itemId = (int) (offs[j] & pageLayout.offsetMask());
                        pageLayout.setItem(pageAddr, itemId, pageLayout.directItemFromOffset(offNext + delta));
                    }
                    else {
                        curOff = offNext;
                        curEntrySize = nextSize;

                        break;
                    }
                }

                moveBytes(pageAddr, off, entrySize, delta, pageLayout.pageSize());

                off += delta;
            }
            else if (i > 0) {
                curOff = (int) (offs[i - 1] >>> offsetSize);
                curEntrySize = pageLayout.getPageEntrySize(pageAddr, curOff, SHOW_PAYLOAD_LEN | SHOW_LINK);
            }

            prevOff = off;
        }

        return prevOff;
    }

    /**
     * Full-scan free space calculation procedure.
     *
     * @param pageAddr Page to scan.
     * @param pageLayout Page layout.
     * @return Actual free space in the buffer.
     */
    private int actualFreeSpace(long pageAddr, PageLayout pageLayout) {
        int directCnt = pageLayout.getDirectCount(pageAddr);

        int entriesSize = 0;

        for (int i = 0; i < directCnt; i++) {
            int off = pageLayout.directItemToOffset(pageLayout.getItem(pageAddr, i));

            int entrySize = pageLayout.getPageEntrySize(pageAddr, off, SHOW_PAYLOAD_LEN | SHOW_LINK);

            entriesSize += entrySize;
        }

        return pageLayout.pageSize() - entriesSize - getHeaderSizeWithItems(pageAddr, directCnt, pageLayout);
    }

    /**
     * @param pageAddr Page address.
     * @param directCnt Direct items count.
     * @param pageLayout Page layout.
     * @return Size of the page header including all items.
     */
    private int getHeaderSizeWithItems(
        long pageAddr,
        int directCnt,
        PageLayout pageLayout
    ) {
        return pageLayout.itemsOffset() + (directCnt + pageLayout.getIndirectCount(pageAddr)) * pageLayout.itemSize();
    }

    /**
     * @param addr Address.
     * @param off Offset.
     * @param cnt Count.
     * @param step Step.
     * @param pageSize Page size.
     */
    private void moveBytes(long addr, int off, int cnt, int step, int pageSize) {
        assert cnt >= 0 : cnt;
        assert step != 0 : step;
        assert off + step >= 0;
        assert off + step + cnt <= pageSize : "[off=" + off + ", step=" + step + ", cnt=" + cnt +
            ", cap=" + pageSize + ']';

        PageHandler.copyMemory(addr, off, addr, off + step, cnt);
    }

    /**
     * @param pageLayout Page layout.
     * @param pageAddr Page address.
     * @param dataOff Data offset.
     * @param payloadSize Payload size.
     * @param row Data row.
     * @param newRow {@code False} if existing cache entry is updated, in this case skip key data write.
     * @throws IgniteCheckedException If failed.
     */
    protected abstract void writeRowData(
        PageLayout pageLayout,
        long pageAddr,
        int dataOff,
        int payloadSize,
        T row,
        boolean newRow
    ) throws IgniteCheckedException;

    /**
     * Defines closure interface for applying computations to data page items.
     *
     * @param <T> Closure return type.
     */
    public interface CC<T> {
        /**
         * Closure body.
         *
         * @param link Link to item.
         * @return Closure return value.
         * @throws IgniteCheckedException In case of error in closure body.
         */
        public T apply(long link) throws IgniteCheckedException;
    }
}
