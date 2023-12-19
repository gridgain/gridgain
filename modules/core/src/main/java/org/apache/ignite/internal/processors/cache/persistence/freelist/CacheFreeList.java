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

package org.apache.ignite.internal.processors.cache.persistence.freelist;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.metric.IoStatisticsHolder;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.record.delta.DataPageUpdateRecord;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageLockTrackerManager;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPagePayload;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageHandler;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * FreeList implementation for cache.
 */
public class CacheFreeList extends AbstractFreeList<CacheDataRow> {
    /** Page handler that is used to update the expiration time of CacheDataRow. */
    private final PageHandler<CacheDataRow, Boolean> updateDataRowTtl = new UpdateRowMetaHandler();

    /**
     * @param cacheGrpId Cache group id.
     * @param name Name.
     * @param dataRegion Data region.
     * @param wal Wal.
     * @param metaPageId Meta page id.
     * @param initNew Initialize new.
     * @param pageFlag Default flag value for allocated pages.
     */
    public CacheFreeList(
        int cacheGrpId,
        String name,
        DataRegion dataRegion,
        @Nullable IgniteWriteAheadLogManager wal,
        long metaPageId,
        boolean initNew,
        PageLockTrackerManager pageLockTrackerManager,
        GridKernalContext ctx,
        @Nullable AtomicLong pageListCacheLimit,
        byte pageFlag
    ) throws IgniteCheckedException {
        super(
            cacheGrpId,
            name,
            dataRegion,
            null,
            wal,
            metaPageId,
            initNew,
            pageLockTrackerManager,
            ctx,
            pageListCacheLimit,
            pageFlag
        );
    }

    /** {@inheritDoc} */
    @Override public void insertDataRow(CacheDataRow row, IoStatisticsHolder statHolder) throws IgniteCheckedException {
        super.insertDataRow(row, statHolder);

        assert row.key().partition() == PageIdUtils.partId(row.link()) :
            "Constructed a link with invalid partition ID [partId=" + row.key().partition() +
                ", link=" + U.hexLong(row.link()) + ']';
    }

    /** {@inheritDoc} */
    @Override public boolean updateDataRowTtl(long link, CacheDataRow row, IoStatisticsHolder statHolder) throws IgniteCheckedException {
        assert link != 0;

        try {
            long pageId = PageIdUtils.pageId(link);
            int itemId = PageIdUtils.itemId(link);

            Boolean updated = write(pageId, updateDataRowTtl, row, itemId, null, statHolder);

            assert updated != null; // Can't fail here.

            return Boolean.TRUE.equals(updated);
        }
        catch (AssertionError e) {
            throw corruptedFreeListException(e);
        }
        catch (IgniteCheckedException | Error e) {
            throw e;
        }
        catch (Throwable t) {
            throw new CorruptedFreeListException("Failed to update data row", t, grpId);
        }
    }

    /**
     * Page handler that is used to update the expiration time of CacheDataRow.
     * This page handler is used only for CacheDataTree and PageIO is an instance of DataPageIO.
     **/
    private final class UpdateRowMetaHandler extends PageHandler<CacheDataRow, Boolean> {
        /** {@inheritDoc} */
        @Override public Boolean run(
            int cacheId,
            long pageId,
            long page,
            long pageAddr,
            PageIO iox,
            Boolean walPlc,
            CacheDataRow row,
            int itemId,
            IoStatisticsHolder statHolder
        ) throws IgniteCheckedException {
            DataPageIO io = (DataPageIO) iox;

            Boolean updated = updateTtl(cacheId, pageId, page, pageAddr, io, row, itemId, walPlc, statHolder);

            evictionTracker().touchPage(pageId);

            return updated;
        }

        /**
         * Updats the expiration time of CacheDataRow.
         *
         * @param cacheId Cache ID.
         * @param pageId Page ID.
         * @param page Page pointer.
         * @param pageAddr Page address.
         * @param io IO.
         * @param row Row.
         * @param itemId Item id.
         * @return {@code true} If the expiration time was updated.
         * @throws IgniteCheckedException If failed.
         */
        private Boolean updateTtl(
            int cacheId,
            long pageId,
            long page,
            long pageAddr,
            DataPageIO io,
            CacheDataRow row,
            int itemId,
            Boolean walPlc,
            IoStatisticsHolder statHolder
        ) throws IgniteCheckedException {
            // Read payload to determine that the row is fragmented.
            DataPagePayload data = io.readPayload(pageAddr, itemId, pageMem.realPageSize(grpId));
            if (data.nextLink() == 0) {
                // The data is not fragmented, so it is one page update.
                io.updateExpirationTime(pageAddr, data.offset(), row);

                if (needWalDeltaRecord(pageId, page, walPlc)) {
                    // TODO This record must contain only a reference to a logical WAL record with the actual data.
                    int rowSize = data.payloadSize();

                    byte[] payload = new byte[rowSize];

                    // Reload the payload to get the actual data.
                    data = io.readPayload(pageAddr, itemId, pageSize());

                    assert data.payloadSize() == rowSize;

                    PageUtils.getBytes(pageAddr, data.offset(), payload, 0, rowSize);

                    wal.log(new DataPageUpdateRecord(
                        cacheId,
                        pageId,
                        itemId,
                        payload));
                }

                return Boolean.TRUE;
            }

            // The data is fragmented, so need to update the required fragment.
            long nextLink = 0L;
            boolean pageUpdated = false;
            int scannedBytes = 0;
            int updatedBytes = 0;

            // We are under write lock here, so it is safe to read/write the first page.
            boolean firstPage = true;

            do {
                final int cutItemId = firstPage ? itemId : PageIdUtils.itemId(nextLink);
                final long curPageId = firstPage ? pageId : PageIdUtils.pageId(nextLink);

                final long curPage = firstPage ? page : pageMem.acquirePage(grpId, curPageId, statHolder);

                try {
                    // It is safe to acquire a write lock on the next pages because we are under the write lock on the first page (head of
                    // the entry). This means that reading of entry is not possible, since it requires holding a read lock on the first page
                    // until all pages related to the entry are read. Moreover, following the current implementation of inserting a new
                    // entry, the tail of the entry is always placed on empty pages so these pages cannot be used for other entries.
                    long curPageAddr = firstPage ? pageAddr : pageMem.writeLock(grpId, curPageId, curPage);

                    try {
                        assert curPageAddr != 0L : "Failed to acquire write lock on the page [grpId=" + grpId + ", pageId=" + curPageId +
                            ", page=" + curPage + ", firstPage=" + firstPage + ", nextLink=" + nextLink + ']';

                        ByteBuffer buf = pageMem.pageBuffer(curPageAddr);

                        DataPageIO curIo = DataPageIO.VERSIONS.forPage(curPageAddr);

                        data = curIo.readPayload(curPageAddr, cutItemId, pageMem.realPageSize(grpId));

                        buf.position(data.offset());
                        buf.limit(data.offset() + data.payloadSize());

                        // This variable contains the number of bytes that were updated on the current page.
                        // Take into account that in the case when all bytes were updated, the method below returns Integer.MAX_VALUE.
                        int updatedOnCurrPage = curIo.updateExpirationTimeFragmentData(
                            row,
                            buf,
                            data.payloadSize(),
                            updatedBytes,
                            scannedBytes
                        );

                        pageUpdated = updatedOnCurrPage != 0;
                        scannedBytes += data.payloadSize();
                        updatedBytes += updatedOnCurrPage;

                        if (pageUpdated && needWalDeltaRecord(pageId, page, walPlc)) {
                            // TODO Log a new version of DataPageUpdateRecord.
                            // For now, DataPageUpdateRecord cannot update fragmented data.
                        }

                        if (updatedOnCurrPage == Integer.MAX_VALUE) {
                            // The update is completed.
                            return Boolean.TRUE;
                        }

                        nextLink = data.nextLink();
                    }
                    finally {
                        if (!firstPage)
                            pageMem.writeUnlock(grpId, curPageId, curPage, walPlc, pageUpdated);
                    }
                }
                finally {
                    if (!firstPage)
                        pageMem.releasePage(grpId, curPageId, curPage);
                }

                firstPage = false;
            } while (nextLink != 0L);

            return Boolean.FALSE;
        }
    }
}
