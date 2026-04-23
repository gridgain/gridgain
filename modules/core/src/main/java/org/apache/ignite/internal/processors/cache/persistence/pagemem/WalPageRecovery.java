/*
 * Copyright 2026 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.cache.persistence.pagemem;

import java.nio.ByteBuffer;
import java.util.function.IntFunction;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.CheckpointRecord;
import org.apache.ignite.internal.pagemem.wal.record.PageSnapshot;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.PageDeltaRecord;
import org.apache.ignite.internal.processors.cache.persistence.StorageException;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.IterationReason;
import org.apache.ignite.internal.processors.compress.CompressionProcessor;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

/**
 * Reusable utility that reconstructs a single data page by replaying the WAL.
 * <p>
 * Handles {@link WALRecord.RecordType#PAGE_RECORD} (full page snapshots, with optional compression),
 * {@link WALRecord.RecordType#CHECKPOINT_RECORD} (promotes an in-progress reconstruction to the last
 * known-valid page), {@link WALRecord.RecordType#MEMORY_RECOVERY} (discards an in-progress
 * reconstruction whose preceding checkpoint was broken) and any {@link PageDeltaRecord} targeting the
 * requested page (applied incrementally on top of the current reconstruction).
 */
public class WalPageRecovery {
    /** WAL manager used to replay records. */
    private final IgniteWriteAheadLogManager walMgr;

    /** Compression processor used to decompress compressed page snapshots; {@code null} disables decompression. */
    @Nullable private final CompressionProcessor compressor;

    /** Logger. */
    private final IgniteLogger log;

    /** System page size (including encryption padding when applicable). */
    private final int pageSize;

    /**
     * Resolver that returns the {@link PageMemoryEx} owning a given cache group ID.
     * May return {@code null} if the group is not present.
     * <p>
     * A {@code null} resolution is treated differently per call site:
     * <ul>
     *     <li>{@link #realPageSize(int)} (compressed-snapshot decompression sizing) falls back to {@link #pageSize}.</li>
     *     <li>Delta application aborts recovery with a {@link StorageException}.</li>
     * </ul>
     */
    private final IntFunction<PageMemoryEx> pageMemoryByGroup;

    /**
     * @param walMgr WAL manager used to replay records.
     * @param compressor Compression processor (may be {@code null} when compressed snapshots are not expected).
     * @param log Logger.
     * @param pageSize System page size.
     * @param pageMemoryByGroup Resolver that returns the {@link PageMemoryEx} for a given cache group ID,
     *     or {@code null} if the group is not currently known. Used to derive the real page size for
     *     compressed snapshots and to provide the target {@link org.apache.ignite.internal.pagemem.PageMemory}
     *     when applying {@link PageDeltaRecord}s.
     */
    public WalPageRecovery(
        IgniteWriteAheadLogManager walMgr,
        @Nullable CompressionProcessor compressor,
        IgniteLogger log,
        int pageSize,
        IntFunction<PageMemoryEx> pageMemoryByGroup
    ) {
        this.walMgr = walMgr;
        this.compressor = compressor;
        this.log = log;
        this.pageSize = pageSize;
        this.pageMemoryByGroup = pageMemoryByGroup;
    }

    /**
     * Resolves the real page size for {@code grpId} via {@link #pageMemoryByGroup}. If the resolver
     * returns {@code null} (group not present), falls back to {@link #pageSize}.
     *
     * @param grpId Cache group id.
     * @return Real page size (without page header overhead), or {@link #pageSize} if the group is unknown.
     */
    private int realPageSize(int grpId) {
        PageMemoryEx mem = pageMemoryByGroup.apply(grpId);

        return mem == null ? pageSize : mem.realPageSize(grpId);
    }

    /**
     * Reconstructs the page identified by {@code fullId} by replaying the WAL and writes the
     * reconstructed contents into {@code destBuf}.
     * <p>
     * If a {@link PageDeltaRecord} targets the page but its owning {@link PageMemoryEx} cannot be
     * resolved via the constructor-supplied resolver, the delta is skipped and a WARN is logged.
     * This matches the existing defensive posture of the recovery path and avoids hard-failing the
     * reconstruction when a delta would otherwise NPE on a {@code null} page-memory argument.
     *
     * @param fullId Full page id to reconstruct.
     * @param destBuf Destination buffer; the caller is responsible for its synchronization.
     * @throws IgniteCheckedException If WAL iteration fails.
     * @throws StorageException If the page could not be reconstructed (no matching record in the WAL).
     */
    public void recoverPage(FullPageId fullId, ByteBuffer destBuf) throws IgniteCheckedException {
        Long tmpAddr = null;
        try {
            ByteBuffer curPage = null;
            ByteBuffer lastValidPage = null;

            try (WALIterator it = walMgr.replay(null, IterationReason.RESTORE_PAGE)) {
                for (IgniteBiTuple<WALPointer, WALRecord> tuple : it) {
                    WALRecord rec = tuple.getValue();

                    switch (rec.type()) {
                        case PAGE_RECORD: {
                            PageSnapshot snapshot = (PageSnapshot)rec;

                            if (snapshot.fullPageId().equals(fullId)) {
                                if (tmpAddr == null) {
                                    assert snapshot.pageDataSize() <= pageSize : snapshot.pageDataSize();

                                    tmpAddr = GridUnsafe.allocateMemory(pageSize);
                                }

                                if (curPage == null)
                                    curPage = GridUnsafe.wrapPointer(tmpAddr, pageSize);

                                PageUtils.putBytes(tmpAddr, 0, snapshot.pageData());

                                if (compressor != null
                                    && PageIO.getCompressionType(tmpAddr) != CompressionProcessor.UNCOMPRESSED_PAGE) {
                                    int realPageSize = realPageSize(snapshot.groupId());

                                    assert snapshot.pageDataSize() < realPageSize : snapshot.pageDataSize();

                                    compressor.decompressPage(curPage, realPageSize);
                                }
                            }

                            break;
                        }

                        case CHECKPOINT_RECORD: {
                            CheckpointRecord cpRec = (CheckpointRecord)rec;

                            assert !cpRec.end();

                            if (curPage != null) {
                                lastValidPage = curPage;
                                curPage = null;
                            }

                            break;
                        }

                        case MEMORY_RECOVERY:
                            curPage = null;

                            break;

                        default:
                            if (rec instanceof PageDeltaRecord) {
                                PageDeltaRecord deltaRec = (PageDeltaRecord)rec;

                                if (curPage != null
                                    && deltaRec.pageId() == fullId.pageId()
                                    && deltaRec.groupId() == fullId.groupId()) {
                                    assert tmpAddr != null;

                                    PageMemoryEx pageMem = pageMemoryByGroup.apply(fullId.groupId());

                                    if (pageMem == null)
                                        throw new StorageException("Cannot apply WAL page-delta during recovery: "
                                            + "no PageMemory for group "
                                            + "[fullPageId=" + fullId
                                            + ", grpId=" + fullId.groupId()
                                            + ", deltaType=" + deltaRec.type() + "]");

                                    deltaRec.applyDelta(pageMem, tmpAddr);
                                }
                            }
                    }
                }
            }

            ByteBuffer restored = curPage == null ? lastValidPage : curPage;

            if (restored == null)
                throw new StorageException(String.format(
                    "Page is broken. Can't restore it from WAL. (grpId = %d, pageId = %X).",
                    fullId.groupId(), fullId.pageId()
                ));

            destBuf.put(restored);
        }
        finally {
            if (tmpAddr != null)
                GridUnsafe.freeMemory(tmpAddr);
        }
    }

    /**
     * Scans the WAL archive and counts records relevant to {@code fullId}.
     * <p>
     * <b>Cost:</b> this opens a fresh {@link WALIterator} and iterates the entire WAL archive once.
     * For a node with a multi-GB archive this can take minutes. Call only from the ERROR-logging
     * path where the cost is acceptable.
     *
     * @param fullId Full page id to summarize.
     * @return Summary of WAL records relevant to the page.
     * @throws IgniteCheckedException If WAL iteration fails.
     */
    public WalRecordSummary summarize(FullPageId fullId) throws IgniteCheckedException {
        WalRecordSummary sum = new WalRecordSummary();

        try (WALIterator it = walMgr.replay(null, IterationReason.RESTORE_PAGE)) {
            for (IgniteBiTuple<WALPointer, WALRecord> tuple : it) {
                WALPointer ptr = tuple.getKey();
                WALRecord rec = tuple.getValue();

                switch (rec.type()) {
                    case PAGE_RECORD:
                        if (((PageSnapshot)rec).fullPageId().equals(fullId)) {
                            sum.setPageRecordCount(sum.getPageRecordCount() + 1);
                            sum.updatePointer(ptr);
                        }
                        break;
                    case CHECKPOINT_RECORD:
                        if (!((CheckpointRecord)rec).end())
                            sum.setCheckpointBoundariesSeen(sum.getCheckpointBoundariesSeen() + 1);
                        break;
                    case MEMORY_RECOVERY:
                        sum.setMemoryRecoverySeen(true);
                        break;
                    default:
                        if (rec instanceof PageDeltaRecord) {
                            PageDeltaRecord d = (PageDeltaRecord)rec;
                            if (d.pageId() == fullId.pageId() && d.groupId() == fullId.groupId()) {
                                sum.setPageDeltaRecordCount(sum.getPageDeltaRecordCount() + 1);
                                sum.updatePointer(ptr);
                            }
                        }
                }
            }
        }

        return sum;
    }

    /**
     * Summary of WAL records relevant to a specific {@link FullPageId}, produced by
     * {@link #summarize(FullPageId)}. Intended for forensic logging.
     */
    public static final class WalRecordSummary {
        private int pageRecordCount;

        private int pageDeltaRecordCount;

        private int checkpointBoundariesSeen;

        private boolean memoryRecoverySeen;

        private WALPointer earliestPointer;

        private WALPointer latestPointer;

        /**
         * Updates {@link #earliestPointer} / {@link #latestPointer} to reflect a newly seen matching record.
         *
         * @param p WAL pointer of the matching record.
         */
        void updatePointer(WALPointer p) {
            if (getEarliestPointer() == null)
                setEarliestPointer(p);

            setLatestPointer(p);
        }

        /** Number of {@link WALRecord.RecordType#PAGE_RECORD} snapshots matching the target page. */
        public int getPageRecordCount() {
            return pageRecordCount;
        }

        public void setPageRecordCount(int pageRecordCount) {
            this.pageRecordCount = pageRecordCount;
        }

        /** Number of {@link PageDeltaRecord}s targeting the target page. */
        public int getPageDeltaRecordCount() {
            return pageDeltaRecordCount;
        }

        public void setPageDeltaRecordCount(int pageDeltaRecordCount) {
            this.pageDeltaRecordCount = pageDeltaRecordCount;
        }

        /** Number of non-end {@link WALRecord.RecordType#CHECKPOINT_RECORD} boundaries observed during the scan. */
        public int getCheckpointBoundariesSeen() {
            return checkpointBoundariesSeen;
        }

        public void setCheckpointBoundariesSeen(int checkpointBoundariesSeen) {
            this.checkpointBoundariesSeen = checkpointBoundariesSeen;
        }

        /** {@code true} if any {@link WALRecord.RecordType#MEMORY_RECOVERY} record was observed during the scan. */
        public boolean isMemoryRecoverySeen() {
            return memoryRecoverySeen;
        }

        public void setMemoryRecoverySeen(boolean memoryRecoverySeen) {
            this.memoryRecoverySeen = memoryRecoverySeen;
        }

        /** Earliest WAL pointer among records that matched the target page. */
        public WALPointer getEarliestPointer() {
            return earliestPointer;
        }

        public void setEarliestPointer(WALPointer earliestPointer) {
            this.earliestPointer = earliestPointer;
        }

        /** Latest WAL pointer among records that matched the target page. */
        public WALPointer getLatestPointer() {
            return latestPointer;
        }

        public void setLatestPointer(WALPointer latestPointer) {
            this.latestPointer = latestPointer;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "WalRecordSummary[pageRecords=" + getPageRecordCount()
                + ", pageDeltas=" + getPageDeltaRecordCount()
                + ", checkpoints=" + getCheckpointBoundariesSeen()
                + ", memoryRecoverySeen=" + isMemoryRecoverySeen()
                + ", earliest=" + getEarliestPointer()
                + ", latest=" + getLatestPointer() + "]";
        }
    }

}
