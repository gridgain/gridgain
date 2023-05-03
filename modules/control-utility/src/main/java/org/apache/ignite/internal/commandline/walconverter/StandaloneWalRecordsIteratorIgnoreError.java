/*
 * Copyright 2021 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.commandline.walconverter;

import java.io.EOFException;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileDescriptor;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.SegmentEofException;
import org.apache.ignite.internal.processors.cache.persistence.wal.io.FileInput;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.StandaloneWalRecordsIterator;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * WAL reader which won't stop on exceptions.
 * It differs from the standard iterator (StandaloneWalRecordsIterator) in that it does not interrupt reading
 * on an error but tries to read further.
 */
public class StandaloneWalRecordsIteratorIgnoreError extends StandaloneWalRecordsIterator {
    private volatile @Nullable WALPointer lastReadPointer;

    /** */
    public StandaloneWalRecordsIteratorIgnoreError(@NotNull IgniteLogger log,
        @NotNull GridCacheSharedContext sharedCtx,
        @NotNull FileIOFactory ioFactory,
        @NotNull List<FileDescriptor> walFiles,
        IgniteBiPredicate<WALRecord.RecordType, WALPointer> readTypeFilter,
        FileWALPointer lowBound, FileWALPointer highBound, boolean keepBinary, int initialReadBufferSize,
        boolean strictBoundsCheck) throws IgniteCheckedException {
        super(log, sharedCtx, ioFactory, walFiles, readTypeFilter, lowBound, highBound, keepBinary, initialReadBufferSize, strictBoundsCheck);
    }

    /** {@inheritDoc} */
    @Override protected IgniteCheckedException handleRecordException(
        @NotNull Exception e,
        @Nullable FileWALPointer ptr
    ) {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected IgniteBiTuple<WALPointer, WALRecord> advanceRecord(@Nullable AbstractReadFileHandle hnd) {
        if (hnd == null)
            return null;

        IgniteBiTuple<WALPointer, WALRecord> result = null;

        while (result == null) {
            FileWALPointer actualFilePtr = new FileWALPointer(hnd.idx(), (int)hnd.in().position(), 0);

            lastReadPointer = actualFilePtr;

            try {
                WALRecord rec = hnd.ser().readRecord(hnd.in(), actualFilePtr);

                actualFilePtr.length(rec.size());

                result = new IgniteBiTuple<>(actualFilePtr, postProcessRecord(rec));
            }
            catch (SegmentEofException | EOFException eof) {
                log.error("Critical exception has happened during WAL was scanned: " + lastReadPointer, eof);

                break;
            }
            catch (Exception ignore) {
                log.error("Critical exception has happened during WAL was scanned", ignore);

                try {
                    FileInput in = hnd.in();

                    in.seek(actualFilePtr.fileOffset());

                    int recordType = in.readUnsignedByte() - 1;

                    long idx = in.readLong();

                    int offset = in.readInt();

                    int len = in.readInt();

                    in.seek(offset + len);

                    log.error("Error read record [recordType=" + recordType + ", idx=" + idx + ", offset=" + offset + ", len=" + len + "]", ignore);
                }
                catch (Exception e) {
                    log.error("Couldn't miss read error", e);

                    break;
                }
            }
        }
        return result;
    }

    @Override public @Nullable WALPointer lastReadPointer() {
        return lastReadPointer;
    }
}
