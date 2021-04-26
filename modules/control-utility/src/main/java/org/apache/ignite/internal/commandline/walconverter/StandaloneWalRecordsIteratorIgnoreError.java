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

package org.apache.ignite.internal.commandline.walconverter;

import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileDescriptor;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.StandaloneWalRecordsIterator;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * WAL reader iterator with ignore Exception
 */
public class StandaloneWalRecordsIteratorIgnoreError extends StandaloneWalRecordsIterator {

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
    @Override protected IgniteBiTuple<WALPointer, WALRecord> advanceRecord(
        @Nullable AbstractReadFileHandle hnd) throws IgniteCheckedException {
        FileWALPointer actualFilePtr = new FileWALPointer(hnd.idx(), (int)hnd.in().position(), 0);

        try {
            WALRecord rec = hnd.ser().readRecord(hnd.in(), actualFilePtr);

            actualFilePtr.length(rec.size());

            // cast using diamond operator here can break compile for 7
            return new IgniteBiTuple<>((WALPointer)actualFilePtr, postProcessRecord(rec));
        }catch (Exception e){
            // ignore
            e.printStackTrace();
        }
        return null;
    }
}
