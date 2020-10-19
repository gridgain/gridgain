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
package org.apache.ignite.internal.processors.cache.persistence.wal;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.file.AsyncFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.IgniteDataIntegrityViolationException;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.StandaloneGridKernalContext;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.StandaloneWalRecordsIterator;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordV1Serializer.CRC_SIZE;
import static org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordV1Serializer.HEADER_RECORD_SIZE;
import static org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordV1Serializer.REC_TYPE_SIZE;
import static org.apache.ignite.internal.util.typedef.X.cause;

/**
 * Tests correctness of exception message when WAL is corrupted.
 */
public class WalCorruptionTest extends GridCommonAbstractTest {
    /** Size of memory recovery record. */
    private static final int MEMORY_RECOVERY_RECORD_PLAIN_SIZE = 8;

    /** Size of init new page record. */
    private static final int INIT_NEW_PAGE_RECORD_PLAIN_SIZE = 24;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true))
            );
    }

    /** */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    /** */
    @Test
    public void testWalCorruptionException() throws Exception {
        IgniteEx ignite = startGrid(0);

        ignite.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 10; i++)
            cache.put(i, i);

        File walPath = new File(
            U.resolveWorkDirectory(
                ignite.context().config().getWorkDirectory(),
                DataStorageConfiguration.DFLT_WAL_PATH,
                false
            ),
            ignite.context().pdsFolderResolver().resolveFolders().folderName()
        );

        File wal = new File(walPath, "0000000000000000.wal");

        stopGrid(0);

        int walPtrSerializedSize = FileWALPointer.POINTER_SIZE;

        int secondRecordPos = HEADER_RECORD_SIZE + REC_TYPE_SIZE + walPtrSerializedSize + MEMORY_RECOVERY_RECORD_PLAIN_SIZE + CRC_SIZE;

        // Corrupt second record in file.
        try (RandomAccessFile raf = new RandomAccessFile(wal, "rw")) {
            raf.seek(secondRecordPos + REC_TYPE_SIZE + walPtrSerializedSize + 1);

            raf.write("a".getBytes());
        }

        try (WALIterator stIt = walIterator(walPath, ignite.context().config().getDataStorageConfiguration().getPageSize())) {
            stIt.next();

            try {
                stIt.next();
            }
            catch (Exception e) {
                IgniteDataIntegrityViolationException ex = cause(e, IgniteDataIntegrityViolationException.class);

                if (ex != null) {
                    assertTrue(ex.getMessage().matches("val: [-0-9]{1,11}, writtenCrc: [-0-9]{1,11}, " +
                        "crcStartPos: " + secondRecordPos +
                        ", crcEndPos: " + (secondRecordPos + REC_TYPE_SIZE + walPtrSerializedSize + INIT_NEW_PAGE_RECORD_PLAIN_SIZE)));

                    return;
                }
                else
                    fail("Test failed with wrong exception: " + e);
            }

            fail("Test passed without expected exception.");
        }
    }

    /** */
    private WALIterator walIterator(File walPath, int pageSize) throws IgniteCheckedException {
        IgniteWalIteratorFactory.IteratorParametersBuilder builder = new IgniteWalIteratorFactory.IteratorParametersBuilder()
            .filesOrDirs(walPath);

        return new StandaloneWalRecordsIteratorTest(
            log,
            prepareSharedCtx(pageSize),
            new AsyncFileIOFactory(),
            new IgniteWalIteratorFactory(log).resolveWalFiles(builder),
            null,
            new FileWALPointer(Long.MIN_VALUE, 0, 0),
            new FileWALPointer(Long.MAX_VALUE, Integer.MAX_VALUE, 0),
            false,
            StandaloneWalRecordsIterator.DFLT_BUF_SIZE,
            false
        );
    }

    /**
     * @return Fake shared context required for create minimal services for record reading.
     */
    @NotNull private GridCacheSharedContext prepareSharedCtx(int pageSize) throws IgniteCheckedException {
        GridKernalContext kernalCtx = new StandaloneGridKernalContext(log, null, null);

        IgniteCacheDatabaseSharedManager dbMgr = new IgniteCacheDatabaseSharedManager() {{
            setPageSize(pageSize);
        }};

        return new GridCacheSharedContext<>(
            kernalCtx, null, null, null,
            null, null, null, dbMgr, null,
            null, null, null, null, null,
            null, null, null, null, null, null
        );
    }

    /**
     * Test WAL iterator.
     */
    private static class StandaloneWalRecordsIteratorTest extends StandaloneWalRecordsIterator {
        /** */
        StandaloneWalRecordsIteratorTest(
            @NotNull IgniteLogger log,
            @NotNull GridCacheSharedContext sharedCtx,
            @NotNull FileIOFactory ioFactory,
            @NotNull List<FileDescriptor> walFiles,
            IgniteBiPredicate<WALRecord.RecordType, WALPointer> readTypeFilter,
            FileWALPointer lowBound,
            FileWALPointer highBound,
            boolean keepBinary,
            int initialReadBufferSize,
            boolean strictBoundsCheck
        ) throws IgniteCheckedException {
            super(log, sharedCtx, ioFactory, walFiles, readTypeFilter, lowBound, highBound, keepBinary,
                initialReadBufferSize, strictBoundsCheck);
        }

        /** {@inheritDoc} */
        @Override protected IgniteCheckedException handleRecordException(
            @NotNull Exception e,
            @Nullable FileWALPointer ptr
        ) {
            return e instanceof IgniteCheckedException ? (IgniteCheckedException)e : null;
        }
    }
}
