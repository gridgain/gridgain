/*
 * Copyright 2022 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.cache.persistence.wal.reader;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.RolloverType;
import org.apache.ignite.internal.pagemem.wal.record.SnapshotRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.pagemem.wal.record.WalRecordCacheGroupAware;
import org.apache.ignite.internal.pagemem.wal.record.delta.PartitionDestroyRecord;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileDescriptor;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory.IteratorParametersBuilder;

/**
 * The test check, that StandaloneWalRecordsIterator correctly close file descriptors associated with WAL files.
 */
public class StandaloneWalRecordsIteratorTest extends GridCommonAbstractTest {
    /** Wal segment size. */
    private static final int WAL_SEGMENT_SIZE = 512 * 1024;

    /** Wal compaction enabled. */
    private boolean walCompactionEnabled;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                )
                .setWalSegmentSize(WAL_SEGMENT_SIZE)
                .setWalCompactionEnabled(walCompactionEnabled)
        );

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        walCompactionEnabled = false;

        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     *
     */
    public void testBlinkingTemporaryFile() throws Exception {
        walCompactionEnabled = true;

        IgniteEx ig = (IgniteEx)startGrid();

        String archiveWalDir = getArchiveWalDirPath(ig);

        ig.cluster().active(true);

        IgniteCache<Object, Object> cache = ig.getOrCreateCache(
            new CacheConfiguration<>().setName("c-n").setAffinity(new RendezvousAffinityFunction(false, 32)));

        IgniteCacheDatabaseSharedManager sharedMgr = ig.context().cache().context().database();

        IgniteWriteAheadLogManager walMgr = ig.context().cache().context().wal();

        WALPointer fromPtr = null;

        int recordsCnt = WAL_SEGMENT_SIZE / 8 /* record size */ * 5;

        AtomicBoolean stopBlinking = new AtomicBoolean(false);
        AtomicInteger blinkIterations = new AtomicInteger(0);

        IgniteInternalFuture blinkFut = GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
                try {
                    while (walMgr.lastCompactedSegment() < 2)
                        U.sleep(10);

                    File walArchive = new File(archiveWalDir);
                    File consIdFolder = new File(walArchive, "node00-" + ig.cluster().localNode().consistentId().toString());
                    File compressedWalSegment = new File(consIdFolder, FileDescriptor.fileName(1) + ".zip");
                    File compressedTmpWalSegment = new File(consIdFolder, FileDescriptor.fileName(1) + ".zip.tmp");

                    while (!stopBlinking.get()) {
                        Files.copy(compressedWalSegment.toPath(), compressedTmpWalSegment.toPath());

                        U.sleep(10);

                        U.delete(compressedTmpWalSegment);

                        blinkIterations.incrementAndGet();
                    }
                }
                catch (IgniteInterruptedCheckedException | IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }, "blinky");

        for (int i = 0; i < recordsCnt; i++) {
            WALPointer ptr = walMgr.log(new PartitionDestroyRecord(i, i));

            if (i == 100)
                fromPtr = ptr;
        }

        assertNotNull(fromPtr);

        cache.put(1, 1);

        forceCheckpoint();

        // Generate WAL segments for filling WAL archive folder.
        for (int i = 0; i < 2 * ig.configuration().getDataStorageConfiguration().getWalSegments(); i++) {
            sharedMgr.checkpointReadLock();

            try {
                walMgr.log(new SnapshotRecord(i, false), RolloverType.NEXT_SEGMENT);
            }
            finally {
                sharedMgr.checkpointReadUnlock();
            }
        }

        cache.put(2, 2);

        forceCheckpoint();

        System.out.println("@@@ " + blinkIterations.get() + " blink iterations already completed");

        U.sleep(5000);

        stopGrid();

        for (int i = 0; i < 20; i++) {
            WALIterator it = new IgniteWalIteratorFactory(log)
                .iterator(new IteratorParametersBuilder().from((FileWALPointer)fromPtr).filesOrDirs(archiveWalDir));

            TreeSet<Integer> foundCounters = new TreeSet<>();

            it.forEach(x -> {
                WALRecord rec = x.get2();

                if (rec instanceof PartitionDestroyRecord)
                    foundCounters.add(((WalRecordCacheGroupAware)rec).groupId());
            });

            assertEquals(new Integer(100), foundCounters.first());
            assertEquals(new Integer(recordsCnt - 1), foundCounters.last());
            assertEquals(recordsCnt - 100, foundCounters.size());

            System.out.println("@@@ " + blinkIterations.get() + " blink iterations already completed");
        }

        stopBlinking.set(true);

        System.out.println("@@@ " + blinkIterations.get() + " blink iterations finally completed");

        blinkFut.get();
    }

    /**
     *
     */
    public void testBoundedIterationOverSeveralSegments() throws Exception {
        walCompactionEnabled = true;

        IgniteEx ig = (IgniteEx)startGrid();

        String archiveWalDir = getArchiveWalDirPath(ig);

        ig.cluster().active(true);

        IgniteCache<Object, Object> cache = ig.getOrCreateCache(
            new CacheConfiguration<>().setName("c-n").setAffinity(new RendezvousAffinityFunction(false, 32)));

        IgniteCacheDatabaseSharedManager sharedMgr = ig.context().cache().context().database();

        IgniteWriteAheadLogManager walMgr = ig.context().cache().context().wal();

        WALPointer fromPtr = null;

        int recordsCnt = WAL_SEGMENT_SIZE / 8 /* record size */ * 5;

        for (int i = 0; i < recordsCnt; i++) {
            WALPointer ptr = walMgr.log(new PartitionDestroyRecord(i, i));

            if (i == 100)
                fromPtr = ptr;
        }

        assertNotNull(fromPtr);

        cache.put(1, 1);

        forceCheckpoint();

        // Generate WAL segments for filling WAL archive folder.
        for (int i = 0; i < 2 * ig.configuration().getDataStorageConfiguration().getWalSegments(); i++) {
            sharedMgr.checkpointReadLock();

            try {
                walMgr.log(new SnapshotRecord(i, false), RolloverType.NEXT_SEGMENT);
            }
            finally {
                sharedMgr.checkpointReadUnlock();
            }
        }

        cache.put(2, 2);

        forceCheckpoint();

        U.sleep(5000);

        stopGrid();

        WALIterator it = new IgniteWalIteratorFactory(log)
            .iterator(new IteratorParametersBuilder().from((FileWALPointer)fromPtr).filesOrDirs(archiveWalDir));

        TreeSet<Integer> foundCounters = new TreeSet<>();

        it.forEach(x -> {
            WALRecord rec = x.get2();

            if (rec instanceof PartitionDestroyRecord)
                foundCounters.add(((WalRecordCacheGroupAware)rec).groupId());
        });

        assertEquals(new Integer(100), foundCounters.first());
        assertEquals(new Integer(recordsCnt - 1), foundCounters.last());
        assertEquals(recordsCnt - 100, foundCounters.size());
    }

    /**
     * Check correct closing file descriptors.
     *
     */
    private String createWalFiles() throws Exception {
        return createWalFiles(1);
    }

    /** */
    private String createWalFiles(int segRecCnt) throws Exception {
        IgniteEx ig = (IgniteEx)startGrid();

        String archiveWalDir = getArchiveWalDirPath(ig);

        ig.cluster().active(true);

        IgniteCacheDatabaseSharedManager sharedMgr = ig.context().cache().context().database();

        IgniteWriteAheadLogManager walMgr = ig.context().cache().context().wal();

        // Generate WAL segments for filling WAL archive folder.
        for (int i = 0; i < 2 * ig.configuration().getDataStorageConfiguration().getWalSegments(); i++) {
            sharedMgr.checkpointReadLock();

            try {
                for (int j = 0; j < segRecCnt - 1; j++)
                    walMgr.log(new SnapshotRecord(i * segRecCnt + j, false));

                walMgr.log(new SnapshotRecord(i * segRecCnt + segRecCnt - 1, false), RolloverType.NEXT_SEGMENT);
            }
            finally {
                sharedMgr.checkpointReadUnlock();
            }
        }

        stopGrid();

        return archiveWalDir;
    }

    /**
     * Check correct closing file descriptors.
     *
     * @throws Exception if test failed.
     */
    @Test
    public void testCorrectClosingFileDescriptors() throws Exception {

        // Iterate by all archived WAL segments.
        createWalIterator(createWalFiles()).forEach(x -> {
        });

        assertTrue("At least one WAL file must be opened!", CountedFileIO.getCountOpenedWalFiles() > 0);

        assertTrue("All WAL files must be closed at least ones!", CountedFileIO.getCountOpenedWalFiles() <= CountedFileIO.getCountClosedWalFiles());
    }

    /** */
    @Test
    public void testNoNextIfLowBoundInTheEnd() throws Exception {
        String dir = createWalFiles(3);

        WALIterator iter = createWalIterator(dir, null, null, false);

        assertFalse(iter.lastRead().isPresent());
        assertTrue(iter.hasNext());

        while (iter.hasNext()) {
            IgniteBiTuple<WALPointer, WALRecord> curr = iter.next();

            assertEquals("Last read should point to the current record", curr.get1(), iter.lastRead().get());
        }

        iter.close();

        iter = createWalIterator(dir, ((FileWALPointer)iter.lastRead().get().next()), null, false);

        assertFalse(iter.lastRead().isPresent());

        assertFalse(iter.hasNext());

        iter.close();
    }

    /** */
    @Test
    public void testNextRecordReturnedForLowBounds() throws Exception {
        String dir = createWalFiles(3);

        WALIterator iter = createWalIterator(dir, null, null, false);

        IgniteBiTuple<WALPointer, WALRecord> prev = iter.next();

        assertEquals("Last read should point to the current record", prev.get1(), iter.lastRead().get());

        iter.close();

        iter = createWalIterator(dir, ((FileWALPointer)iter.lastRead().get().next()), null, false);

        assertFalse(iter.lastRead().isPresent());
        assertTrue(iter.hasNext());

        while (iter.hasNext()) {
            IgniteBiTuple<WALPointer, WALRecord> cur = iter.next();

            assertEquals("Last read should point to the current record", cur.get1(), iter.lastRead().get());

            assertFalse(
                "Should read next record[prev=" + prev.get1() + ", cur=" + cur.get1() + ']',
                prev.get1().equals(cur.get1())
            );

            prev = cur;

            iter.close();

            iter = createWalIterator(dir, ((FileWALPointer)iter.lastRead().get().next()), null, false);

            assertFalse(iter.lastRead().isPresent());
        }

        iter.close();
    }

    /** */
    @Test
    public void testLastRecordFiltered() throws Exception {
        String dir = createWalFiles();

        WALIterator iter = createWalIterator(dir, null, null, false);

        IgniteBiTuple<WALPointer, WALRecord> lastRec = null;

        // Search for the last record.
        while (iter.hasNext())
            lastRec = iter.next();

        iter.close();

        assertNotNull(lastRec);

        FileWALPointer lastPointer = (FileWALPointer)iter.lastRead().get();

        WALRecord.RecordType lastRecType = lastRec.get2().type();

        // Iterating and filter out last record.
        iter = createWalIterator(dir, null, null, false, (type, ptr) -> type != lastRecType);

        assertTrue(iter.hasNext());

        while (iter.hasNext()) {
            lastRec = iter.next();

            assertNotNull(lastRec.get2().type()); // Type is null for filtered records.

            assertTrue(lastRec.get2().type() != lastRecType);
        }

        iter.close();

        assertNotNull(lastRec);

        assertEquals(
            "LastRead should point to the last WAL Record even it filtered",
            lastPointer,
            iter.lastRead().get()
        );

        // Record on `lastPointer` is filtered so.
        assertEquals(
            "Last returned record should be before lastPointer",
            -1, ((FileWALPointer)lastRec.get1()).compareTo(lastPointer)
        );
    }

    /**
     * Check correct check bounds.
     *
     * @throws Exception if test failed.
     */
    @Test
    public void testStrictBounds() throws Exception {
        String dir = createWalFiles();

        FileWALPointer lowBound = null, highBound = null;

        for (IgniteBiTuple<WALPointer, WALRecord> p : createWalIterator(dir, null, null, false)) {
            if (lowBound == null)
                lowBound = (FileWALPointer) p.get1();

            highBound = (FileWALPointer) p.get1();
        }

        assertNotNull(lowBound);

        assertNotNull(highBound);

        createWalIterator(dir, lowBound, highBound, true);

        final FileWALPointer lBound = lowBound;
        final FileWALPointer hBound = highBound;

        //noinspection ThrowableNotThrown
        GridTestUtils.assertThrows(log, () -> {
            createWalIterator(dir, new FileWALPointer(lBound.index() - 1, 0, 0), hBound, true);

            return 0;
        }, IgniteCheckedException.class, null);

        //noinspection ThrowableNotThrown
        GridTestUtils.assertThrows(log, () -> {
            createWalIterator(dir, lBound, new FileWALPointer(hBound.index() + 1, 0, 0), true);

            return 0;
        }, IgniteCheckedException.class, null);

        List<FileDescriptor> walFiles = listWalFiles(dir);

        assertNotNull(walFiles);

        assertTrue(!walFiles.isEmpty());

        assertTrue(walFiles.get(new Random().nextInt(walFiles.size())).file().delete());

        //noinspection ThrowableNotThrown
        GridTestUtils.assertThrows(log, () -> {
            createWalIterator(dir, lBound, hBound, true);

            return 0;
        }, IgniteCheckedException.class, null);
    }

    /**
     * Checks if binary-metadata-writer thread is not hung after standalone iterator is closed.
     *
     * @throws Exception if test failed.
     */
    @Test
    public void testBinaryMetadataWriterStopped() throws Exception {
        String dir = createWalFiles();

        final IgniteWalIteratorFactory factory = new IgniteWalIteratorFactory(new NullLogger());

        IgniteWalIteratorFactory.IteratorParametersBuilder iterParametersBuilder =
            new IgniteWalIteratorFactory.IteratorParametersBuilder().filesOrDirs(dir)
                .pageSize(4096);

        try (WALIterator stIt = factory.iterator(iterParametersBuilder)) {
        }

        boolean binaryMetadataWriterStopped = GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                Set<String> threadNames = Thread.getAllStackTraces().keySet().stream().map(Thread::getName).collect(Collectors.toSet());

                return threadNames.stream().noneMatch(t -> t.startsWith("binary-metadata-writer"));
            }
        }, 10_000L);

        assertTrue(binaryMetadataWriterStopped);
    }

    /**
     * Creates WALIterator associated with files inside walDir.
     *
     * @param walDir - path to WAL directory.
     * @return WALIterator associated with files inside walDir.
     * @throws IgniteCheckedException if error occur.
     */
    private WALIterator createWalIterator(String walDir) throws IgniteCheckedException {
        IteratorParametersBuilder params = new IteratorParametersBuilder();

        params.ioFactory(new CountedFileIOFactory());

        return new IgniteWalIteratorFactory(log).iterator(params.filesOrDirs(walDir));
    }

    /**
     * @param walDir Wal directory.
     */
    private List<FileDescriptor> listWalFiles(String walDir) throws IgniteCheckedException {
        IteratorParametersBuilder params = new IteratorParametersBuilder();

        params.ioFactory(new RandomAccessFileIOFactory());

        return new IgniteWalIteratorFactory(log).resolveWalFiles(params.filesOrDirs(walDir));
    }

    /** */
    private WALIterator createWalIterator(
        String walDir,
        FileWALPointer lowBound,
        FileWALPointer highBound,
        boolean strictCheck
    ) throws IgniteCheckedException {
        return createWalIterator(walDir, lowBound, highBound, strictCheck, null);
    }

    /**
     * @param walDir Wal directory.
     * @param lowBound Low bound.
     * @param highBound High bound.
     * @param strictCheck Strict check.
     */
    private WALIterator createWalIterator(
        String walDir,
        FileWALPointer lowBound,
        FileWALPointer highBound,
        boolean strictCheck,
        IgniteBiPredicate<WALRecord.RecordType, WALPointer> filter
    ) throws IgniteCheckedException {
        IteratorParametersBuilder params = new IteratorParametersBuilder();

        params.ioFactory(new RandomAccessFileIOFactory()).
            filesOrDirs(walDir).
            strictBoundsCheck(strictCheck);

        if (lowBound != null)
            params.from(lowBound);

        if (highBound != null)
            params.to(highBound);

        if (filter != null)
            params.filter(filter);

        return new IgniteWalIteratorFactory(log).iterator(params);
    }

    /**
     * Evaluate path to directory with WAL archive.
     *
     * @param ignite instance of Ignite.
     * @return path to directory with WAL archive.
     * @throws IgniteCheckedException if error occur.
     */
    private String getArchiveWalDirPath(Ignite ignite) throws IgniteCheckedException {
        return U.resolveWorkDirectory(
            U.defaultWorkDirectory(),
            ignite.configuration().getDataStorageConfiguration().getWalArchivePath(),
            false
        ).getAbsolutePath();
    }

    /**
     *
     */
    private static class CountedFileIOFactory extends RandomAccessFileIOFactory {
        /** {@inheritDoc} */
        @Override public FileIO create(File file, OpenOption... modes) throws IOException {
            assertEquals(Collections.singletonList(StandardOpenOption.READ), Arrays.asList(modes));

            return new CountedFileIO(file, modes);
        }
    }

    /**
     *
     */
    private static class CountedFileIO extends RandomAccessFileIO {
        /** Wal open counter. */
        private static final AtomicInteger WAL_OPEN_COUNTER = new AtomicInteger();

        /** Wal close counter. */
        private static final AtomicInteger WAL_CLOSE_COUNTER = new AtomicInteger();

        /** File name. */
        private final String fileName;

        /** */
        public CountedFileIO(File file, OpenOption... modes) throws IOException {
            super(file, modes);

            fileName = file.getName();

            if (FileWriteAheadLogManager.WAL_NAME_PATTERN.matcher(fileName).matches())
                WAL_OPEN_COUNTER.incrementAndGet();
        }

        /** {@inheritDoc} */
        @Override public void close() throws IOException {
            super.close();

            if (FileWriteAheadLogManager.WAL_NAME_PATTERN.matcher(fileName).matches())
                WAL_CLOSE_COUNTER.incrementAndGet();
        }

        /**
         *
         * @return number of opened files.
         */
        public static int getCountOpenedWalFiles() { return WAL_OPEN_COUNTER.get(); }

        /**
         *
         * @return number of closed files.
         */
        public static int getCountClosedWalFiles() { return WAL_CLOSE_COUNTER.get(); }
    }
}
