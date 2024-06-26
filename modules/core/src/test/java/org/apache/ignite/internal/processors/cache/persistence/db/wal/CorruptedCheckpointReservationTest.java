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

package org.apache.ignite.internal.processors.cache.persistence.db.wal;

import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.crc.WalTestUtils;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileDescriptor;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.FilteredWalIterator;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.WalFilters;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_DISABLE_GRP_STATE_LAZY_STORE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.testframework.GridTestUtils.wal;

/**
 * Tests if reservation of corrupted checkpoint works correctly, also checks correct behaviour for corrupted zip wal file
 * during PME.
 */
@WithSystemProperty(key = IGNITE_PDS_WAL_REBALANCE_THRESHOLD, value = "0")
@WithSystemProperty(key = IGNITE_DISABLE_GRP_STATE_LAZY_STORE, value = "true")
public class CorruptedCheckpointReservationTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String CACHE_NAME = "cache";

    /** Partitions count. */
    private static final int PARTS_CNT = 16;

    /** Wal compaction enabled. */
    private boolean walCompactionEnabled;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setConsistentId(gridName);

        CacheConfiguration<Object, Object> ccfg1 = new CacheConfiguration<>(CACHE_NAME)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setCacheMode(CacheMode.PARTITIONED)
            .setBackups(1)
            .setAffinity(new RendezvousAffinityFunction(false, PARTS_CNT));

        cfg.setCacheConfiguration(ccfg1);

        DataStorageConfiguration dbCfg = new DataStorageConfiguration()
            .setWalMode(WALMode.LOG_ONLY)
            .setWalSegmentSize(1024 * 1024)
            .setCheckpointFrequency(Integer.MAX_VALUE)
            .setWalCompactionEnabled(walCompactionEnabled)
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setPersistenceEnabled(true)
                    .setMaxSize(100 * 1024 * 1024)
            );

        cfg.setDataStorageConfiguration(dbCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testCorruptedCheckpointReservation() throws Exception {
        walCompactionEnabled = false;

        startGrids(2);

        IgniteEx ig0 = grid(0);

        ig0.cluster().state(ACTIVE);

        generateCps(ig0);

        corruptWalRecord(ig0, 3, false);

        startGrid(1);

        awaitPartitionMapExchange();
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testCorruptedCheckpointInCompressedWalReservation() throws Exception {
        walCompactionEnabled = true;

        startGrids(2);

        IgniteEx ig0 = grid(0);

        ig0.cluster().state(ACTIVE);

        generateCps(ig0);

        corruptWalRecord(ig0, 3, true);

        startGrid(1);

        awaitPartitionMapExchange();
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testCorruptedCompressedWalSegment() throws Exception {
        walCompactionEnabled = true;

        startGrids(2);

        IgniteEx ig0 = grid(0);

        ig0.cluster().state(ACTIVE);

        generateCps(ig0);

        corruptCompressedWalSegment(ig0, 3);

        startGrid(1);

        awaitPartitionMapExchange();
    }

    /**
     * @param ig Ignite.
     */
    private void generateCps(IgniteEx ig) throws IgniteCheckedException {
        IgniteCache<Object, Object> cache = ig.cache(CACHE_NAME);

        final int entryCnt = PARTS_CNT * 1000;

        for (int i = 0; i < entryCnt; i++)
            cache.put(i, i);

        forceCheckpoint();

        for (int i = 0; i < entryCnt; i++)
            cache.put(i, i);

        forceCheckpoint();

        stopGrid(1);

        for (int i = 0; i < entryCnt; i++)
            cache.put(i, i + 100);

        forceCheckpoint();

        for (int i = 0; i < entryCnt; i++)
            cache.put(i, i + 1000);

        forceCheckpoint();

        for (int i = 0; i < entryCnt; i++)
            cache.put(i, i + 10000);

        forceCheckpoint();
    }

    /**
     * @param ig Ignite.
     * @param cpIdx Checkpoint index.
     */
    private void corruptWalRecord(IgniteEx ig, int cpIdx, boolean segmentCompressed) throws IgniteCheckedException, IOException {
        IgniteWriteAheadLogManager walMgr = wal(ig);

        FileWALPointer corruptedCp = getCp(ig, cpIdx);

        if (segmentCompressed)
            GridTestUtils.waitForCondition(() -> walMgr.lastCompactedSegment() >= corruptedCp.index(), getTestTimeout());

        Optional<FileDescriptor> cpSegment = getFileDescriptor(segmentCompressed, walMgr, corruptedCp);

        if (segmentCompressed) {
            assertTrue("Cannot find " + FilePageStoreManager.ZIP_SUFFIX + " segment for checkpoint.", cpSegment.isPresent());

            WalTestUtils.corruptWalRecordInCompressedSegment(cpSegment.get(), corruptedCp);
        }
        else {
            assertTrue("Cannot find " + FileDescriptor.WAL_SEGMENT_FILE_EXT + " segment for checkpoint.", cpSegment.isPresent());

            WalTestUtils.corruptWalRecord(cpSegment.get(), corruptedCp);
        }
    }

    /**
     * @param segmentCompressed Segment compressed.
     * @param walMgr Wal manager.
     * @param corruptedCp Corrupted checkpoint.
     */
    @NotNull private Optional<FileDescriptor> getFileDescriptor(boolean segmentCompressed,
        IgniteWriteAheadLogManager walMgr, FileWALPointer corruptedCp) throws IOException {

        File walArchiveDir = U.field(walMgr, "walArchiveDir");

        String suffix = segmentCompressed ? FilePageStoreManager.ZIP_SUFFIX : FileDescriptor.WAL_SEGMENT_FILE_EXT;
        AtomicReference<File> wantedFile = new AtomicReference<>();
        String corruptedIdx = Long.toString(corruptedCp.index());
        Files.walkFileTree(walArchiveDir.toPath(), new SimpleFileVisitor<Path>() {
            @Override public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) {
                String fileName = path.toFile().getName();

                if (fileName.endsWith(suffix) && fileName.contains(corruptedIdx)) {
                    wantedFile.set(path.toFile());

                    return FileVisitResult.TERMINATE;
                }

                return FileVisitResult.CONTINUE;
            }
        });

        return Optional.ofNullable(wantedFile.get() != null ? new FileDescriptor(wantedFile.get()) : null);
    }

    /**
     * @param ig Ignite.
     * @param cpIdx Checkpoint index.
     */
    private void corruptCompressedWalSegment(IgniteEx ig, int cpIdx) throws IgniteCheckedException, IOException {
        IgniteWriteAheadLogManager walMgr = wal(ig);

        FileWALPointer corruptedCp = getCp(ig, cpIdx);

        Optional<FileDescriptor> cpSegment = getFileDescriptor(true, walMgr, corruptedCp);

        assertTrue("Cannot find " + FilePageStoreManager.ZIP_SUFFIX + " segment for checkpoint.", cpSegment.isPresent());

        WalTestUtils.corruptCompressedFile(cpSegment.get());
    }

    /**
     * @param ig Ignite.
     * @param cpIdx Checkpoint index.
     */
    private FileWALPointer getCp(IgniteEx ig, int cpIdx) throws IgniteCheckedException {
        IgniteWriteAheadLogManager walMgr = wal(ig);

        List<IgniteBiTuple<WALPointer, WALRecord>> checkpoints;

        try (FilteredWalIterator iter = new FilteredWalIterator(walMgr.replay(null), WalFilters.checkpoint())) {
            checkpoints = Lists.newArrayList((Iterable<? extends IgniteBiTuple<WALPointer, WALRecord>>)iter);
        }

        return (FileWALPointer) checkpoints.get(cpIdx).get2().position();
    }

    /**
     * @param walDir Wal directory.
     * @param iterFactory Iterator factory.
     */
    private List<FileDescriptor> getWalFiles(File walDir, IgniteWalIteratorFactory iterFactory) {
        return iterFactory.resolveWalFiles(
            new IgniteWalIteratorFactory.IteratorParametersBuilder()
                .filesOrDirs(walDir)
        );
    }
}
