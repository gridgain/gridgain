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

package org.apache.ignite.internal.processors.cache.persistence.wal;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_THRESHOLD_WAL_ARCHIVE_SIZE_PERCENTAGE;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_WAL_PATH;
import static org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager.WAL_NAME_PATTERN;
import static org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager.WAL_TEMP_NAME_PATTERN;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Class for testing cases when WAL archive configuration was changed and the node was able to start.
 */
@RunWith(Parameterized.class)
@WithSystemProperty(key = IGNITE_THRESHOLD_WAL_ARCHIVE_SIZE_PERCENTAGE, value = "0.0")
public class WalArchiveConsistencyTest extends GridCommonAbstractTest {
    /**
     * WAL mode.
     */
    @Parameterized.Parameter
    public WALMode walMode;

    /**
     * @return Test parameters.
     */
    @Parameterized.Parameters(name = "walMode={0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[] {WALMode.LOG_ONLY},
            new Object[] {WALMode.FSYNC}
        );
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
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

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME))
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setWalSegments(10)
                    .setWalSegmentSize((int)U.MB)
                    .setMaxWalArchiveSize(10 * U.MB)
                    .setWalMode(walMode)
                    .setWalFsyncDelayNanos(100)
                    .setDefaultDataRegionConfiguration(
                        new DataRegionConfiguration()
                            .setPersistenceEnabled(true)
                            .setMaxSize(2 * U.GB)
                    )
            );
    }

    /** {@inheritDoc} */
    @Override protected IgniteEx startGrid(int idx, Consumer<IgniteConfiguration> cfgOp) throws Exception {
        IgniteEx n = super.startGrid(idx, cfgOp);

        n.cluster().state(ClusterState.ACTIVE);
        awaitPartitionMapExchange();

        return n;
    }

    /**
     * Verify that when switching WAL archive off -> on and increasing the
     * number of WAL segments on restarting the node, the recovery will be consistent.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testIncreaseWalSegmentsWithoutTruncate() throws Exception {
        checkRecoveryWithoutWalTruncate(12);
    }

    /**
     * Verify that when switching WAL archive off -> on and decreasing the
     * number of WAL segments on restarting the node, the recovery will be consistent.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDecreaseWalSegmentsWithoutTruncate() throws Exception {
        checkRecoveryWithoutWalTruncate(4);
    }

    /**
     * Checking that when switching WAL archive off -> on,
     * reducing WAL segments at the start of the node
     * and truncation some WAL segments, the recovery will be consistent.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDecreaseWalSegmentsWithTruncate0() throws Exception {
        checkRecoveryWithWalTruncate(5);
    }

    /**
     * Checking that when switching WAL archive off -> on,
     * reducing WAL segments at the start of the node
     * and truncation some WAL segments, the recovery will be consistent.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDecreaseWalSegmentsWithTruncate1() throws Exception {
        checkRecoveryWithWalTruncate(6);
    }

    /**
     * Checking that when switching WAL archive off -> on
     * and truncation some WAL segments, the recovery will be consistent.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNotChangeWalSegmentsWithTruncate() throws Exception {
        checkRecoveryWithWalTruncate(10);
    }

    /**
     * Checking the consistency of recovery from a WAL when switching
     * WAL archive off -> on and changing the number of segments on node restart.
     * With truncate WAL segments.
     *
     * @param segments Segment count on node restart.
     * @throws Exception If failed.
     */
    private void checkRecoveryWithWalTruncate(int segments) throws Exception {
        IgniteEx n = startGrid(0, cfg -> {
            cfg.getDataStorageConfiguration().setWalArchivePath(DFLT_WAL_PATH);
        });

        AtomicInteger key = new AtomicInteger();

        dbMgr(n).checkpointReadLock();

        try {
            fill(n, 6, key);

            // Protection against deleting WAL segments.
            assertTrue(walMgr(n).reserve(new FileWALPointer(5, 0, 0)));
        }
        finally {
            dbMgr(n).checkpointReadUnlock();
        }

        forceCheckpoint();

        assertTrue(waitForCondition(() -> walMgr(n).lastTruncatedSegment() == 4, getTestTimeout()));

        // Guaranteed recovery from WAL segments.
        dbMgr(n).enableCheckpoints(false).get(getTestTimeout());

        fill(n, 2, key);

        DataStorageConfiguration dsCfg = n.configuration().getDataStorageConfiguration();

        int walSegments = dsCfg.getWalSegments();
        File walDir = walMgr(n).getSegmentRouter().getWalWorkDir();

        stopAllGrids();

        contaminateWalWorkDirWithTmpFiles(walDir, walSegments);

        IgniteEx n0;

        if (walMode == WALMode.FSYNC) {
            AtomicReference<String> tmpSegmentLocation = new AtomicReference<>(null);
            AtomicReference<String> workSegmentLocation = new AtomicReference<>(null);

            n0 = startGrid(0, cfg -> {
                DataStorageConfiguration dsCfg0 = cfg.getDataStorageConfiguration();

                dsCfg0.setWalSegments(segments)
                    .setFileIOFactory(new LocationTrackingFileIOFactory(dsCfg0.getFileIOFactory(),
                        dsCfg0.getWalArchivePath(),
                        workSegmentLocation,
                        tmpSegmentLocation));
            });

            assertNotNull(workSegmentLocation.get());
            assertNotNull(tmpSegmentLocation.get());

            assertEquals("Tmp segment is placed in a different directory than a corresponding WAL segment: [tmp="
                    + tmpSegmentLocation.get() + ", work=" + workSegmentLocation.get() + ']',
                workSegmentLocation.get(),
                tmpSegmentLocation.get());
        }
        else {
            n0 = startGrid(0, cfg -> {
                cfg.getDataStorageConfiguration().setWalSegments(segments);
            });
        }

        checkNoTmpFilesAfterRestart(walDir, walSegments);

        assertEquals(key.get(), n0.cache(DEFAULT_CACHE_NAME).size());
    }

    /**
     * Create a tmp file in wal work directory to check that they are automatically removed on node startup.
     */
    private void contaminateWalWorkDirWithTmpFiles(File walDir, int walSegments) throws IOException {
        for (int i = 0; i < walSegments; i++) {
            String tmpSegName = FileDescriptor.fileName(i) + FilePageStoreManager.TMP_SUFFIX;

            Files.createFile(walDir.toPath().resolve(tmpSegName));
        }
    }

    /** */
    private void checkNoTmpFilesAfterRestart(File walDir, int walSegments) {
        for (int i = 0; i < walSegments; i++) {
            String tmpSegName = FileDescriptor.fileName(i) + FilePageStoreManager.TMP_SUFFIX;

            assertFalse(Files.exists(walDir.toPath().resolve(tmpSegName)));
        }
    }

    /**
     * Checking the consistency of recovery from a WAL when switching
     * WAL archive off -> on and changing the number of segments on node restart.
     * Without truncate WAL segments.
     *
     * @param segments Segment count on node restart.
     * @throws Exception If failed.
     */
    private void checkRecoveryWithoutWalTruncate(int segments) throws Exception {
        IgniteEx n = startGrid(0, cfg -> {
            cfg.getDataStorageConfiguration().setWalArchivePath(DFLT_WAL_PATH);
        });

        // Protection against deleting WAL segments.
        assertTrue(walMgr(n).reserve(new FileWALPointer(0, 0, 0)));

        AtomicInteger key = new AtomicInteger();

        fill(n, 3, key);
        forceCheckpoint();

        // Guaranteed recovery from WAL segments.
        dbMgr(n).enableCheckpoints(false).get(getTestTimeout());

        fill(n, 3, key);

        stopAllGrids();

        n = startGrid(0, cfg -> {
            cfg.getDataStorageConfiguration().setWalSegments(segments);
        });

        assertEquals(key.get(), n.cache(DEFAULT_CACHE_NAME).size());
    }

    /**
     * Filling the cache until N WAL segments are created.
     *
     * @param n Node.
     * @param segments Number of segments.
     * @param key Key counter.
     */
    private void fill(IgniteEx n, int segments, AtomicInteger key) {
        long end = walMgr(n).currentSegment() + segments;
        int i = 0;

        while (walMgr(n).currentSegment() < end) {
            int k = key.getAndIncrement();
            int[] arr = new int[64];

            Arrays.fill(arr, k);

            n.cache(DEFAULT_CACHE_NAME).put(key, arr);

            i++;
        }

        if (log.isInfoEnabled()) {
            log.info("Fill [keys=" + i + ", totalKeys=" + key.get() +
                ", segNum=" + segments + ", currSeg=" + walMgr(n).currentSegment() + ']');
        }
    }

    /**
     * FileIO factory that enables to track where WAL segments and tmp segments are created.
     */
    private static final class LocationTrackingFileIOFactory implements FileIOFactory {
        /** */
        private final FileIOFactory delegate;

        /** */
        private final String archivePath;

        /** */
        private final AtomicReference<String> workDirWalSegmentLocation;

        /** */
        private final AtomicReference<String> tmpWalSegmentLocation;

        /** */
        private LocationTrackingFileIOFactory(FileIOFactory delegate,
                                              String archivePath,
                                              AtomicReference<String> workDirWalSegmentLocation,
                                              AtomicReference<String> tmpWalSegmentLocation) {
            this.delegate = delegate;
            this.archivePath = new File(archivePath).getPath();
            this.workDirWalSegmentLocation = workDirWalSegmentLocation;
            this.tmpWalSegmentLocation = tmpWalSegmentLocation;
        }

        /** {@inheritDoc} */
        @Override public FileIO create(File file, OpenOption... modes) throws IOException {
            if (workDirWalSegmentLocation.get() == null
                && WAL_NAME_PATTERN.matcher(file.getName()).matches()
                && !file.getAbsolutePath().contains(archivePath)
            )
                workDirWalSegmentLocation.set(file.getParentFile().getAbsolutePath());

            if (tmpWalSegmentLocation.get() == null && WAL_TEMP_NAME_PATTERN.matcher(file.getName()).matches())
                tmpWalSegmentLocation.set(file.toPath().toAbsolutePath().getParent().toAbsolutePath().toString());

            return delegate.create(file, modes);
        }
    }
}
