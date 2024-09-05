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

package org.apache.ignite.internal.processors.diagnostic;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.LongStream;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.record.PageSnapshot;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.GridCacheUtils;
import org.apache.ignite.internal.processors.cache.persistence.CheckpointState;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointMarkersStorage;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.tree.CorruptedTreeException;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileDescriptor;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.SegmentRouter;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.encryption.keystore.KeystoreEncryptionSpi;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.GridAbstractTest;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_WAL_PATH;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.OLD_METASTORE_PARTITION;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CACHE_DATA_FILENAME;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.INDEX_FILE_NAME;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.PART_FILE_TEMPLATE;
import static org.apache.ignite.internal.processors.diagnostic.DiagnosticProcessor.DEFAULT_TARGET_FOLDER;
import static org.apache.ignite.internal.processors.diagnostic.DiagnosticProcessor.corruptedPagesFile;
import static org.apache.ignite.internal.processors.diagnostic.DiagnosticProcessor.walDirs;
import static org.apache.ignite.testframework.GridTestUtils.getFieldValue;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertThat;

/**
 * Class for testing diagnostics.
 */
@WithSystemProperty(key = IgniteSystemProperties.IGNITE_DUMP_PERSISTENCE_FILES_ON_DATA_CORRUPTION, value = "true")
public class DiagnosticProcessorTest extends GridCommonAbstractTest {
    /** Keystore path for encryption SPI. */
    private static final String KEYSTORE_PATH =
        IgniteUtils.resolveIgnitePath("modules/core/src/test/resources/tde.jks").getAbsolutePath();

    /** Keystore password. */
    private static final String KEYSTORE_PASSWORD = "tde-password";

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
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        return super.getConfiguration(gridName)
            .setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME))
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true))
            );
    }

    /** {@inheritDoc} */
    @Override protected void cleanPersistenceDir() throws Exception {
        super.cleanPersistenceDir();

        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), DEFAULT_TARGET_FOLDER, false));
    }

    /**
     * Checks the correctness of the {@link DiagnosticProcessor#corruptedPagesFile}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCorruptedPagesFile() throws Exception {
        File tmpDir = new File(System.getProperty("java.io.tmpdir"), getName());

        try {
            int grpId = 10;
            long[] pageIds = {20, 40};

            File f = corruptedPagesFile(tmpDir.toPath(), new RandomAccessFileIOFactory(), grpId, pageIds);

            assertTrue(f.exists());
            assertTrue(f.isFile());
            assertTrue(f.length() > 0);
            assertTrue(Arrays.asList(tmpDir.listFiles()).contains(f));
            assertTrue(corruptedPagesFileNamePattern().matcher(f.getName()).matches());

            try (BufferedReader br = new BufferedReader(new FileReader(f))) {
                List<String> lines = br.lines().collect(toList());
                List<String> pageStrs = LongStream.of(pageIds).mapToObj(pageId -> grpId + ":" + pageId).collect(toList());

                assertEqualsCollections(lines, pageStrs);
            }
        }
        finally {
            if (tmpDir.exists())
                assertTrue(U.delete(tmpDir));
        }
    }

    /**
     * Checks the correctness of the {@link DiagnosticProcessor#walDirs}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testWalDirs() throws Exception {
        IgniteEx n = startGrid(0);

        // Work + archive dirs.
        File[] expWalDirs = expWalDirs(n);
        assertEquals(2, expWalDirs.length);
        assertEqualsCollections(F.asList(expWalDirs), F.asList(walDirs(n.context())));

        stopAllGrids();
        cleanPersistenceDir();

        n = startGrid(0,
            (Consumer<IgniteConfiguration>)cfg -> cfg.getDataStorageConfiguration().setWalArchivePath(DFLT_WAL_PATH));

        // Only work dir.
        expWalDirs = expWalDirs(n);
        assertEquals(1, expWalDirs.length);
        assertEqualsCollections(F.asList(expWalDirs), F.asList(walDirs(n.context())));

        stopAllGrids();
        cleanPersistenceDir();

        n = startGrid(0,
            (Consumer<IgniteConfiguration>)cfg -> cfg.setDataStorageConfiguration(new DataStorageConfiguration()));

        // No wal dirs.
        assertNull(expWalDirs(n));
        assertNull(walDirs(n.context()));
    }

    /**
     * Simple POJO to generate binary metadata in {@link #doTestOutputDiagnosticCorruptedPagesInfo(Consumer, BiConsumer)}
     */
    public static class TestValue {
        public final String val;

        public TestValue(String val) {
            this.val = val;
        }
    }

    /**
     * Check that when an CorruptedTreeException is thrown, a "corruptedPages_TIMESTAMP.txt" will be created, data will
     * be dumped and a warning will be in the log.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testOutputDiagnosticCorruptedPagesInfo() throws Exception {
        doTestOutputDiagnosticCorruptedPagesInfo(cfg -> {}, (n, anyPageId) -> {
            validateDiagnosticPathDir(n);

            File baseDumpDir = getBaseDumpDir(n);
            assertEquals(7, baseDumpDir.list().length);

            // 6 folders to validate.
            validateCacheDir(anyPageId, baseDumpDir);
            validateWalDir(baseDumpDir);
            validateBinaryMetaDir(baseDumpDir);
            validateCpDir(baseDumpDir);
            validateMetaStorageDir(baseDumpDir);
            validateUtilityCacheDir(baseDumpDir);
            validateLogsCacheDir(baseDumpDir);
        });
    }

    /**
     * Check that when an CorruptedTreeException is thrown, a "corruptedPages_TIMESTAMP.txt" will be created, data will
     * be dumped and a warning will be in the log.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testOutputDiagnosticCorruptedPagesInfoEncryptionEnabled() throws Exception {
        Consumer<IgniteConfiguration> cfgClosure = cfg -> {
            KeystoreEncryptionSpi encSpi = new KeystoreEncryptionSpi();

            encSpi.setKeyStorePath(KEYSTORE_PATH);
            encSpi.setKeyStorePassword(KEYSTORE_PASSWORD.toCharArray());

            cfg.setEncryptionSpi(encSpi);
        };

        doTestOutputDiagnosticCorruptedPagesInfo(cfgClosure, (n, anyPageId) -> {
            validateDiagnosticPathDir(n);

            File baseDumpDir = getBaseDumpDir(n);
            assertEquals(8, baseDumpDir.list().length);

            // 7 folders to validate.
            validateCacheDir(anyPageId, baseDumpDir);
            validateWalDir(baseDumpDir);
            validateBinaryMetaDir(baseDumpDir);
            validateCpDir(baseDumpDir);
            validateMetaStorageDir(baseDumpDir);
            validateUtilityCacheDir(baseDumpDir);
            validateLogsCacheDir(baseDumpDir);
            validateJksDir(baseDumpDir);
        });
    }

    private void validateDiagnosticPathDir(IgniteEx n) {
        Path diagnosticPath = getFieldValue(n.context().diagnostic(), "diagnosticPath");

        List<File> corruptedPagesFiles = Arrays.stream(diagnosticPath.toFile().listFiles())
            .filter(f -> corruptedPagesFileNamePattern().matcher(f.getName()).matches()).collect(toList());

        assertEquals(1, corruptedPagesFiles.size());
        assertTrue(corruptedPagesFiles.get(0).length() > 0);
    }

    /**
     * Returns {@code <work>/db/dump} directory, that contains all dumped information.
     */
    private static File getBaseDumpDir(IgniteEx n) {
        try {
            return n.context().diagnostic().getBaseDumpDir();
        }
        catch (IgniteCheckedException e) {
            fail(e.getMessage());

            return null;
        }
    }

    /**
     * Validates that {@code <baseDumpDir>/<cacheId>} directory exists and contains all necessary cache files.
     */
    private static void validateCacheDir(T2<Integer, Long> anyPageId, File baseDumpDir) {
        File partitionsPath = new File(baseDumpDir, Integer.toString(anyPageId.get1()));
        assertTrue(partitionsPath.exists());
        assertTrue(partitionsPath.isDirectory());

        // cache_data.dat
        assertEquals(3, partitionsPath.list().length);
        assertTrue(new File(partitionsPath, CACHE_DATA_FILENAME).exists());

        // index.bin
        assertTrue(new File(partitionsPath, INDEX_FILE_NAME).exists());

        // part-N.bin
        String partFileName = String.format(PART_FILE_TEMPLATE, PageIdUtils.partId(anyPageId.get2()));
        assertTrue(new File(partitionsPath, partFileName).exists());
    }

    /**
     * Validates that {@code <baseDumpDir>/wal} directory exists and contains WAL segments.
     */
    private static void validateWalDir(File baseDumpDir) {
        File walPath = new File(baseDumpDir, "wal");
        assertTrue(walPath.exists());
        assertTrue(walPath.isDirectory());

        assertEquals(1, walPath.list().length);
        assertTrue(new File(walPath, FileDescriptor.fileName(0)).exists());
    }

    /**
     * Validates that {@code <baseDumpDir>/binary_meta} directory exists and contains binary meta files.
     */
    private static void validateBinaryMetaDir(File baseDumpDir) {
        // "dump/binary_meta" directory should contain a single file.
        File metaPath = new File(baseDumpDir, "binary_meta");
        assertTrue(metaPath.exists());
        assertTrue(metaPath.isDirectory());

        assertEquals(1, metaPath.list().length);
    }

    /**
     * Validates that {@code <baseDumpDir>/cp} directory exists and contains checkpoint history snapshot and markers.
     */
    private static void validateCpDir(File baseDumpDir) {
        File cpPath = new File(baseDumpDir, "cp");
        assertTrue(cpPath.exists());
        assertTrue(cpPath.isDirectory());

        assertThat(cpPath.list().length, greaterThanOrEqualTo(3));
        assertTrue(new File(cpPath, CheckpointMarkersStorage.EARLIEST_CP_SNAPSHOT_FILE).exists());
    }

    /**
     * Validates that {@code <baseDumpDir>/metastorage} directory exists and contains meta storage partition files.
     */
    private static void validateMetaStorageDir(File baseDumpDir) {
        File metaStoragePath = new File(baseDumpDir, "metastorage");
        assertTrue(metaStoragePath.exists());
        assertTrue(metaStoragePath.isDirectory());

        assertEquals(1, metaStoragePath.list().length);
        // Somehow we use partition 0 instead of partition 1 for meta storage in this test, I don't know why.
        String msPartFileName = String.format(PART_FILE_TEMPLATE, OLD_METASTORE_PARTITION);
        assertTrue(new File(metaStoragePath, msPartFileName).exists());
    }

    /**
     * Validates that {@code <baseDumpDir>/sys-cache} directory exists and contains all necessary cache files.
     */
    private static void validateUtilityCacheDir(File baseDumpDir) {
        File utilityCachePath = new File(baseDumpDir, "sys-cache");
        assertTrue(utilityCachePath.exists());
        assertTrue(utilityCachePath.isDirectory());

        // Also includes a single partition file.
        assertEquals(3, utilityCachePath.list().length);

        // cache_data.dat
        assertTrue(new File(utilityCachePath, CACHE_DATA_FILENAME).exists());

        // index.bin
        assertTrue(new File(utilityCachePath, INDEX_FILE_NAME).exists());
    }

    /**
     * Validates that {@code <baseDumpDir>/log} directory exists and contains log files.
     */
    private void validateLogsCacheDir(File baseDumpDir) {
        File logPath = new File(baseDumpDir, "log");
        assertTrue(logPath.exists());
        assertTrue(logPath.isDirectory());

        assertThat(logPath.list().length, greaterThanOrEqualTo(1));
    }

    /**
     * Validates that {@code <baseDumpDir>/jks} directory exists and contains all necessary files.
     */
    private void validateJksDir(File baseDumpDir) {
        File jksPath = new File(baseDumpDir, "jks");
        assertTrue(jksPath.exists());
        assertTrue(jksPath.isDirectory());

        assertEquals(2, jksPath.list().length);
        assertTrue(new File(jksPath, "keystore.jks").exists());
        assertTrue(new File(jksPath, "extras.txt").exists());
    }

    /** */
    private void doTestOutputDiagnosticCorruptedPagesInfo(
        Consumer<IgniteConfiguration> cfgClosure,
        BiConsumer<IgniteEx, T2<Integer, Long>> validator
    ) throws Exception {
        ListeningTestLogger listeningTestLog = new ListeningTestLogger(GridAbstractTest.log);

        IgniteEx n = startGrid(0, cfg -> {
            cfg.setGridLogger(listeningTestLog).setConsistentId(getTestIgniteInstanceName(0));

            cfgClosure.accept(cfg);
        });

        n.cluster().state(ClusterState.ACTIVE);
        awaitPartitionMapExchange();

        for (int i = 0; i < 10_000; i++)
            n.cache(DEFAULT_CACHE_NAME).put(i, new TestValue("val_" + i));

        n.context().cache().utilityCache().put("key", "value");

        n.context().cache().context().database()
            .forceCheckpoint("test")
            .futureFor(CheckpointState.FINISHED)
            .get(getTestTimeout());

        assertNotNull(n.context().diagnostic());

        T2<Integer, Long> anyPageId = findAnyPageId(n);
        assertNotNull(anyPageId);

        LogListener logLsnr = LogListener.matches("CorruptedTreeException has occurred. " +
            "To diagnose it, make a backup of the following directories: ").build();

        listeningTestLog.registerListener(logLsnr);

        n.context().failure().process(new FailureContext(FailureType.CRITICAL_ERROR,
            new CorruptedTreeException("Test ex", null, DEFAULT_CACHE_NAME, anyPageId.get1(), anyPageId.get2())));

        assertTrue(logLsnr.check());

        validator.accept(n, anyPageId);
    }

    /**
     * Find first any page id for test.
     *
     * @param n Node.
     * @return Page id in WAL.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable private T2<Integer, Long> findAnyPageId(IgniteEx n) throws IgniteCheckedException {
        int cacheId = GridCacheUtils.cacheId(DEFAULT_CACHE_NAME);

        try (WALIterator walIter = n.context().cache().context().wal().replay(new FileWALPointer(0, 0, 0))) {
            while (walIter.hasNextX()) {
                WALRecord walRecord = walIter.nextX().get2();

                if (walRecord instanceof PageSnapshot) {
                    PageSnapshot rec = (PageSnapshot)walRecord;

                    if (rec.fullPageId().groupId() != cacheId)
                        continue;

                    int partId = PageIdUtils.partId(rec.fullPageId().pageId());
                    if (partId == PageIdAllocator.INDEX_PARTITION)
                        continue;

                    return new T2<>(rec.groupId(), rec.fullPageId().pageId());
                }
            }
        }

        return null;
    }

    /**
     * Getting expected WAL directories.
     *
     * @param n Node.
     * @return WAL directories.
     */
    @Nullable private File[] expWalDirs(IgniteEx n) {
        FileWriteAheadLogManager walMgr = walMgr(n);

        if (walMgr != null) {
            SegmentRouter sr = walMgr.getSegmentRouter();
            assertNotNull(sr);

            File workDir = sr.getWalWorkDir();
            return sr.hasArchive() ? F.asArray(workDir, sr.getWalArchiveDir()) : F.asArray(workDir);
        }

        return null;
    }

    /**
     * Getting pattern corrupted pages file name.
     *
     * @return Pattern.
     */
    private Pattern corruptedPagesFileNamePattern() {
        return Pattern.compile("corruptedPages_\\d{4}-\\d{2}-\\d{2}_\\d{2}-\\d{2}-\\d{2}_\\d{3}\\.txt");
    }
}
