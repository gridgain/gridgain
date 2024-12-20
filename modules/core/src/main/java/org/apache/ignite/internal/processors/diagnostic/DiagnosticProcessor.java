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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.URL;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.stream.LongStream;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.pagemem.store.IgnitePageStoreManager;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl;
import org.apache.ignite.internal.processors.cache.persistence.CorruptedDataStructureException;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.SegmentRouter;
import org.apache.ignite.internal.processors.cacheobject.IgniteCacheObjectProcessor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.encryption.EncryptionSpi;
import org.apache.ignite.spi.encryption.keystore.KeystoreEncryptionSpi;
import org.jetbrains.annotations.Nullable;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.joining;
import static org.apache.ignite.IgniteSystemProperties.getBoolean;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_PAGE_SIZE;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.isPersistenceEnabled;

/**
 * Processor which contained helper methods for different diagnostic cases.
 */
public class DiagnosticProcessor extends GridProcessorAdapter {
    /** @see IgniteSystemProperties#IGNITE_DUMP_PAGE_LOCK_ON_FAILURE */
    public static final boolean DFLT_DUMP_PAGE_LOCK_ON_FAILURE = true;

    /** Value of the system property that enables page locks dumping on failure. */
    private static final boolean IGNITE_DUMP_PAGE_LOCK_ON_FAILURE = getBoolean(
        IgniteSystemProperties.IGNITE_DUMP_PAGE_LOCK_ON_FAILURE, DFLT_DUMP_PAGE_LOCK_ON_FAILURE);

    /** Time formatter for dump file name. */
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'_'HH-mm-ss_SSS");

    /** Folder name for store diagnostic info. **/
    public static final String DEFAULT_TARGET_FOLDER = "diagnostic";

    /** Full path for store dubug info. */
    private final Path diagnosticPath;

    /** Reconciliation execution context. */
    private final ReconciliationExecutionContext reconciliationExecutionContext;

    /** File I/O factory. */
    @Nullable private final FileIOFactory fileIOFactory;

    /** Dumps latest WAL segments and related index and partition files on data corruption error. */
    private final boolean dumpPersistenceFilesOnDataCorruption = getBoolean(
            IgniteSystemProperties.IGNITE_DUMP_PERSISTENCE_FILES_ON_DATA_CORRUPTION);

    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     */
    public DiagnosticProcessor(GridKernalContext ctx) throws IgniteCheckedException {
        super(ctx);

        diagnosticPath = U.resolveWorkDirectory(ctx.config().getWorkDirectory(), DEFAULT_TARGET_FOLDER, false)
            .toPath();

        reconciliationExecutionContext = new ReconciliationExecutionContext(ctx);

        fileIOFactory = isPersistenceEnabled(ctx.config()) ?
            ctx.config().getDataStorageConfiguration().getFileIOFactory() : null;
    }

    /**
     * Print diagnostic info about failure occurred on {@code ignite} instance.
     * Failure details is contained in {@code failureCtx}.
     *
     * @param failureCtx Failure context.
     */
    public void onFailure(FailureContext failureCtx) {
        // Dump data structures page locks.
        if (IGNITE_DUMP_PAGE_LOCK_ON_FAILURE)
            ctx.cache().context().diagnostic().pageLockTracker().dumpLocksToLog();

        CorruptedDataStructureException corruptedDataStructureEx =
            X.cause(failureCtx.error(), CorruptedDataStructureException.class);

        if (corruptedDataStructureEx != null && !F.isEmpty(corruptedDataStructureEx.pageIds()) && fileIOFactory != null) {
            File[] walDirs = walDirs(ctx);

            if (F.isEmpty(walDirs)) {
                if (log.isInfoEnabled())
                    log.info("Skipping dump diagnostic info due to WAL not configured");
            }
            else {
                try {
                    File corruptedPagesFile = corruptedPagesFile(
                        diagnosticPath,
                        fileIOFactory,
                        corruptedDataStructureEx.groupId(),
                        corruptedDataStructureEx.pageIds()
                    );

                    String walDirsStr = Arrays.stream(walDirs).map(File::getAbsolutePath)
                        .collect(joining(", ", "[", "]"));

                    String args = "--wal-dir " + walDirs[0].getAbsolutePath() + (walDirs.length == 1 ? "" :
                        " --wal-archive-dir " + walDirs[1].getAbsolutePath());

                    if (ctx.config().getDataStorageConfiguration().getPageSize() != DFLT_PAGE_SIZE)
                        args += " --page-size " + ctx.config().getDataStorageConfiguration().getPageSize();

                    args += " --pages " + corruptedPagesFile.getAbsolutePath();

                    log.warning(corruptedDataStructureEx.getClass().getSimpleName() + " has occurred. " +
                        "To diagnose it, make a backup of the following directories: " + walDirsStr + ". " +
                        "Then, run the following command: bin/wal-reader.sh " + args);
                }
                catch (Throwable t) {
                    String pages = LongStream.of(corruptedDataStructureEx.pageIds())
                        .mapToObj(pageId -> corruptedDataStructureEx.groupId() + ":" + pageId)
                        .collect(joining("\n", "", ""));

                    log.error("Failed to dump diagnostic info of partition corruption. Page ids:\n" + pages, t);
                }
            }
        }

        if (dumpPersistenceFilesOnDataCorruption && corruptedDataStructureEx != null) {
            log.warning("Copying persistence files to dump folder.");

            dumpPersistenceFilesOnFailure(corruptedDataStructureEx);
        }
    }

    /** Return a directory that will contain dumped files after a tree corruption. */
    File getBaseDumpDir() throws IgniteCheckedException {
        Serializable consistentId = ctx.config().getConsistentId();
        String path = "db/dump/" + U.maskForFileName(String.valueOf(consistentId));

        return U.resolveWorkDirectory(ctx.config().getWorkDirectory(), path, false);
    }

    /** Dumps latest WAL segments and related index and partition files on data corruption error. */
    private void dumpPersistenceFilesOnFailure(CorruptedDataStructureException ex) {
        File baseDumpDir;
        try {
            baseDumpDir = getBaseDumpDir();
        }
        catch (IgniteCheckedException e) {
            log.error("Unable to resolve dump directory", e);

            return;
        }

        // Do it first, without encryption keys we can't really read WAL.
        EncryptionSpi encSpi = ctx.config().getEncryptionSpi();
        if (encSpi instanceof KeystoreEncryptionSpi) {
            // We can't call "encSpi.dumpKeys" because it's a public class, we shouldn't add unnecessary methods to it.
            dumpEncryptionKeys((KeystoreEncryptionSpi)encSpi, baseDumpDir);
        }

        IgniteCacheObjectProcessor processor = ctx.cacheObjects();
        if (processor instanceof CacheObjectBinaryProcessorImpl)
            ((CacheObjectBinaryProcessorImpl)processor).dumpMetadata(baseDumpDir);

        dumpLogs(baseDumpDir);

        IgniteWriteAheadLogManager wal = ctx.cache().context().wal();
        if (wal instanceof FileWriteAheadLogManager)
            ((FileWriteAheadLogManager) wal).dumpWalFiles(baseDumpDir);

        IgnitePageStoreManager storeManager = ctx.cache().context().pageStore();
        if (storeManager instanceof FilePageStoreManager) {
            ((FilePageStoreManager)storeManager).dumpPartitionFiles(baseDumpDir, ex.groupId(), ex.pageIds());

            ((FilePageStoreManager)storeManager).dumpUtilityCache(baseDumpDir);
        }

        IgniteCacheDatabaseSharedManager dbSharedManager = ctx.cache().context().database();
        if (dbSharedManager instanceof GridCacheDatabaseSharedManager)
            ((GridCacheDatabaseSharedManager)dbSharedManager).dumpMetaStorageAndCheckpoints(baseDumpDir);
    }

    /** Dumps keystore and encryption SPI settings. */
    private void dumpEncryptionKeys(KeystoreEncryptionSpi encSpi, File baseDumpDir) {
        InputStream jksInputStream = null;

        try {
            File dumpDir = new File(baseDumpDir, "jks");
            dumpDir.mkdirs();

            log.warning(
                "Sensitive encryption information is being collected into " +
                dumpDir.getAbsolutePath() +
                " in order to make further corruption analysis possible."
            );

            String keyStorePath = encSpi.getKeyStorePath();

            // Copy of "org.apache.ignite.spi.encryption.keystore.KeystoreEncryptionSpi.keyStoreFile".
            File absolutePathKeyStoreFile = new File(keyStorePath);

            if (absolutePathKeyStoreFile.exists())
                jksInputStream = new FileInputStream(absolutePathKeyStoreFile);

            if (jksInputStream == null) {
                URL clsPthRes = KeystoreEncryptionSpi.class.getClassLoader().getResource(keyStorePath);

                if (clsPthRes != null)
                    jksInputStream = clsPthRes.openStream();
            }

            if (jksInputStream != null)
                writeStreamToFile(jksInputStream, new File(dumpDir, "keystore.jks"));

            String extras = String.format(
                "keySize=%d\n" +
                "keyStorePassword=%s\n" +
                "masterKeyName=%s",
                encSpi.getKeySize(),
                new String(encSpi.getKeyStorePwd()),
                encSpi.getMasterKeyName()
            );

            // No need to close byte array input stream when we're done.
            writeStreamToFile(new ByteArrayInputStream(extras.getBytes(UTF_8)), new File(dumpDir, "extras.txt"));
        }
        catch (Throwable t) {
            if (jksInputStream != null) {
                try {
                    jksInputStream.close();
                }
                catch (Throwable e) {
                    t.addSuppressed(e);
                }

                jksInputStream = null;
            }

            log.error("Failed to dump encryption keys.", t);
        }
        finally {
            if (jksInputStream != null) {
                try {
                    jksInputStream.close();
                }
                catch (Throwable t) {
                    log.error("Failed to close JKS input stream.", t);
                }
            }
        }
    }

    /** Dumps "log" dir if it exists. */
    private void dumpLogs(File baseDumpDir) {
        try {
            File logDir = U.resolveWorkDirectory(ctx.config().getWorkDirectory(), "log", false);

            if (logDir.exists() && logDir.isDirectory()) {
                File dumpDir = new File(baseDumpDir, "log");

                for (File logFile : logDir.listFiles()) {
                    U.copy(logFile, new File(dumpDir, logFile.getName()), false);
                }
            }
        }
        catch (Exception e) {
            log.error("Failed to dump logs.", e);
        }
    }

    /** Writes all data from the input stream into a given file. */
    private static void writeStreamToFile(InputStream in, File outFile) throws IOException {
        try (
            FileOutputStream out = new FileOutputStream(outFile)
        ) {
            U.copy(in, out);

            out.getFD().sync();
        }
    }

    /**
     * @return Reconciliation execution context.
     */
    public ReconciliationExecutionContext reconciliationExecutionContext() {
        return reconciliationExecutionContext;
    }

    /**
     * Creation and filling of a file with pages that can be corrupted.
     * Pages are written on each line in format "grpId:pageId".
     * File name format "corruptedPages_yyyy-MM-dd'_'HH-mm-ss_SSS.txt".
     *
     * @param dirPath   Path to the directory where the file will be created.
     * @param ioFactory File I/O factory.
     * @param grpId     Cache group id.
     * @param pageIds   PageId's that can be corrupted.
     * @return Created and filled file.
     * @throws IOException If an I/O error occurs.
     */
    public static File corruptedPagesFile(
        Path dirPath,
        FileIOFactory ioFactory,
        int grpId,
        long... pageIds
    ) throws IOException {
        dirPath.toFile().mkdirs();

        File f = dirPath.resolve("corruptedPages_" + LocalDateTime.now().format(TIME_FORMATTER) + ".txt").toFile();

        assert !f.exists();

        try (FileIO fileIO = ioFactory.create(f)) {
            for (long pageId : pageIds) {
                byte[] bytes = (grpId + ":" + pageId + U.nl()).getBytes(UTF_8);

                int left = bytes.length;

                while ((left - fileIO.writeFully(bytes, bytes.length - left, left)) > 0)
                    ;
            }

            fileIO.force();
        }

        return f;
    }

    /**
     * Getting the WAL directories.
     * Note:
     * Index 0: WAL working directory.
     * Index 1: WAL archive directory (may be absent).
     *
     * @param ctx Kernal context.
     * @return WAL directories.
     */
    @Nullable static File[] walDirs(GridKernalContext ctx) {
        IgniteWriteAheadLogManager walMgr = ctx.cache().context().wal();

        if (walMgr instanceof FileWriteAheadLogManager) {
            SegmentRouter sr = ((FileWriteAheadLogManager)walMgr).getSegmentRouter();

            if (sr != null) {
                File workDir = sr.getWalWorkDir();
                return sr.hasArchive() ? F.asArray(workDir, sr.getWalArchiveDir()) : F.asArray(workDir);
            }
        }

        return null;
    }
}
