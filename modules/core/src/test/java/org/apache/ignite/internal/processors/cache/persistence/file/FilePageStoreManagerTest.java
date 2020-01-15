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

package org.apache.ignite.internal.processors.cache.persistence.file;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.filename.PdsFolderSettings;
import org.apache.ignite.internal.processors.cache.persistence.filename.PdsFoldersResolver;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.NoOpPageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.NoOpWALManager;
import org.apache.ignite.internal.processors.plugin.IgnitePluginProcessor;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.GridTestKernalContext;
import org.apache.ignite.testframework.junits.logger.GridTestLog4jLogger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CACHE_DIR_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CACHE_GRP_DIR_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.FILE_SUFFIX;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.META_STORAGE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for {@link FilePageStoreManager} class.
 */
public class FilePageStoreManagerTest {
    private final IgniteLogger log = new GridTestLog4jLogger();

    /** */
    private static GridTestKernalContext ctx;

    /** */
    private static GridCacheSharedContext<Object, Object> sharedCtx;

    /** */
    private static IgniteConfiguration cfg;

    /** */
    private File storeBasePath;

    /** */
    private String consId = UUID.randomUUID().toString().replace('-', '_');

    /** File suffix of a file from cache working directory that should not be cleaned up by FilePageStoreManager. */
    private static final String RANDOM_FILE_SUFFIX = ".dat";

    /** Directory name that should not be cleaned up by FilePageStoreManager on cleanPersistenceSpace method call
     * as it doesn't match pattern of directory name to delete. */
    private static final String RANDOM_DIR_NAME = "randomDir";

    /** */
    @Before public void beforeTest() throws Exception {
        cfg = new IgniteConfiguration();
        cfg.setClientMode(false);
        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true)));

        ctx = new GridTestKernalContext(log, cfg) {
            @Override public PdsFoldersResolver pdsFolderResolver() {
                return new PdsFoldersResolver() {
                    @Override public PdsFolderSettings resolveFolders() throws IgniteCheckedException {
                        return new PdsFolderSettings(storeBasePath, consId);
                    }
                };
            }
        };

        ctx.add(new IgnitePluginProcessor(ctx, cfg, Collections.emptyList()));

        sharedCtx = new GridCacheSharedContext<>(
            ctx,
            null,
            null,
            null,
            new NoOpPageStoreManager(),
            new NoOpWALManager(),
            null,
            new IgniteCacheDatabaseSharedManager(),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );
    }

    /**
     * @throws Exception If cleanup of persistent directories has failed.
     */
    @After public void afterTest() throws Exception {
        GridTestUtils.cleanPersistenceDir();
    }

    /**
     * Verifies cleanup of whole work directory (files and directories are cleaned up for all caches excluding metastorage).
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCleanupPersistenceSpace() throws Exception {
        storeBasePath = U.resolveWorkDirectory(U.defaultWorkDirectory(),
            "db",
            false);

        FilePageStoreManager fileMgr = new FilePageStoreManager(cfg);

        fileMgr.start(sharedCtx);

        Path dataFilesPath = storeBasePath.toPath().resolve(consId);

        createMixedDirectories(dataFilesPath);

        assertEquals(4, dataFilesPath.toFile().listFiles().length);

        fileMgr.cleanupPersistentSpace();

        File[] survivedDirs = dataFilesPath.toFile().listFiles();
        assertEquals(2, survivedDirs.length);

        boolean metastorageDirSurvived = false;
        boolean randomDirSurvived = false;

        for (File dir : survivedDirs) {
            if (dir.getName().equals(META_STORAGE_NAME))
                metastorageDirSurvived = true;
            else if (dir.getName().equals(RANDOM_DIR_NAME))
                randomDirSurvived = true;
        }

        assertTrue(metastorageDirSurvived);
        assertTrue(randomDirSurvived);
    }

    /**
     * Verifies cleanup of a directory of a single cache (not part of any cache group).
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCleanupPersistenceSpaceForCache() throws Exception {
        execTestForCacheCleanup(new CacheConfiguration("singleCache"));
    }

    /**
     * Verifies cleanup of a directory of a cache inside a cache group.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCleanupPersistenceSpaceForCacheGroup() throws Exception {
        execTestForCacheCleanup(new CacheConfiguration("cacheInGroup").setGroupName("cacheGroup"));
    }

    /**
     * Executes test of cleaning up files from cache directory.
     *
     * @param cacheCfg Configuration of cache to pass to {@link FilePageStoreManager#cleanupPersistentSpace(CacheConfiguration)} method.
     * @throws Exception If failed.
     */
    private void execTestForCacheCleanup(CacheConfiguration cacheCfg) throws Exception {
        storeBasePath = U.resolveWorkDirectory(U.defaultWorkDirectory(),
            "db",
            false);

        FilePageStoreManager fileMgr = new FilePageStoreManager(cfg);

        fileMgr.start(sharedCtx);

        Path dataFilesPath = storeBasePath.toPath().resolve(consId);

        Path cacheWorkDir = createAndFillDirectoryForCache(dataFilesPath, cacheCfg);

        File[] cacheDirFiles = cacheWorkDir.toFile().listFiles();

        assertEquals(3, cacheDirFiles.length);

        fileMgr.cleanupPersistentSpace(cacheCfg);

        cacheDirFiles = cacheWorkDir.toFile().listFiles();

        assertEquals(1, cacheDirFiles.length);

        assertTrue(cacheDirFiles[0].getName().endsWith(RANDOM_FILE_SUFFIX));
    }

    /**
     * Creates mixed set of directories: one matches cache dir pattern, one matches cache group directory pattern,
     * one matches metastorage directory pattern and one doesn't match anything.
     *
     * @param basePath Root path for all directories.
     * @throws IgniteCheckedException If directory creation failed (for any reason).
     */
    private void createMixedDirectories(Path basePath) throws IgniteCheckedException {
        String cacheDirName = CACHE_DIR_PREFIX + "101";
        String cacheGroupDirName = CACHE_GRP_DIR_PREFIX + "101";

        U.ensureDirectory(basePath.resolve(cacheDirName), null, null);
        U.ensureDirectory(basePath.resolve(cacheGroupDirName), null, null);
        U.ensureDirectory(basePath.resolve(META_STORAGE_NAME), null, null);
        U.ensureDirectory(basePath.resolve(RANDOM_DIR_NAME), null, null);
    }

    /**
     * Creates mixed set of files inside cache directory: two files matching cache data file pattern and one
     * file not matching it.
     *
     * @param basePath Root path for all cache files.
     * @param cacheCfg Configuration of cache: method needs cache or group name to derive directory name for cache files.
     * @return Path of directory where cache and non-cache files are created.
     */
    private Path createAndFillDirectoryForCache(Path basePath, CacheConfiguration cacheCfg) throws Exception {
        String cacheDirName;
        String igniteCacheFileName0 = "part0" + FILE_SUFFIX;
        String igniteCacheFileName1 = "part1" + FILE_SUFFIX;
        String randomFileName = "randFile" + RANDOM_FILE_SUFFIX;

        if (cacheCfg.getGroupName() != null)
            cacheDirName = CACHE_GRP_DIR_PREFIX + cacheCfg.getGroupName();
        else
            cacheDirName = CACHE_DIR_PREFIX + cacheCfg.getName();

        Path cacheWorkingDir = basePath.resolve(cacheDirName);

        U.ensureDirectory(cacheWorkingDir, null, null);

        Files.createFile(cacheWorkingDir.resolve(igniteCacheFileName0));
        Files.createFile(cacheWorkingDir.resolve(igniteCacheFileName1));
        Files.createFile(cacheWorkingDir.resolve(randomFileName));

        return cacheWorkingDir;
    }
}
