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
import java.nio.file.Path;
import java.util.Collections;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
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
import org.apache.ignite.testframework.junits.GridTestKernalContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CACHE_DIR_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CACHE_GRP_DIR_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.META_STORAGE_NAME;

/**
 * Unit test for {@link FilePageStoreManager} class.
 */
public class FilePageStoreManagerTest extends GridCommonAbstractTest {
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

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
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

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        cleanPersistenceDir();
    }

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

        assertEquals(2, dataFilesPath.toFile().listFiles().length);
    }

    private void createMixedDirectories(Path basePath) throws IgniteCheckedException {
        String cacheDirName = CACHE_DIR_PREFIX + "101";
        String cacheGroupDirName = CACHE_GRP_DIR_PREFIX + "101";
        String randomDirName = "randomDir";

        U.ensureDirectory(basePath.resolve(cacheDirName), null, null);
        U.ensureDirectory(basePath.resolve(cacheGroupDirName), null, null);
        U.ensureDirectory(basePath.resolve(META_STORAGE_NAME), null, null);
        U.ensureDirectory(basePath.resolve(randomDirName), null, null);
    }
}
