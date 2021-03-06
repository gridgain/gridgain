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

package org.apache.ignite.internal.processors.cache.persistence;

import java.io.File;

import org.apache.ignite.maintenance.MaintenanceAction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CACHE_DATA_FILENAME;

/**
 *
 */
public class CleanCacheStoresMaintenanceAction implements MaintenanceAction<Void> {
    /** */
    public static final String ACTION_NAME = "clean_data_files";

    /** */
    private final File rootStoreDir;

    /** */
    private final String[] cacheStoreDirs;

    /**
     * @param rootStoreDir
     * @param cacheStoreDirs
     */
    public CleanCacheStoresMaintenanceAction(File rootStoreDir, String[] cacheStoreDirs) {
        this.rootStoreDir = rootStoreDir;
        this.cacheStoreDirs = cacheStoreDirs;
    }

    /** {@inheritDoc} */
    @Override public Void execute() {
        for (String cacheStoreDirName : cacheStoreDirs) {
            File cacheStoreDir = new File(rootStoreDir, cacheStoreDirName);

            if (cacheStoreDir.exists() && cacheStoreDir.isDirectory()) {
                for (File file : cacheStoreDir.listFiles()) {
                    if (!file.getName().equals(CACHE_DATA_FILENAME))
                        file.delete();
                }
            }
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull String name() {
        return ACTION_NAME;
    }

    /** {@inheritDoc} */
    @Override public @Nullable String description() {
        return "Cleans data files of cache groups";
    }
}
