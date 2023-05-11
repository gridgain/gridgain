/*
 * Copyright 2023 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.cache.persistence.maintenance;

import java.io.File;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.maintenance.MaintenanceAction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The action clears storage folders with all data files.
 */
public class CleanFolderAction implements MaintenanceAction<Void> {
    /** */
    public static final String ACTION_NAME = "clean_forder";

    /** */
    private final File rootStoreDir;

    /** */
    private final String[] cacheStoreFolders;

    /** Grid context. */
    private final GridKernalContext ctx;

    /**
     * The constructor.
     *
     * @param ctx Kernal contest.
     * @param rootStoreDir Root storage directory.
     * @param cacheStoreFolders Cache folder names.
     */
    public CleanFolderAction(GridKernalContext ctx, File rootStoreDir, String[] cacheStoreFolders) {
        this.ctx = ctx;
        this.rootStoreDir = rootStoreDir;
        this.cacheStoreFolders = cacheStoreFolders;
    }

    /** {@inheritDoc} */
    @Override public Void execute() {
        for (String cacheStoreFolderName : cacheStoreFolders) {
            File cacheStoreDir = new File(rootStoreDir, cacheStoreFolderName);

            if (cacheStoreDir.exists() && cacheStoreDir.isDirectory()) {
                for (File file : cacheStoreDir.listFiles()) {
                    file.delete();
                }

                cacheStoreDir.delete();
            }
        }

        ctx.maintenanceRegistry().unregisterMaintenanceTask(ClearFolderWorkflow.CLEAR_FOLDER_TASK);

        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull String name() {
        return ACTION_NAME;
    }

    /** {@inheritDoc} */
    @Override public @Nullable String description() {
        return "Clear cache folders with all cache storage files";
    }
}
