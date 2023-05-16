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
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.maintenance.MaintenanceAction;
import org.apache.ignite.maintenance.MaintenanceWorkflowCallback;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The workflow is used for {@link ClearFolderAction} in maintenance mode.
 */
public class ClearFolderWorkflow implements MaintenanceWorkflowCallback {
    /** Task name. */
    public static final String CLEAR_FOLDER_TASK = "clean_forder_task";

    /** Work directory. */
    private final File workDir;

    /** Folders to clear. */
    private final List<String> folders;

    /** Grid context. */
    private final GridKernalContext ctx;

    /**
     * The constructor.
     *
     * @param ctx Kernal contest.
     * @param workDir Root storage directory.
     * @param foldersToClear Cache folder names.
     */
    public ClearFolderWorkflow(GridKernalContext ctx, File workDir, List<String> foldersToClear) {
        this.ctx = ctx;
        this.workDir = workDir;
        this.folders = foldersToClear;
    }

    /** {@inheritDoc} */
    @Override public boolean shouldProceedWithMaintenance() {
        for (String cacheStoreDirName : folders) {
            File cacheStoreDir = new File(workDir, cacheStoreDirName);

            if (cacheStoreDir.exists() && cacheStoreDir.isDirectory()) {
                return true;
            }
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public @NotNull List<MaintenanceAction<?>> allActions() {
        return Arrays.asList(
            new ClearFolderAction(ctx, workDir, folders.toArray(new String[0]))
        );
    }

    /** {@inheritDoc} */
    @Override public @Nullable MaintenanceAction<?> automaticAction() {
        return new ClearFolderAction(ctx, workDir, folders.toArray(new String[0]));
    }
}
