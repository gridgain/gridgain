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
import org.apache.ignite.maintenance.MaintenanceAction;
import org.apache.ignite.maintenance.MaintenanceRegistry;
import org.apache.ignite.maintenance.MaintenanceWorkflowCallback;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The workflow is used for {@link ClearFolderAction} in maintenance mode.
 */
public class ClearFolderWorkflow implements MaintenanceWorkflowCallback {
    /** Task name. */
    public static final String CLEAR_FOLDER_TASK = "cleanForderTask";

    /** Work directory. */
    private final File workDir;

    /** Folders to clear. */
    private final List<String> folders;

    /** Maintenance registry. */
    private final MaintenanceRegistry maintenanceRegistry;

    /**
     * The constructor.
     *
     * @param ctx Maintenance registry.
     * @param workDir Root storage directory.
     * @param foldersToClear Cache folder names.
     */
    public ClearFolderWorkflow(MaintenanceRegistry maintenanceRegistry, File workDir, List<String> foldersToClear) {
        this.maintenanceRegistry = maintenanceRegistry;
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
            new ClearFolderAction(maintenanceRegistry, workDir, folders.toArray(new String[0]))
        );
    }

    /** {@inheritDoc} */
    @Override public @Nullable MaintenanceAction<?> automaticAction() {
        return new ClearFolderAction(maintenanceRegistry, workDir, folders.toArray(new String[0]));
    }
}
