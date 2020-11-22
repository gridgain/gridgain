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

package org.apache.ignite.internal.processors.cache.persistence.defragmentation.maintenance;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.cache.persistence.defragmentation.CachePartitionDefragmentationManager;
import org.apache.ignite.internal.processors.failure.FailureProcessor;
import org.apache.ignite.maintenance.MaintenanceAction;
import org.apache.ignite.maintenance.MaintenanceWorkflowCallback;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Defragmentation specific callback for maintenance mode.
 */
public class DefragmentationWorkflowCallback implements MaintenanceWorkflowCallback {
    /** Defragmentation manager. */
    private final CachePartitionDefragmentationManager defrgMgr;

    /** Logger provider. */
    private final Function<Class<?>, IgniteLogger> logProvider;

    /** Failure processor. */
    private final FailureProcessor failureProc;

    /**
     * @param logProvider Logger provider.
     * @param defrgMgr Defragmentation manager.
     * @param failureProc Failure processor.
     */
    public DefragmentationWorkflowCallback(
        Function<Class<?>, IgniteLogger> logProvider,
        CachePartitionDefragmentationManager defrgMgr,
        FailureProcessor failureProc
    ) {
        this.defrgMgr = defrgMgr;
        this.logProvider = logProvider;
        this.failureProc = failureProc;
    }

    /** {@inheritDoc} */
    @Override public boolean shouldProceedWithMaintenance() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public @NotNull List<MaintenanceAction<?>> allActions() {
        return Collections.singletonList(new StopDefragmentationAction(defrgMgr));
    }

    /** {@inheritDoc} */
    @Override public @Nullable MaintenanceAction<Boolean> automaticAction() {
        return new ExecuteDefragmentationAction(logProvider, defrgMgr, failureProc);
    }
}
