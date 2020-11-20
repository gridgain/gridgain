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

import java.util.function.Function;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.cache.persistence.defragmentation.CachePartitionDefragmentationManager;
import org.apache.ignite.maintenance.MaintenanceAction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Action which allows to start the defragmentation process.
 */
class ExecuteDefragmentationAction implements MaintenanceAction<Boolean> {
    /** Logger. */
    private final IgniteLogger log;

    /** Defragmentation manager. */
    private final CachePartitionDefragmentationManager defrgMgr;

    /**
     * @param logFunction Logger provider.
     * @param defrgMgr Defragmentation manager.
     */
    public ExecuteDefragmentationAction(
        Function<Class<?>, IgniteLogger> logFunction,
        CachePartitionDefragmentationManager defrgMgr
    ) {
        this.log = logFunction.apply(ExecuteDefragmentationAction.class);
        this.defrgMgr = defrgMgr;
    }

    /** {@inheritDoc} */
    @Override public Boolean execute() {
        try {
            defrgMgr.executeDefragmentation();
        }
        catch (IgniteCheckedException e) {
            log.error("Defragmentation is failed", e);

            return false;
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public @NotNull String name() {
        return "execute";
    }

    /** {@inheritDoc} */
    @Override public @Nullable String description() {
        return "Starting the process of defragmentation.";
    }
}
