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

package org.apache.ignite.internal.processors.query.h2.maintenance;

import java.util.Collections;
import java.util.List;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.maintenance.MaintenanceAction;
import org.apache.ignite.maintenance.MaintenanceWorkflowCallback;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Workflow for the index rebuild maintenance task.
 */
public class RebuildIndexWorkflowCallback implements MaintenanceWorkflowCallback {
    /** Id of the cache that holds the index. */
    private final int cacheId;

    /** Target index's name. */
    private final String idxName;

    /** Indexing. */
    private final IgniteH2Indexing indexing;

    /** Logger. */
    private final IgniteLogger log;

    /**
     * Constructor.
     *
     * @param cacheId Id of the cache that contains target index.
     * @param idxName Target index's name.
     * @param indexing Indexing.
     * @param log Logger.
     */
    public RebuildIndexWorkflowCallback(int cacheId, String idxName, IgniteH2Indexing indexing, IgniteLogger log) {
        this.cacheId = cacheId;
        this.idxName = idxName;
        this.indexing = indexing;
        this.log = log;
    }

    /** {@inheritDoc} */
    @Override public boolean shouldProceedWithMaintenance() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public @NotNull List<MaintenanceAction<?>> allActions() {
        return Collections.singletonList(new RebuildIndexAction(cacheId, idxName, indexing, log));
    }

    /** {@inheritDoc} */
    @Override public @Nullable MaintenanceAction<?> automaticAction() {
        return new RebuildIndexAction(cacheId, idxName, indexing, log);
    }
}
