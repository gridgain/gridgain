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
package org.apache.ignite.internal.processors.cache.persistence.metastorage.pendingtask;

import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.query.schema.operation.SchemaAbstractOperation;
import org.apache.ignite.internal.processors.query.schema.operation.SchemaIndexDropOperation;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.util.IgniteUtils.unquote;

/**
 * Factory for creating pending tasks (see {@link AbstractNodePendingTask}).
 */
public class NodePendingTaskFactory {
    /** Kernal context. */
    private final GridKernalContext ctx;

    /** */
    public NodePendingTaskFactory(GridKernalContext ctx) {
        this.ctx = ctx;
    }

    /**
     * Builds tasks that change SQL schema. Can return {@code null} if pending task creation is not needed.
     * @param operation Schema operation.
     * @param deploymentId Cache deployment id.
     * @return Pending task object, or {@code null} if pending task creation is not needed.
     */
    public @Nullable AbstractNodePendingTask buildTaskIfNeeded(SchemaAbstractOperation operation, IgniteUuid deploymentId) {
        DynamicCacheDescriptor cacheDesc = ctx.cache().cacheDescriptor(operation.cacheName());

        if (cacheDesc == null
            || !F.eq(cacheDesc.deploymentId(), deploymentId)
            || !unquote(operation.schemaName().toUpperCase())
                    .equals(unquote(cacheDesc.cacheConfiguration().getSqlSchema().toUpperCase())))
            return null;

        if (operation instanceof SchemaIndexDropOperation) {
            return new PendingDropIndexTask(
                (SchemaIndexDropOperation)operation,
                cacheDesc.toStoredData(ctx.cache().splitter())
            );
        }

        return null;
    }
}
