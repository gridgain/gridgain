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
import org.apache.ignite.internal.processors.cache.StoredCacheData;
import org.apache.ignite.internal.processors.query.schema.operation.SchemaAbstractOperation;
import org.apache.ignite.internal.processors.query.schema.operation.SchemaIndexDropOperation;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

public class NodePendingTaskFactory {
    private final GridKernalContext ctx;

    /** */
    public NodePendingTaskFactory(GridKernalContext ctx) {
        this.ctx = ctx;
    }

    public @Nullable AbstractNodePendingTask buildTaskIfNeeded(SchemaAbstractOperation operation, IgniteUuid deploymentId) {
        if (operation instanceof SchemaIndexDropOperation) {
            SchemaIndexDropOperation op0 = (SchemaIndexDropOperation)operation;

            DynamicCacheDescriptor cacheDesc = ctx.cache().cacheDescriptor(op0.cacheName());

            if (cacheDesc != null && F.eq(cacheDesc.deploymentId(), deploymentId)) {
                StoredCacheData cacheData = cacheDesc.toStoredData(ctx.cache().splitter());

                AbstractNodePendingTask task = new PendingDropIndexTask(op0, cacheData);

                return task;
            }
        }

        return null;
    }
}
