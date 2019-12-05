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

import java.util.LinkedList;
import java.util.List;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.GridCacheContextInfo;
import org.apache.ignite.internal.processors.cache.StoredCacheData;
import org.apache.ignite.internal.processors.query.QueryTypeDescriptorImpl;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexOperationCancellationToken;
import org.apache.ignite.internal.processors.query.schema.SchemaOperationException;
import org.apache.ignite.internal.processors.query.schema.operation.SchemaIndexDropOperation;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;

public class PendingDropIndexTask extends AbstractSchemaChangePendingTask {
    public PendingDropIndexTask() {
        /* No op. */
    }

    public PendingDropIndexTask(SchemaIndexDropOperation operation, StoredCacheData originalCacheData) {
        super(operation, originalCacheData);

        filteredCacheData = new StoredCacheData(originalCacheData);

        SchemaIndexDropOperation op0 = (SchemaIndexDropOperation)schemaOperation;

        for (QueryEntity queryEntity : filteredCacheData.queryEntities()) {
            List<QueryIndex> idxs = new LinkedList<>();

            for (QueryIndex idx : queryEntity.getIndexes()) {
                if (!idx.getName().equals(op0.indexName()))
                    idxs.add(idx);
            }

            queryEntity.setIndexes(idxs);
        }
    }

    /** {@inheritDoc} */
    @Override public String shortName() {
        SchemaIndexDropOperation op0 = (SchemaIndexDropOperation)schemaOperation;

        return "DROP_SQL_INDEX-" + op0.schemaName() + "." + op0.indexName();
    }

    /** {@inheritDoc} */
    @Override public void execute(GridKernalContext ctx) {
        String cacheName = filteredCacheData.config().getName();

        GridCacheContextInfo cacheInfo = ctx.query().getIndexing().registeredCacheInfo(cacheName);

        if (cacheInfo == null)
            throw new IgniteException("Could not get cache info for cache: " + cacheName);

        IgniteUuid depId = cacheInfo.dynamicDeploymentId();

        QueryTypeDescriptorImpl type =
            new QueryTypeDescriptorImpl(cacheName, null);

        SchemaIndexDropOperation op0 = (SchemaIndexDropOperation)schemaOperation;

        try {
            ctx.query().processSchemaOperationLocal(op0, type, depId, new SchemaIndexOperationCancellationToken());
        }
        catch (SchemaOperationException e) {
            throw new IgniteException(e);
        }

        ctx.query().onLocalOperationFinished(op0, type);

        ctx.cache().removePendingNodeTask(this);
    }

    /** {@inheritDoc} */
    public String toString() {
        return S.toString(PendingDropIndexTask.class, this, "super", super.toString());
    }
}
