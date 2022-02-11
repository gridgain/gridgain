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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointManager;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.query.aware.IndexBuildStatusStorage;
import org.apache.ignite.internal.processors.query.h2.H2TableDescriptor;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.SchemaManager;
import org.apache.ignite.internal.processors.query.h2.database.H2TreeIndex;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitorClosure;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitorImpl;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.maintenance.MaintenanceAction;
import org.gridgain.internal.h2.engine.Session;
import org.gridgain.internal.h2.index.Index;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.persistence.CheckpointState.FINISHED;

/**
 * Maintenance action that handles index rebuilding.
 */
public class RebuildIndexAction implements MaintenanceAction<Boolean> {
    /** Id of the cache that contains target index. */
    private final int cacheId;

    /** Target index's name. */
    private final String idxName;

    /** Ignite indexing. */
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
    public RebuildIndexAction(int cacheId, String idxName, IgniteH2Indexing indexing, IgniteLogger log) {
        this.cacheId = cacheId;
        this.idxName = idxName;
        this.indexing = indexing;
        this.log = log;
    }

    /** {@inheritDoc} */
    @Override public Boolean execute() {
        try {
            execute0();
        }
        catch (Exception e) {
            log.error("Rebuilding index " + idxName + " for cache " + cacheId + " failed", e);

            return false;
        }
        return true;
    }

    /**
     * Executes index rebuild.
     *
     * @throws Exception If failed to execute rebuild.
     */
    private void execute0() throws Exception {
        GridKernalContext kernalContext = indexing.kernalContext();

        GridCacheDatabaseSharedManager database = (GridCacheDatabaseSharedManager) kernalContext.cache()
            .context().database();

        CheckpointManager manager = database.getCheckpointManager();

        IndexBuildStatusStorage storage = getIndexBuildStatusStorage(kernalContext);

        prepareForRebuild(database, manager, storage);

        GridCacheContext<?, ?> context = kernalContext.cache().context().cacheContext(cacheId);

        String cacheName = context.name();

        SchemaManager schemaManager = indexing.schemaManager();

        H2TreeIndex targetIndex = findIndex(cacheName, schemaManager);

        if (targetIndex == null) {
            // Our job here is already done.
            unregisterMaintenanceTask(kernalContext);
            return;
        }

        GridH2Table targetTable = targetIndex.getTable();

        destroyOldIndex(targetIndex, targetTable);

        try {
            recreateIndex(targetIndex, context, cacheName, storage, schemaManager, targetTable);

            manager.forceCheckpoint("afterIndexRebuild", null).futureFor(FINISHED).get();

            unregisterMaintenanceTask(kernalContext);
        }
        finally {
            cleanUpAfterRebuild(manager, storage);
        }
    }

    /**
     * Creates new index from the old one and builds it.
     *
     * @param oldIndex Old index.
     * @param context Cache context.
     * @param cacheName Cache name.
     * @param storage Index build status storage.
     * @param schemaManager Schema manager.
     * @param targetTable Table for the index.
     * @throws IgniteCheckedException If failed to recreate an index.
     */
    private void recreateIndex(
        H2TreeIndex oldIndex,
        GridCacheContext<?, ?> context,
        String cacheName,
        IndexBuildStatusStorage storage,
        SchemaManager schemaManager,
        GridH2Table targetTable
    ) throws IgniteCheckedException {
        GridFutureAdapter<Void> createIdxFut = new GridFutureAdapter<>();

        // Create new index from the old one.
        H2TreeIndex newIndex = oldIndex.getRecreator().apply(context.dataRegion().pageMemory(), context.offheap());

        SchemaIndexCacheVisitorImpl visitor = new SchemaIndexCacheVisitorImpl(context, null, createIdxFut) {
            /** {@inheritDoc} */
            @Override public void visit(SchemaIndexCacheVisitorClosure clo) {
                // Rebuild index after it is created.
                storage.onStartRebuildIndexes(context);

                try {
                    super.visit(clo);

                    buildIdxFut.get();
                }
                catch (Exception e) {
                    throw new IgniteException(e);
                }
                finally {
                    storage.onFinishRebuildIndexes(cacheName);
                }
            }
        };

        schemaManager.createIndex(
            targetTable,
            targetTable.rowDescriptor().tableDescriptor(),
            targetTable.getSchema().getName(),
            newIndex,
            true,
            visitor
        );

        // This future must be already finished by the schema index cache visitor above
        assert createIdxFut.isDone();
    }

    /**
     * Destroys old index.
     *
     * @param index Index.
     * @param table Table.
     * @throws IgniteCheckedException If failed to destroy index.
     */
    private void destroyOldIndex(H2TreeIndex index, GridH2Table table) throws IgniteCheckedException {
        index.destroy0(true, true);

        Session session = table.getDatabase().getSystemSession();

        table.removeIndex(session, index);
    }

    /**
     * Finds index for the cache by the name.
     *
     * @param cacheName Cache name.
     * @param schemaManager Schema manager.
     * @return Index or {@code null} if index was not found.
     */
    @Nullable
    private H2TreeIndex findIndex(String cacheName, SchemaManager schemaManager) {
        H2TreeIndex targetIndex = null;

        for (H2TableDescriptor tblDesc : schemaManager.tablesForCache(cacheName)) {
            GridH2Table tbl = tblDesc.table();

            assert tbl != null;

            Index index = tbl.getIndex(idxName);

            if (index != null) {
                assert index instanceof H2TreeIndex;

                targetIndex = (H2TreeIndex) index;
                break;
            }
        }

        return targetIndex;
    }

    /**
     * Prepares system for the rebuild.
     *
     * @param database Database manager.
     * @param manager Checkpoint manager.
     * @param storage Index build status storage.
     * @throws IgniteCheckedException
     */
    private void prepareForRebuild(
        GridCacheDatabaseSharedManager database,
        CheckpointManager manager,
        IndexBuildStatusStorage storage
    ) throws IgniteCheckedException {
        // Enable WAL
        database.resumeWalLogging();

        // Enable checkpointer
        database.onStateRestored(null);

        // Wait for checkpoint
        manager.forceCheckpoint("beforeIndexRebuild", null).futureFor(FINISHED).get();

        // Flush WAL
        database.preserveWalTailPointer();

        // IndexBuildStatusStorage listens for checkpoint to update the status of the rebuild in the metastorage.
        // We need to set up the listener manually here, because it's maintenance mode.
        manager.addCheckpointListener(storage, null);
    }

    /**
     * Cleans up after the rebuild.
     *
     * @param manager Checkpoint manager.
     * @param storage Index build status storage.
     */
    private void cleanUpAfterRebuild(CheckpointManager manager, IndexBuildStatusStorage storage) {
        // Remove the checkpoint listener
        manager.removeCheckpointListener(storage);
    }

    /**
     * Removes maintenance task.
     *
     * @param kernalContext Kernal context.
     */
    private void unregisterMaintenanceTask(GridKernalContext kernalContext) {
        kernalContext.maintenanceRegistry().unregisterMaintenanceTask(IgniteH2Indexing.INDEX_REBUILD_MNTC_TASK_NAME);
    }

    /**
     * Gets index build status storage.
     *
     * @param kernalContext Kernal context.
     * @return Index build status storage.
     */
    private IndexBuildStatusStorage getIndexBuildStatusStorage(GridKernalContext kernalContext) {
        GridQueryProcessor query = kernalContext.query();

        return query.getIdxBuildStatusStorage();
    }

    /** {@inheritDoc} */
    @Override public @NotNull String name() {
        return "rebuild";
    }

    /** {@inheritDoc} */
    @Override public @Nullable String description() {
        return "Rebuilding indexes";
    }
}
