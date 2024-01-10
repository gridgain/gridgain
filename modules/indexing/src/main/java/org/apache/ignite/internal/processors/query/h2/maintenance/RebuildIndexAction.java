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

package org.apache.ignite.internal.processors.query.h2.maintenance;

import java.util.List;
import java.util.StringJoiner;
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
import org.jetbrains.annotations.Nullable;

import static java.util.stream.Collectors.joining;
import static org.apache.ignite.internal.processors.cache.persistence.CheckpointState.FINISHED;

/**
 * Maintenance action that handles index rebuilding.
 * TODO: GG-34742 Rebuild multiple indexes of a cache at once.
 */
public class RebuildIndexAction implements MaintenanceAction<Boolean> {
    /** Indexes to rebuild. */
    private final List<MaintenanceRebuildIndexTarget> indexesToRebuild;

    /** Ignite indexing. */
    private final IgniteH2Indexing indexing;

    /** Logger. */
    private final IgniteLogger log;

    /**
     * Constructor.
     *
     * @param indexesToRebuild Indexes to rebuild.
     * @param indexing Indexing.
     * @param log Logger.
     */
    public RebuildIndexAction(List<MaintenanceRebuildIndexTarget> indexesToRebuild, IgniteH2Indexing indexing, IgniteLogger log) {
        this.indexesToRebuild = indexesToRebuild;
        this.indexing = indexing;
        this.log = log;
    }

    /** {@inheritDoc} */
    @Override public Boolean execute() {
        GridKernalContext kernalContext = indexing.kernalContext();

        GridCacheDatabaseSharedManager database = (GridCacheDatabaseSharedManager) kernalContext.cache()
            .context().database();

        CheckpointManager manager = database.getCheckpointManager();

        IndexBuildStatusStorage storage = getIndexBuildStatusStorage(kernalContext);

        try {
            prepareForRebuild(database, manager, storage);

            for (MaintenanceRebuildIndexTarget params : indexesToRebuild) {
                int cacheId = params.cacheId();
                String idxName = params.idxName();

                try {
                    execute0(cacheId, idxName, kernalContext, storage, manager);
                }
                catch (Exception e) {
                    log.error("Rebuilding index " + idxName + " for cache " + cacheId + " failed", e);

                    return false;
                }
            }
        }
        catch (IgniteCheckedException e) {
            log.error("Failed to prepare for the rebuild of indexes", e);
            return false;
        }
        finally {
            cleanUpAfterRebuild(manager, storage);
        }

        unregisterMaintenanceTask(kernalContext);

        return true;
    }

    /**
     * Executes index rebuild.
     *
     * @param cacheId Cache id.
     * @param idxName Name of the index.
     * @param kernalContext Context.
     * @param storage Index build status storage.
     * @param manager Checkpoint manager.
     * @throws Exception If failed to execute rebuild.
     */
    private void execute0(
        int cacheId,
        String idxName,
        GridKernalContext kernalContext,
        IndexBuildStatusStorage storage,
        CheckpointManager manager
    ) throws Exception {
        GridCacheContext<?, ?> context = kernalContext.cache().context().cacheContext(cacheId);

        String cacheName = context.name();

        SchemaManager schemaManager = indexing.schemaManager();

        H2TreeIndex targetIndex = findIndex(cacheName, idxName, schemaManager);

        if (targetIndex == null) {
            if (log.isInfoEnabled()) {
                log.info("Could not find index: [indexName=" + idxName + ", "
                    + createCacheTablesInfo(cacheName, schemaManager) + ']');
            }

            return;
        }

        GridH2Table targetTable = targetIndex.getTable();

        destroyOldIndex(targetIndex, targetTable);

        recreateIndex(targetIndex, context, cacheName, storage, schemaManager, targetTable);

        manager.forceCheckpoint("afterIndexRebuild", null).futureFor(FINISHED).get();
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
        H2TreeIndex newIndex = oldIndex.createCopy(context.dataRegion().pageMemory(), context.offheap());

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
     * @param idxName Index name.
     * @param schemaMgr Schema manager.
     * @return Index or {@code null} if index was not found.
     */
    public static @Nullable H2TreeIndex findIndex(String cacheName, String idxName, SchemaManager schemaMgr) {
        H2TreeIndex targetIndex = null;

        for (H2TableDescriptor tblDesc : schemaMgr.tablesForCache(cacheName)) {
            GridH2Table tbl = tblDesc.table();

            assert tbl != null;

            Index index = tbl.getIndexSafe(idxName);

            if (index != null) {
                assert index instanceof H2TreeIndex;

                targetIndex = (H2TreeIndex)index;

                break;
            }
        }

        return targetIndex;
    }

    /**
     * Creates information about the cache tables.
     *
     * @param cacheName Cache name.
     * @param schemaMgr Schema manager.
     * @return String in format: cacheName=foo, tables=[[name=user, rebuild=false, indexes=[IDX_1, IDX_2]].
     */
    public static String createCacheTablesInfo(String cacheName, SchemaManager schemaMgr) {
        StringJoiner joiner = new StringJoiner("], [", "[", "]");

        for (H2TableDescriptor tableDescriptor : schemaMgr.tablesForCache(cacheName)) {
            GridH2Table tbl = tableDescriptor.table();

            String idx = tbl.getIndexes().stream()
                .filter(H2TreeIndex.class::isInstance)
                .map(H2TreeIndex.class::cast)
                .map(H2TreeIndex::getIndexName)
                .collect(joining(", ", "[", "]"));

            joiner.add("name=" + tbl.getName() + ", rebuild=" + tbl.rebuildFromHashInProgress() + ", indexes=" + idx);
        }

        return "cacheName=" + cacheName + ", tables=[" + joiner + ']';
    }

    /**
     * Prepares system for the rebuild.
     *
     * @param database Database manager.
     * @param manager Checkpoint manager.
     * @param storage Index build status storage.
     * @throws IgniteCheckedException If failed.
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
    @Override public String name() {
        return "rebuild";
    }

    /** {@inheritDoc} */
    @Nullable @Override public String description() {
        return "Rebuilding indexes";
    }
}
