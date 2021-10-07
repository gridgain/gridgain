/*
 * Copyright 2021 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.query.h2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.metric.IoStatisticsHolder;
import org.apache.ignite.internal.metric.IoStatisticsHolderIndex;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.persistence.RootPage;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageLockTrackerManager;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.pendingtask.DurableBackgroundTask;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.pendingtask.DurableBackgroundTaskResult;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIoResolver;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.failure.FailureProcessor;
import org.apache.ignite.internal.processors.query.h2.database.H2Tree;
import org.apache.ignite.internal.processors.query.h2.database.inlinecolumn.InlineIndexColumnFactory;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.thread.IgniteThread;
import org.gridgain.internal.h2.table.IndexColumn;
import org.jetbrains.annotations.Nullable;

import static java.util.Collections.singleton;
import static org.apache.ignite.internal.metric.IoStatisticsType.SORTED_INDEX;

/**
 * Tasks that cleans up index tree.
 *
 * @deprecated Use {@link DurableBackgroundCleanupIndexTreeTaskV2}.
 */
public class DurableBackgroundCleanupIndexTreeTask implements DurableBackgroundTask {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private List<Long> rootPages;

    /** */
    private transient volatile List<H2Tree> trees;

    /** */
    private String cacheGrpName;

    /** */
    private String cacheName;

    /** */
    private String schemaName;

    /** */
    private final String treeName;

    /** */
    private String idxName;

    /** */
    private String id;

    /** Logger. */
    @Nullable private transient volatile IgniteLogger log;

    /** Worker tasks. */
    @Nullable private transient volatile GridWorker worker;

    /** */
    public DurableBackgroundCleanupIndexTreeTask(
        List<Long> rootPages,
        List<H2Tree> trees,
        String cacheGrpName,
        String cacheName,
        String schemaName,
        String treeName,
        String idxName
    ) {
        this.rootPages = rootPages;
        this.trees = trees;
        this.cacheGrpName = cacheGrpName;
        this.cacheName = cacheName;
        this.schemaName = schemaName;
        this.treeName = treeName;
        this.idxName = idxName;
        this.id = UUID.randomUUID().toString();
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return "DROP_SQL_INDEX-" + schemaName + "." + idxName + "-" + id;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<DurableBackgroundTaskResult> executeAsync(GridKernalContext ctx) {
        log = ctx.log(this.getClass());

        assert worker == null;

        GridFutureAdapter<DurableBackgroundTaskResult> fut = new GridFutureAdapter<>();

        worker = new GridWorker(
            ctx.igniteInstanceName(),
            "async-durable-background-task-executor-" + name(),
            log
        ) {
            /** {@inheritDoc} */
            @Override protected void body() {
                try {
                    execute(ctx);

                    worker = null;

                    fut.onDone(DurableBackgroundTaskResult.complete(null));
                }
                catch (Throwable t) {
                    worker = null;

                    fut.onDone(DurableBackgroundTaskResult.restart(t));
                }
            }
        };

        new IgniteThread(worker).start();

        return fut;
    }

    /**
     * Task execution.
     *
     * @param ctx Kernal context.
     */
    private void execute(GridKernalContext ctx) {
        List<H2Tree> trees0 = trees;

        if (trees0 == null) {
            trees0 = new ArrayList<>(rootPages.size());

            int grpId = CU.cacheGroupId(cacheName, cacheGrpName);

            CacheGroupContext grpCtx = ctx.cache().cacheGroup(grpId);

            // If group context is null, it means that group doesn't exist and we don't need this task anymore.
            if (grpCtx == null)
                return;

            IgniteCacheOffheapManager offheap = grpCtx.offheap();

            if (treeName != null) {
                ctx.cache().context().database().checkpointReadLock();

                try {
                    int cacheId = CU.cacheId(cacheName);

                    for (int segment = 0; segment < rootPages.size(); segment++) {
                        try {
                            RootPage rootPage = offheap.findRootPageForIndex(cacheId, treeName, segment);

                            if (rootPage != null && rootPages.get(segment) == rootPage.pageId().pageId())
                                offheap.dropRootPageForIndex(cacheId, treeName, segment);
                        }
                        catch (IgniteCheckedException e) {
                            throw new IgniteException(e);
                        }
                    }
                }
                finally {
                    ctx.cache().context().database().checkpointReadUnlock();
                }
            }

            IoStatisticsHolderIndex stats = new IoStatisticsHolderIndex(
                SORTED_INDEX,
                cacheGrpName,
                idxName,
                grpCtx.statisticsHolderData(),
                ctx.metric()
            );

            PageMemory pageMem = grpCtx.dataRegion().pageMemory();

            for (int i = 0; i < rootPages.size(); i++) {
                Long rootPage = rootPages.get(i);

                assert rootPage != null;

                if (skipDeletedRoot(grpId, pageMem, rootPage)) {
                    ctx.log(getClass()).warning(S.toString("Skipping deletion of the index tree",
                        "cacheGrpName", cacheGrpName, false,
                        "cacheName", cacheName, false,
                        "idxName", idxName, false,
                        "segment", i, false,
                        "rootPageId", PageIdUtils.toDetailString(rootPage), false
                    ));

                    continue;
                }

                // Below we create a fake index tree using it's root page, stubbing some parameters,
                // because we just going to free memory pages that are occupied by tree structure.
                try {
                    String treeName = "deletedTree_" + i + "_" + name();

                    H2TreeToDestroy tree = new H2TreeToDestroy(
                        grpCtx,
                        null,
                        treeName,
                        idxName,
                        cacheName,
                        null,
                        offheap.reuseListForIndex(treeName),
                        grpId,
                        cacheGrpName,
                        pageMem,
                        ctx.cache().context().wal(),
                        offheap.globalRemoveId(),
                        rootPage,
                        false,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        new AtomicInteger(0),
                        false,
                        false,
                        false,
                        null,
                        ctx.failure(),
                        ctx.cache().context().diagnostic().pageLockTracker(),
                        null,
                        stats,
                        null,
                        0,
                        PageIoResolver.DEFAULT_PAGE_IO_RESOLVER
                    );

                    trees0.add(tree);
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException(e);
                }
            }
        }

        ctx.cache().context().database().checkpointReadLock();

        try {
            for (BPlusTree<?, ?> tree : trees0) {
                try {
                    tree.destroy(null, true);
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException(e);
                }
            }
        }
        finally {
            ctx.cache().context().database().checkpointReadUnlock();
        }
    }

    /**
     * Checks that pageId is still relevant and has not been deleted / reused.
     * @param grpId Cache group id.
     * @param pageMem Page memory instance.
     * @param rootPageId Root page identifier.
     * @return {@code true} if root page was deleted/reused, {@code false} otherwise.
     */
    private boolean skipDeletedRoot(int grpId, PageMemory pageMem, long rootPageId) {
        try {
            long page = pageMem.acquirePage(grpId, rootPageId);

            try {
                long pageAddr = pageMem.readLock(grpId, rootPageId, page);

                try {
                    return pageAddr == 0;
                }
                finally {
                    if (pageAddr != 0)
                        pageMem.readUnlock(grpId, rootPageId, page);
                }
            }
            finally {
                pageMem.releasePage(grpId, rootPageId, page);
            }
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Cannot acquire tree root page.", e);
        }
    }

    /** {@inheritDoc} */
    @Override public void cancel() {
        trees = null;

        GridWorker w = worker;

        if (w != null) {
            worker = null;

            U.awaitForWorkersStop(singleton(w), true, log);
        }
    }

    /** {@inheritDoc} */
    @Override public DurableBackgroundTask<?> convertAfterRestoreIfNeeded() {
        return new DurableBackgroundCleanupIndexTreeTaskV2(
            cacheGrpName,
            cacheName,
            idxName,
            treeName,
            UUID.randomUUID().toString(),
            rootPages.size(),
            null
        );
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DurableBackgroundCleanupIndexTreeTask.class, this);
    }

    /**
     * H2Tree that can be used only to destroy it.
     */
    public static class H2TreeToDestroy extends H2Tree {
        /** */
        private final CacheGroupContext grpCtx;

        /**
         * Constructor.
         *
         * @param grpCtx                  Cache group context.
         * @param table                   Owning table.
         * @param name                    Name of the tree.
         * @param idxName                 Name of the index.
         * @param cacheName               Name of the cache.
         * @param tblName                 Name of the table.
         * @param reuseList               Reuse list.
         * @param grpId                   Cache group ID.
         * @param grpName                 Name of the cache group.
         * @param pageMem                 Page memory.
         * @param wal                     Write ahead log manager.
         * @param globalRmvId             Global remove ID counter.
         * @param metaPageId              Meta page ID.
         * @param initNew                 if {@code true} new tree will be initialized, else meta page info will be read.
         * @param unwrappedCols           Unwrapped indexed columns.
         * @param wrappedCols             Original indexed columns.
         * @param maxCalculatedInlineSize Keep max calculated inline size for current index.
         * @param pk                      {@code true} for primary key.
         * @param affinityKey             {@code true} for affinity key.
         * @param mvccEnabled             Mvcc flag.
         * @param rowCache                Row cache.
         * @param failureProcessor        if the tree is corrupted.
         * @param pageLockTrackerManager  Page lock tracker manager.
         * @param log                     Logger.
         * @param stats                   Statistics holder.
         * @param factory                 Inline helper factory.
         * @param configuredInlineSize    Size that has been set by user during index creation.
         * @param pageIoRslvr             Page IO resolver.
         * @throws IgniteCheckedException If failed.
         */
        public H2TreeToDestroy(
            CacheGroupContext grpCtx,
            GridH2Table table,
            String name,
            String idxName,
            String cacheName,
            String tblName,
            ReuseList reuseList,
            int grpId,
            String grpName,
            PageMemory pageMem,
            IgniteWriteAheadLogManager wal,
            AtomicLong globalRmvId,
            long metaPageId,
            boolean initNew,
            List<IndexColumn> unwrappedCols,
            List<IndexColumn> wrappedCols,
            AtomicInteger maxCalculatedInlineSize,
            boolean pk,
            boolean affinityKey,
            boolean mvccEnabled,
            @Nullable H2RowCache rowCache,
            @Nullable FailureProcessor failureProcessor,
            PageLockTrackerManager pageLockTrackerManager,
            IgniteLogger log, IoStatisticsHolder stats,
            InlineIndexColumnFactory factory,
            int configuredInlineSize,
            PageIoResolver pageIoRslvr) throws IgniteCheckedException {
            super(
                null,
                table,
                name,
                idxName,
                cacheName,
                tblName,
                reuseList,
                grpId,
                grpName,
                pageMem,
                wal,
                globalRmvId,
                metaPageId,
                initNew,
                unwrappedCols,
                wrappedCols,
                maxCalculatedInlineSize,
                pk,
                affinityKey,
                mvccEnabled,
                rowCache,
                failureProcessor,
                pageLockTrackerManager,
                log,
                stats,
                factory,
                configuredInlineSize,
                pageIoRslvr
            );

            this.grpCtx = grpCtx;
        }

        /** {@inheritDoc} */
        @Override protected void temporaryReleaseLock() {
            grpCtx.shared().database().checkpointReadUnlock();
            grpCtx.shared().database().checkpointReadLock();
        }

        /** {@inheritDoc} */
        @Override protected long maxLockHoldTime() {
            long sysWorkerBlockedTimeout = grpCtx.shared().kernalContext().workersRegistry().getSystemWorkerBlockedTimeout();

            // Using timeout value reduced by 10 times to increase possibility of lock releasing before timeout.
            return sysWorkerBlockedTimeout == 0 ? Long.MAX_VALUE : (sysWorkerBlockedTimeout / 10);
        }
    }
}
