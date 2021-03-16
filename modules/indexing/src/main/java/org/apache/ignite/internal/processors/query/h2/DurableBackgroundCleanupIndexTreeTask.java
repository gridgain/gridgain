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
import org.apache.ignite.internal.metric.IoStatisticsHolder;
import org.apache.ignite.internal.metric.IoStatisticsHolderIndex;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.pendingtask.DurableBackgroundTask;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIoResolver;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.failure.FailureProcessor;
import org.apache.ignite.internal.processors.query.h2.database.H2Tree;
import org.apache.ignite.internal.processors.query.h2.database.inlinecolumn.InlineIndexColumnFactory;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.gridgain.internal.h2.table.IndexColumn;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.metric.IoStatisticsType.SORTED_INDEX;

/**
 * Tasks that cleans up index tree.
 */
public class DurableBackgroundCleanupIndexTreeTask implements DurableBackgroundTask {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private List<Long> rootPages;

    /** */
    private transient List<H2Tree> trees;

    /** */
    private transient volatile boolean completed;

    /** */
    private String cacheGrpName;

    /** */
    private String cacheName;

    /** */
    private String schemaName;

    /** */
    private String idxName;

    /** */
    private String id;

    /** */
    public DurableBackgroundCleanupIndexTreeTask(
        List<Long> rootPages,
        List<H2Tree> trees,
        String cacheGrpName,
        String cacheName,
        String schemaName,
        String idxName
    ) {
        this.rootPages = rootPages;
        this.trees = trees;
        this.completed = false;
        this.cacheGrpName = cacheGrpName;
        this.cacheName = cacheName;
        this.schemaName = schemaName;
        this.idxName = idxName;
        this.id = UUID.randomUUID().toString();
    }

    /** {@inheritDoc} */
    @Override public String shortName() {
        return "DROP_SQL_INDEX-" + schemaName + "." + idxName + "-" + id;
    }

    /** {@inheritDoc} */
    @Override public void execute(GridKernalContext ctx) {
        List<H2Tree> trees0 = trees;

        if (trees0 == null) {
            trees0 = new ArrayList<>(rootPages.size());

            CacheGroupContext grpCtx = ctx.cache().cacheGroup(CU.cacheGroupId(cacheName, cacheGrpName));

            // If group context is null, it means that group doesn't exist and we don't need this task anymore.
            if (grpCtx == null)
                return;

            IoStatisticsHolderIndex stats = new IoStatisticsHolderIndex(
                SORTED_INDEX,
                cacheGrpName,
                idxName,
                grpCtx.statisticsHolderData(),
                ctx.metric()
            );

            for (int i = 0; i < rootPages.size(); i++) {
                Long rootPage = rootPages.get(i);

                assert rootPage != null;

                // Below we create a fake index tree using it's root page, stubbing some parameters,
                // because we just going to free memory pages that are occupied by tree structure.
                try {
                    String treeName = "deletedTree_" + i + "_" + shortName();

                    H2TreeToDestroy tree = new H2TreeToDestroy(
                        grpCtx,
                        null,
                        treeName,
                        idxName,
                        cacheName,
                        null,
                        grpCtx.offheap().reuseListForIndex(treeName),
                        CU.cacheGroupId(cacheName, cacheGrpName),
                        cacheGrpName,
                        grpCtx.dataRegion().pageMemory(),
                        ctx.cache().context().wal(),
                        grpCtx.offheap().globalRemoveId(),
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
            for (int i = 0; i < trees0.size(); i++) {
                BPlusTree tree = trees0.get(i);

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

    /** {@inheritDoc} */
    @Override public void complete() {
        completed = true;
    }

    /** {@inheritDoc} */
    @Override public boolean isCompleted() {
        return completed;
    }

    /** {@inheritDoc} */
    @Override public void onCancel() {
        trees = null;
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
