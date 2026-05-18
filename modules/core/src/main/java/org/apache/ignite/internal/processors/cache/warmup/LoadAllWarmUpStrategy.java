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

package org.apache.ignite.internal.processors.cache.warmup;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Supplier;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.LoadAllWarmUpConfiguration;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;

/**
 * "Load all" warm-up strategy, which loads pages to persistent data region
 * until it reaches {@link DataRegionConfiguration#getMaxSize} with priority
 * to index partitions. Partitions to load are planned with index-partition
 * priority per cache group; the planned partitions are then loaded
 * concurrently using a configurable number of worker threads
 * (see {@link LoadAllWarmUpConfiguration#getThreads()}).
 */
public class LoadAllWarmUpStrategy implements WarmUpStrategy<LoadAllWarmUpConfiguration> {
    /** Logger. */
    @GridToStringExclude
    private final IgniteLogger log;

    /**
     * Cache group contexts supplier.
     * Since {@link GridCacheProcessor} starts later.
     */
    @GridToStringExclude
    private final Supplier<Collection<CacheGroupContext>> grpCtxSup;

    /** Ignite instance name. */
    private final String igniteInstanceName;

    /** Stop flag. */
    private volatile boolean stop;

    /**
     * Constructor.
     *
     * @param log Logger.
     * @param grpCtxSup Cache group contexts supplier. Since {@link GridCacheProcessor} starts later.
     * @param igniteInstanceName Ignite instance name.
     */
    public LoadAllWarmUpStrategy(
            IgniteLogger log,
            Supplier<Collection<CacheGroupContext>> grpCtxSup,
            String igniteInstanceName
    ) {
        this.log = log;
        this.grpCtxSup = grpCtxSup;
        this.igniteInstanceName = igniteInstanceName;
    }

    /** {@inheritDoc} */
    @Override public Class<LoadAllWarmUpConfiguration> configClass() {
        return LoadAllWarmUpConfiguration.class;
    }

    /** {@inheritDoc} */
    @Override public void warmUp(LoadAllWarmUpConfiguration cfg, DataRegion region) throws IgniteCheckedException {
        if (stop)
            return;

        assert region.config().isPersistenceEnabled();

        Map<CacheGroupContext, List<LoadPartition>> loadDataInfo = loadDataInfo(region);

        long availableLoadPageCnt = availableLoadPageCount(region);

        int threads = Math.max(1, cfg.getThreads());

        if (log.isInfoEnabled()) {
            Collection<List<LoadPartition>> parts = loadDataInfo.values();

            int totalParts = loadDataInfo.values().stream().mapToInt(List::size).sum();

            long pageCnt = parts.stream().flatMap(Collection::stream).mapToLong(LoadPartition::pages).sum();

            List<String> grpNames = loadDataInfo.keySet().stream()
                    .map(CacheGroupContext::cacheOrGroupName)
                    .collect(toList());

            log.info(String.format(
                    "Order of cache groups loaded into data region [name=%s, partCnt=%d, pageCnt=%d, " +
                            "availablePageCnt=%d, threads=%d, grpNames=%s]",
                    region.config().getName(),
                    totalParts,
                    pageCnt,
                    availableLoadPageCnt,
                    threads,
                    grpNames
            ));
        }

        PageMemoryEx pageMemEx = (PageMemoryEx) region.pageMemory();

        if (threads == 1)
            warmUpSequential(loadDataInfo, pageMemEx);
        else
            warmUpParallel(loadDataInfo, pageMemEx, threads);
    }

    /**
     * Sequential warmup — loads partitions on the calling thread.
     */
    private void warmUpSequential(
            Map<CacheGroupContext, List<LoadPartition>> loadDataInfo,
            PageMemoryEx pageMemEx
    ) throws IgniteCheckedException {
        try {
            for (Map.Entry<CacheGroupContext, List<LoadPartition>> e : loadDataInfo.entrySet()) {
                if (stop)
                    return;

                CacheGroupContext grp = e.getKey();
                List<LoadPartition> parts = e.getValue();

                logStartGroup(grp, parts);

                for (LoadPartition part : parts) {
                    if (stop)
                        return;

                    loadPartition(pageMemEx, grp, part);
                }

                logFinishGroup(grp);
            }
        }
        catch (CompletionException ex) {
            throw unwrap(ex);
        }
    }

    /**
     * Parallel warmup — submits one {@link CompletableFuture} per planned {@link LoadPartition} to a pool.
     */
    private void warmUpParallel(
            Map<CacheGroupContext, List<LoadPartition>> loadDataInfo,
            PageMemoryEx pageMemEx,
            int threads
    ) throws IgniteCheckedException {
        ExecutorService pool = new IgniteThreadPoolExecutor(
                "warmup-loader",
                igniteInstanceName,
                threads,
                threads,
                30_000L,
                new LinkedBlockingQueue<>()
        );

        try {
            CompletableFuture<?>[] allGrpsFutures = new CompletableFuture[loadDataInfo.size()];
            int i = 0;

            for (Map.Entry<CacheGroupContext, List<LoadPartition>> e : loadDataInfo.entrySet()) {
                if (stop)
                    return;

                CacheGroupContext grp = e.getKey();
                List<LoadPartition> parts = e.getValue();

                logStartGroup(grp, parts);

                CompletableFuture<?>[] partFutures = new CompletableFuture[parts.size()];

                for (int j = 0; j < parts.size(); j++) {
                    LoadPartition part = parts.get(j);

                    partFutures[j] = CompletableFuture.runAsync(() -> loadPartition(pageMemEx, grp, part), pool);
                }

                CompletableFuture<Void> grpFuture = CompletableFuture.allOf(partFutures)
                        .thenRun(() -> logFinishGroup(grp));

                allGrpsFutures[i++] = grpFuture;
            }

            try {
                CompletableFuture.allOf(allGrpsFutures).get();
            }
            catch (ExecutionException ex) {
                throw new IgniteCheckedException(ex.getCause());
            }
            catch (InterruptedException ex) {
                Thread.currentThread().interrupt();

                throw new IgniteCheckedException(ex);
            }
        }
        finally {
            U.shutdownNow(getClass(), pool, log);
        }
    }

    /** Unwraps the underlying {@link IgniteCheckedException} from a {@link CompletionException}. */
    private static IgniteCheckedException unwrap(CompletionException ex) {
        Throwable c = ex.getCause();

        if (c instanceof IgniteCheckedException)
            return (IgniteCheckedException) c;

        return new IgniteCheckedException(c != null ? c : ex);
    }

    /** Logs that we're about to start a cache group. */
    private void logStartGroup(CacheGroupContext grp, List<LoadPartition> parts) {
        if (log.isInfoEnabled()) {
            long pages = parts.stream().mapToLong(LoadPartition::pages).sum();

            log.info(String.format(
                    "Starting warming up cache group [name=%s, partCnt=%d, pageCnt=%d]",
                    grp.cacheOrGroupName(), parts.size(), pages
            ));
        }
    }

    /** Logs that a cache group has been warmed up. */
    private void logFinishGroup(CacheGroupContext grp) {
        if (log.isInfoEnabled())
            log.info("Finished warming up cache group [name=" + grp.cacheOrGroupName() + ']');
    }

    /** Loads every planned page of a single partition. */
    protected void loadPartition(PageMemoryEx pageMemEx, CacheGroupContext grp, LoadPartition part) {
        if (stop)
            return;

        try {
            long pageId = pageMemEx.partitionMetaPageId(grp.groupId(), part.partitionId());

            for (int i = 0; i < part.pages(); i++, pageId++) {
                if (stop)
                    return;

                loadPage(pageMemEx, grp.groupId(), pageId);
            }
        }
        catch (Throwable t) {
            stop = true;

            throw t instanceof CompletionException ? (CompletionException) t : new CompletionException(t);
        }
    }

    /**
     * Acquires and immediately releases a page — the act of acquiring pulls it into the data region.
     */
    private void loadPage(PageMemoryEx pageMemEx, int grpId, long pageId) throws IgniteCheckedException {
        long pagePtr = -1;

        try {
            pagePtr = pageMemEx.acquirePage(grpId, pageId);
        }
        finally {
            if (pagePtr != -1)
                pageMemEx.releasePage(grpId, pageId, pagePtr);
        }
    }

    /** {@inheritDoc} */
    @Override public void stop() throws IgniteCheckedException {
        stop = true;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(LoadAllWarmUpStrategy.class, this);
    }

    /**
     * Getting count of pages available for loading into data region.
     *
     * @param region Data region.
     * @return Count(non-negative) of pages available for loading into data region.
     */
    protected long availableLoadPageCount(DataRegion region) {
        long maxSize = region.config().getMaxSize();
        long curSize = region.pageMemory().loadedPages() * region.pageMemory().systemPageSize();

        return Math.max(0, (maxSize - curSize) / region.pageMemory().systemPageSize());
    }

    /**
     * Calculation of cache groups, partitions and count of pages that can load
     * into data region. Calculation starts and includes an index partition for
     * each group.
     *
     * @param region Data region.
     * @return Loadable groups and partitions.
     * @throws IgniteCheckedException – if faild.
     */
    protected Map<CacheGroupContext, List<LoadPartition>> loadDataInfo(
        DataRegion region
    ) throws IgniteCheckedException {
        // Get cache groups of data region.
        List<CacheGroupContext> regionGrps = grpCtxSup.get().stream()
            .filter(grpCtx -> region.equals(grpCtx.dataRegion())).collect(toList());

        long availableLoadPageCnt = availableLoadPageCount(region);

        // Computing groups, partitions, and pages to load into data region.
        Map<CacheGroupContext, List<LoadPartition>> loadableGrps = new LinkedHashMap<>();

        for (int i = 0; i < regionGrps.size() && availableLoadPageCnt > 0; i++) {
            CacheGroupContext grp = regionGrps.get(i);

            // Index partition in priority.
            List<GridDhtLocalPartition> locParts = grp.topology().localPartitions();

            for (int j = -1; j < locParts.size() && availableLoadPageCnt > 0; j++) {
                int p = j == -1 ? INDEX_PARTITION : locParts.get(j).id();

                long partPageCnt = grp.shared().pageStore().pages(grp.groupId(), p);

                if (partPageCnt > 0) {
                    long pageCnt = (availableLoadPageCnt - partPageCnt) >= 0 ? partPageCnt : availableLoadPageCnt;

                    availableLoadPageCnt -= pageCnt;

                    loadableGrps.computeIfAbsent(grp, grpCtx -> new ArrayList<>()).add(new LoadPartition(p, pageCnt));
                }
            }
        }

        return loadableGrps;
    }

    /**
     * Information about loaded partition.
     */
    protected static class LoadPartition {
        /** Partition id. */
        private final int partitionId;

        /** Number of pages to load. */
        private final long pages;

        /**
         * Constructor.
         *
         * @param partitionId Partition id.
         * @param pages Number of pages to load.
         */
        public LoadPartition(int partitionId, long pages) {
            assert partitionId >= 0 : "Partition id cannot be negative.";
            assert pages > 0 : "Number of pages to load must be greater than zero.";

            this.partitionId = partitionId;
            this.pages = pages;
        }

        /**
         * Return partition id.
         *
         * @return Partition id.
         */
        public int partitionId() {
            return partitionId;
        }

        /**
         * Return number of pages to load.
         *
         * @return Number of pages to load.
         */
        public long pages() {
            return pages;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(LoadPartition.class, this);
        }
    }
}
