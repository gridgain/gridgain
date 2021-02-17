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
package org.apache.ignite.internal.processors.query.stat;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.stat.config.StatisticsColumnConfiguration;
import org.apache.ignite.internal.processors.query.stat.task.GatherPartitionStatistics;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;
import org.gridgain.internal.h2.table.Column;

/**
 * Implementation of statistic collector.
 */
public class StatisticsGatherer {
    /** Logger. */
    private final IgniteLogger log;

    /** Ignite statistics repository. */
    private final IgniteStatisticsRepository statRepo;

    /** Ignite Thread pool executor to do statistics collection tasks. */
    private final IgniteThreadPoolExecutor gatherPool;

    /** (cacheGroupId -> gather context) */
    private final ConcurrentMap<StatisticsKey, LocalStatisticsGatheringContext> gatheringInProgress = new ConcurrentHashMap<>();

    /** Node stop lock. */
    private final GridSpinBusyLock stopLock;

    /**
     * Constructor.
     *
     * @param repo IgniteStatisticsRepository.
     * @param gatherPool Thread pool to gather statistics in.
     * @param logSupplier Log supplier function.
     */
    public StatisticsGatherer(
        IgniteStatisticsRepository repo,
        IgniteThreadPoolExecutor gatherPool,
        GridSpinBusyLock stopLock,
        Function<Class<?>, IgniteLogger> logSupplier
    ) {
        this.log = logSupplier.apply(StatisticsGatherer.class);
        this.statRepo = repo;
        this.gatherPool = gatherPool;
        this.stopLock = stopLock;
    }

    /** */
    public LocalStatisticsGatheringContext aggregateStatisticsAsync(
        final StatisticsKey key,
        Supplier<ObjectStatisticsImpl> aggregate
    ) {
        final LocalStatisticsGatheringContext ctx = new LocalStatisticsGatheringContext(Collections.emptySet());

        // Only refresh local aggregates.
        ctx.futureGather().complete(null);

        LocalStatisticsGatheringContext inProgressCtx = gatheringInProgress.putIfAbsent(key, ctx);

        if (inProgressCtx == null) {
            CompletableFuture<ObjectStatisticsImpl> f = CompletableFuture.supplyAsync(aggregate, gatherPool);

            f.handle((stat, ex) -> {
                if (ex == null)
                    ctx.futureAggregate().complete(stat);
                else
                    ctx.futureAggregate().completeExceptionally(ex);

                gatheringInProgress.remove(key, ctx);

                return null;
            });

            return ctx;
        }
        else {
            inProgressCtx.futureGather().thenAccept((v) -> {
                ObjectStatisticsImpl stat = aggregate.get();

                inProgressCtx.futureAggregate().complete(stat);
            });

            return inProgressCtx;
        }
    }

    /**
     * Collect statistic per partition for specified objects on the same cache group.
     */
    public LocalStatisticsGatheringContext gatherLocalObjectsStatisticsAsync(
        GridH2Table tbl,
        Column[] cols,
        Map<String, StatisticsColumnConfiguration> colCfgs,
        Set<Integer> parts
    ) {
        StatisticsKey key = new StatisticsKey(tbl.getSchema().getName(), tbl.getName());

        if (log.isDebugEnabled()) {
            log.debug("Start statistics gathering [key=" + key +
                ", cols=" + Arrays.toString(cols) +
                ", cfgs=" + colCfgs +
                ", parts=" + parts + ']');
        }

        final LocalStatisticsGatheringContext newCtx = new LocalStatisticsGatheringContext(parts);

        LocalStatisticsGatheringContext oldCtx = gatheringInProgress.put(key, newCtx);

        if (oldCtx != null) {
            if (log.isDebugEnabled())
                log.debug("Cancel previous statistic gathering for [key=" + key + ']');

            oldCtx.futureGather().cancel(false);
        }

        for (int part : parts) {
            final GatherPartitionStatistics task = new GatherPartitionStatistics(
                newCtx,
                tbl,
                cols,
                colCfgs,
                part,
                stopLock,
                log
            );

            submitTask(tbl, key, newCtx, task);
        }

        // Wait all partition gathering tasks.
        return newCtx;
    }

    /** */
    private void submitTask(
        final GridH2Table tbl,
        final StatisticsKey key,
        final LocalStatisticsGatheringContext ctx,
        final GatherPartitionStatistics task)
    {
        CompletableFuture<ObjectPartitionStatisticsImpl> f = CompletableFuture.supplyAsync(task::call, gatherPool);

        f.thenAccept((partStat) -> {
            completePartitionStatistic(tbl, key, ctx, task.partition(), partStat);
        });

        f.exceptionally((ex) -> {
            if (ex instanceof GatherStatisticRetryException) {
                if (log.isDebugEnabled())
                    log.debug("Retry collect statistics task [key=" + key + ", part=" + task.partition() + ']');

                submitTask(tbl, key, ctx, task);
            }
            if (ex instanceof GatherStatisticCancelException) {
                if (log.isDebugEnabled()) {
                    log.debug("Collect statistics task was cancelled " +
                        "[key=" + key + ", part=" + task.partition() + ']');
                }
            }
            else {
                log.error("Unexpected error on statistic gathering", ex);

                ctx.futureGather().obtrudeException(ex);
            }

            return null;
        });
    }

    /** */
    private void completePartitionStatistic(
        GridH2Table tbl,
        StatisticsKey key,
        LocalStatisticsGatheringContext ctx,
        int part,
        ObjectPartitionStatisticsImpl partStat)
    {
        if (!stopLock.enterBusy())
            return;

        try {
            if (partStat == null)
                return;

            statRepo.saveLocalPartitionStatistics(
                new StatisticsKey(tbl.getSchema().getName(), tbl.getName()),
                partStat
            );

            if (log.isDebugEnabled())
                log.debug("Local partitioned statistic saved [stat=" + partStat + ']');

            ctx.partitionDone(part);

            if (ctx.futureGather().isDone())
                gatheringInProgress.remove(key, ctx);
        }
        catch (Throwable ex) {
            if (!X.hasCause(ex, NodeStoppingException.class))
                log.error("Unexpected error os statistic save", ex);
        }
        finally {
            stopLock.leaveBusy();
        }
    }

    /** */
    public LocalStatisticsGatheringContext gatheringInProgress(StatisticsKey key) {
        return gatheringInProgress.get(key);
    }

    /** */
    public void submit(Runnable task) {
        gatherPool.submit(task);
    }

    /** */
    public void stop() {
        gatheringInProgress.values().forEach(ctx -> ctx.futureGather().cancel(true));
    }
}
