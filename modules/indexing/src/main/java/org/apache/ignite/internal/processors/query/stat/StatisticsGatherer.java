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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.h2.SchemaManager;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.h2.opt.H2Row;
import org.apache.ignite.internal.processors.query.stat.config.StatisticsObjectConfiguration;
import org.apache.ignite.internal.processors.query.stat.task.CollectPartitionStatistics;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;
import org.gridgain.internal.h2.table.Column;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.LOST;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.OWNING;

/**
 * Implementation of statistic collector.
 */
public class StatisticsGatherer {
    /** Canceled check interval. */
    private static final int CANCELLED_CHECK_INTERVAL = 100;

    /** Logger. */
    private final IgniteLogger log;

    /** Schema manager. */
    private final SchemaManager schemaMgr;

    /** Discovery manager. */
    private final GridDiscoveryManager discoMgr;

    /**  */
    private final GridCacheProcessor cacheProc;

    /** Query processor */
    private final GridQueryProcessor qryProcessor;

    /** Ignite statistics repository. */
    private final IgniteStatisticsRepository statRepo;

    /** Statistics crawler. */
    private final StatisticsRequestProcessor reqProc;

    /** Ignite Thread pool executor to do statistics collection tasks. */
    private final IgniteThreadPoolExecutor gatherPool;

    /** (cacheGroupId -> gather context) */
    private final ConcurrentMap<Integer, LocalStatisticsGatheringContext> gatheringInProgress = new ConcurrentHashMap<>();

    /**
     * Constructor.
     *
     * @param schemaMgr Schema manager.
     * @param discoMgr Discovery manager.
     * @param qryProcessor Query processor.
     * @param repo IgniteStatisticsRepository.
     * @param reqProc Statistics request crawler.
     * @param gatherPool Thread pool to gather statistics in.
     * @param logSupplier Log supplier function.
     */
    public StatisticsGatherer(
        SchemaManager schemaMgr,
        GridDiscoveryManager discoMgr,
        GridCacheProcessor cacheProc,
        GridQueryProcessor qryProcessor,
        IgniteStatisticsRepository repo,
        StatisticsRequestProcessor reqProc,
        IgniteThreadPoolExecutor gatherPool,
        Function<Class<?>, IgniteLogger> logSupplier
    ) {
        this.log = logSupplier.apply(StatisticsGatherer.class);
        this.schemaMgr = schemaMgr;
        this.discoMgr = discoMgr;
        this.cacheProc = cacheProc;
        this.qryProcessor = qryProcessor;
        this.statRepo = repo;
        this.reqProc = reqProc;
        this.gatherPool = gatherPool;
    }

    /**
     * Collect statistic per partition for specified objects on the same cache group.
     */
    public CompletableFuture<Void> collectLocalObjectsStatisticsAsync(
        GridH2Table tbl,
        Column[] cols,
        Set<Integer> parts,
        long ver
    ) {
        log.info("+++ collectLocalObjectsStatisticsAsync " + tbl.getName() + ", " + parts);
        final LocalStatisticsGatheringContext newCtx = new LocalStatisticsGatheringContext(parts.size());

        List<CompletableFuture<ObjectPartitionStatisticsImpl>> futs = new ArrayList<>(parts.size());

        for (int part : parts) {
            CollectPartitionStatistics task = new CollectPartitionStatistics(
                tbl,
                cols,
                part,
                ver,
                () -> newCtx.future().isCancelled(),
                log
            );

            CompletableFuture<ObjectPartitionStatisticsImpl> f = CompletableFuture.supplyAsync(task::call, gatherPool);

            f.thenAccept((partStat) -> {
                statRepo.saveLocalPartitionStatistics(
                    new StatisticsKey(tbl.getSchema().getName(), tbl.getName()),
                    partStat
                );
            });

            futs.add(f);
        }

        // Wait all partition gathering tasks.
        return CompletableFuture.supplyAsync(() -> {
            futs.forEach(CompletableFuture::join);

            return null;
        }, gatherPool);
    }

    /** */
    private static boolean wasExpired(CacheDataRow row, long time) {
        return row.expireTime() > 0 && row.expireTime() <= time;
    }
}
