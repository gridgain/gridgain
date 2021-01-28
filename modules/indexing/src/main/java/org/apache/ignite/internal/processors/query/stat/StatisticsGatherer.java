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
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;
import org.gridgain.internal.h2.table.Column;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
    public void collectLocalObjectsStatisticsAsync(
        int cacheGrpId,
        final Set<StatisticsObjectConfiguration> objStatCfgs
    ) {
        assert !F.isEmpty(objStatCfgs);

        CacheGroupContext grpCtx = cacheProc.cacheGroup(cacheGrpId);

        Set<Integer> parts = grpCtx.affinity().primaryPartitions(discoMgr.localNode().id(), grpCtx.affinity().lastVersion());

        final LocalStatisticsGatheringContext newCtx = new LocalStatisticsGatheringContext(objStatCfgs, parts.size());

        LocalStatisticsGatheringContext inProgressCtx = gatheringInProgress.put(cacheGrpId, newCtx);

        if (inProgressCtx != null) {
            try {
                inProgressCtx.future().cancel();
            }
            catch (IgniteCheckedException ex) {
                log.warning("Unexpected exception on cancel gathering: [ctx=" + inProgressCtx + ']', ex);
            }
        }

        gatherPool.submit(() -> {
            collectLocalObjectsStatistics(objStatCfgs, parts, newCtx);

            statRepo.refreshAggregatedLocalStatistics(parts, objStatCfgs);
        });
    }

    /**
     * Collect statistic per partition for specified objects on the same cache group.
     */
    private void collectLocalObjectsStatistics(
        Set<StatisticsObjectConfiguration> objStatCfgs,
        Set<Integer> parts,
        LocalStatisticsGatheringContext gathCtx
    ) {
        try {
            log.info("+++ COLLECT " + objStatCfgs);

            Supplier<Boolean> cancelled = () -> gathCtx.future().isCancelled();

            Map<GridH2Table, Column[]> targets = new HashMap<>(objStatCfgs.size());

            for (StatisticsObjectConfiguration statCfg : objStatCfgs) {
                GridH2Table tbl = schemaMgr.dataTable(statCfg.key().schema(), statCfg.key().obj());

                assert tbl != null : "Unable to find to gather statistics [key=" + statCfg.key() + ']';

                targets.put(tbl, IgniteStatisticsHelper.filterColumns(tbl.getColumns(), statCfg.columns()));
            }

            Map<GridH2Table, Collection<ObjectPartitionStatisticsImpl>> tblPartStat = new HashMap<>(objStatCfgs.size());

            for (int partId : parts) {
                Map<GridH2Table, ObjectPartitionStatisticsImpl> partStats = collectPartitionStatistics(
                    targets, partId, cancelled
                );

                if (cancelled.get())
                    return;

                if (partStats != null) {
                    for (Map.Entry<GridH2Table, ObjectPartitionStatisticsImpl> tblStat : partStats.entrySet()) {
                        statRepo.saveLocalPartitionStatistics(
                            new StatisticsKey(tblStat.getKey().getSchema().getName(), tblStat.getKey().getName()),
                            tblStat.getValue()
                        );

                        // TODO: propagate stat to backups nodes
                        tblPartStat.computeIfAbsent(tblStat.getKey(), k -> new ArrayList<>(parts.size()))
                            .add(tblStat.getValue());
                    }
                }

                gathCtx.decrement();
            }
        }
        catch (Throwable ex) {
            log.warning("Unexpected exception on collect statistic [objs=" + objStatCfgs +
                ", parts=" + parts + ']', ex);
        }
    }

    /**
     * Collect single partition level statistics by the given tables.
     *
     * @param targets Table to column collection to collect statistics by. All tables should be in the same cache group.
     * @param partId Partition id to collect statistics by.
     * @param cancelled Supplier to check if collection was cancelled.
     * @return Map of table to Collection of partition level statistics by local primary partitions.
     */
    private Map<GridH2Table, ObjectPartitionStatisticsImpl> collectPartitionStatistics(
            Map<GridH2Table, Column[]> targets,
            int partId,
            Supplier<Boolean> cancelled
    ) {
        GridH2Table ftbl = targets.keySet().iterator().next();
        CacheGroupContext grp = ftbl.cacheContext().group();

        GridDhtPartitionTopology gTop = grp.topology();
        AffinityTopologyVersion topVer = gTop.readyTopologyVersion();

        GridDhtLocalPartition locPart = gTop.localPartition(partId, topVer, false);

        if (locPart == null)
            return null;

        int checkInt = CANCELLED_CHECK_INTERVAL;

        boolean reserved = locPart.reserve();

        try {
            if (!reserved || (locPart.state() != OWNING) || !locPart.primary(discoMgr.topologyVersionEx())) {
                if (locPart.state() == LOST)
                    return Collections.emptyMap();

                return null;
            }

            Map<GridH2Table, List<ColumnStatisticsCollector>> collectors = new HashMap<>(targets.size());
            Map<String, GridH2Table> tables = new HashMap<>(targets.size());

            for (Map.Entry<GridH2Table, Column[]> target : targets.entrySet()) {
                List<ColumnStatisticsCollector> colStatsCollectors = new ArrayList<>(target.getValue().length);

                for (Column col : target.getValue())
                    colStatsCollectors.add(new ColumnStatisticsCollector(col, target.getKey()::compareValues));

                collectors.put(target.getKey(), colStatsCollectors);

                tables.put(target.getKey().identifier().table(), target.getKey());
            }

            Map<GridH2Table, ObjectPartitionStatisticsImpl> res = new HashMap<>(targets.size());

            try {
                for (CacheDataRow row : grp.offheap().partitionIterator(partId)) {

                    if (--checkInt == 0) {
                        if (cancelled.get())
                            return null;

                        checkInt = CANCELLED_CHECK_INTERVAL;
                    }

                    GridCacheContext cacheCtx = (row.cacheId() == CU.UNDEFINED_CACHE_ID) ? grp.singleCacheContext() :
                            grp.shared().cacheContext(row.cacheId());

                    if (cacheCtx == null)
                        continue;

                    GridQueryTypeDescriptor typeDesc = qryProcessor.typeByValue(cacheCtx.name(),
                        cacheCtx.cacheObjectContext(), row.key(), row.value(), false);

                    GridH2Table tbl = tables.get(typeDesc.tableName());
                    if (tbl == null)
                        continue;

                    List<ColumnStatisticsCollector> tblColls = collectors.get(tbl);
                    H2Row h2row = tbl.rowDescriptor().createRow(row);

                    for (ColumnStatisticsCollector colStat : tblColls)
                        colStat.add(h2row.getValue(colStat.col().getColumnId()));
                }
            }
            catch (IgniteCheckedException e) {
                log.warning(String.format("Unable to collect partition level statistics by %s.%s:%d due to %s",
                        ftbl.identifier().schema(), ftbl.identifier().table(), partId, e.getMessage()));
            }

            for (Map.Entry<GridH2Table, List<ColumnStatisticsCollector>> tblCollectors : collectors.entrySet()) {
                Map<String, ColumnStatistics> colStats = tblCollectors.getValue().stream().collect(
                        Collectors.toMap(csc -> csc.col().getName(), ColumnStatisticsCollector::finish));

                ObjectPartitionStatisticsImpl tblStat = new ObjectPartitionStatisticsImpl(
                    partId,
                    colStats.values().iterator().next().total(),
                    locPart.updateCounter(),
                    colStats
                );

                res.put(tblCollectors.getKey(), tblStat);
            }

            return res;
        }
        finally {
            if (reserved)
                locPart.release();
        }
    }

    /**
     * Stop request crawler manager.
     */
    public void stop() {
        if (gatherPool != null) {
            List<Runnable> unfinishedTasks = gatherPool.shutdownNow();
            if (!unfinishedTasks.isEmpty())
                log.warning(String.format("%d statistics collection request cancelled.", unfinishedTasks.size()));
        }
    }
}
