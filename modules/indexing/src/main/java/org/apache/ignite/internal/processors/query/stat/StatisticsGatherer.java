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
import org.apache.ignite.internal.processors.query.stat.config.ObjectStatisticsConfiguration;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsKeyMessage;
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
import java.util.UUID;
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
    private final StatisticsGatheringRequestCrawler statCrawler;

    /** Ignite Thread pool executor to do statistics collection tasks. */
    private final IgniteThreadPoolExecutor gatherPool;

    /** */
    private final IgniteStatisticsStore statStore;

    /** (cacheGroupId -> gather context) */
    private final ConcurrentMap<Integer, LocalStatisticsGatheringContext> gatheringInProgress = new ConcurrentHashMap<>();

    /**
     * Constructor.
     *
     * @param schemaMgr Schema manager.
     * @param discoMgr Discovery manager.
     * @param qryProcessor Query processor.
     * @param repo IgniteStatisticsRepository.
     * @param statCrawler Statistics request crawler.
     * @param gatherPool Thread pool to gather statistics in.
     * @param logSupplier Log supplier function.
     */
    public StatisticsGatherer(
        SchemaManager schemaMgr,
        GridDiscoveryManager discoMgr,
        GridCacheProcessor cacheProc,
        GridQueryProcessor qryProcessor,
        IgniteStatisticsRepository repo,
        StatisticsGatheringRequestCrawler statCrawler,
        IgniteStatisticsStore statStore,
        IgniteThreadPoolExecutor gatherPool,
        Function<Class<?>, IgniteLogger> logSupplier
    ) {
        this.log = logSupplier.apply(StatisticsGatherer.class);
        this.schemaMgr = schemaMgr;
        this.discoMgr = discoMgr;
        this.cacheProc = cacheProc;
        this.qryProcessor = qryProcessor;
        this.statRepo = repo;
        this.statStore = statStore;
        this.statCrawler = statCrawler;
        this.gatherPool = gatherPool;
    }

    /** */
    public void collectLocalObjectsStatisticsAsync(
        final Set<ObjectStatisticsConfiguration> objectStatCfgs
    ) throws IgniteCheckedException {
        assert !F.isEmpty(objectStatCfgs);

        int cacheGroupId = F.first(objectStatCfgs).cacheGroupId();

        CacheGroupContext grpCtx = cacheProc.cacheGroup(cacheGroupId);

        Set<Integer> parts = grpCtx.affinity().primaryPartitions(discoMgr.localNode().id(), grpCtx.affinity().lastVersion());

        final LocalStatisticsGatheringContext newCtx = new LocalStatisticsGatheringContext(objectStatCfgs, parts.size());

        LocalStatisticsGatheringContext inProgressCtx = gatheringInProgress.put(cacheGroupId, newCtx);

        if (inProgressCtx != null)
            inProgressCtx.future().cancel();

        gatherPool.submit(() -> collectLocalObjectsStatistics(objectStatCfgs, parts, () -> newCtx.future().isCancelled()));
    }

    /** */
    private void collectLocalObjectsStatistics(
        Set<ObjectStatisticsConfiguration> objectStatCfgs,
        Set<Integer> parts,
        Supplier<Boolean> cancelled
    ) {
        Map<GridH2Table, Column[]> targets = new HashMap<>(objectStatCfgs.size());
        Map<GridH2Table, ObjectStatisticsConfiguration> tblObjStatCfg = new HashMap<>(objectStatCfgs.size());

        for (ObjectStatisticsConfiguration statCfg : objectStatCfgs) {
            GridH2Table tbl = schemaMgr.dataTable(statCfg.key().schema(), statCfg.key().obj());

            assert tbl != null : "Unable to find to gather statistics [key=" + statCfg.key() + ']';

            targets.put(tbl, IgniteStatisticsHelper.filterColumns(tbl.getColumns(), statCfg.columns()));

            if (tblObjStatCfg.put(tbl, statCfg) != null)
                log.warning(String.format("Unable to collect statistics by same table %s.%s twice in single gathering task",
                    statCfg.key().schema(), statCfg.key().obj()));
        }

        Map<GridH2Table, Collection<ObjectPartitionStatisticsImpl>> tblPartStat = new HashMap<>(objectStatCfgs.size());
        List<Integer> collectedParts = new ArrayList<>(parts.size());

        for (int partId : parts) {
            Map<GridH2Table, ObjectPartitionStatisticsImpl> partStats = collectPartitionStatistics(
                targets, partId, cancelled
            );

            if (cancelled.get())
                return;

            if (partStats != null) {
                collectedParts.add(partId);

                for (Map.Entry<GridH2Table, ObjectPartitionStatisticsImpl> tblStat : partStats.entrySet()) {
                    statStore.saveLocalPartitionStatistics(
                        new StatisticsKey(tblStat.getKey().getSchema().getName(), tblStat.getKey().getName()),
                        tblStat.getValue()
                    );

                    tblPartStat.computeIfAbsent(tblStat.getKey(), k -> new ArrayList<>(parts.size()))
                        .add(tblStat.getValue());
                }
            }
        }

        Map<StatisticsKey, ObjectStatisticsImpl> res = new HashMap<>();

        for (Map.Entry<GridH2Table, Collection<ObjectPartitionStatisticsImpl>> partStats : tblPartStat.entrySet()) {
            ObjectStatisticsImpl locStat = IgniteStatisticsHelper.aggregateLocalStatistics(
                partStats.getKey(),
                targets.get(partStats.getKey()),
                partStats.getValue(),
                log);

            ObjectStatisticsConfiguration objStatCfg = tblObjStatCfg.get(partStats.getKey());

            statRepo.mergeLocalStatistics(objStatCfg.key(), locStat);

            Collection<ObjectPartitionStatisticsImpl> mergedStats = statRepo.mergeLocalPartitionsStatistics(
                objStatCfg.key(),
                partStats.getValue()
            );

            res.put(objStatCfg.key(), locStat);
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

                // TODO: VER & CFG
                ObjectPartitionStatisticsImpl tblStat = new ObjectPartitionStatisticsImpl(partId, true,
                        colStats.values().iterator().next().total(), locPart.updateCounter(), colStats, null, 0);

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
