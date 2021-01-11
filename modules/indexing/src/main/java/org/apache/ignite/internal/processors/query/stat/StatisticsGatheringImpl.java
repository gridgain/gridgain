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
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.h2.SchemaManager;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2RowDescriptor;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.h2.opt.H2Row;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsKeyMessage;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;
import org.gridgain.internal.h2.table.Column;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.OWNING;

/**
 * Implementation of statistic collector.
 */
public class StatisticsGatheringImpl implements StatisticsGathering {
    /** Logger. */
    private final IgniteLogger log;

    /** Schema manager. */
    private final SchemaManager schemaMgr;

    /** Discovery manager. */
    private final GridDiscoveryManager discoMgr;

    /** Query processor */
    private final GridQueryProcessor qryProcessor;

    /** Grid cache processor. */
    private final GridCacheProcessor cacheProcessor;

    /** Statistics crawler. */
    private final StatisticsGatheringRequestCrawler statCrawler;

    /** Ignite Thread pool executor to do statistics collection tasks. */
    private final IgniteThreadPoolExecutor gatMgmtPool;

    /**
     * Constructor.
     *
     * @param schemaMgr Schema manager.
     * @param discoMgr Discovery manager.
     * @param qryProcessor Query processor.
     * @param cacheProcessor Grid cache processor.
     * @param statCrawler Statistics request crawler.
     * @param gatMgmtPool Thread pool to gather statistics in.
     * @param logSupplier Log supplier function.
     */
    public StatisticsGatheringImpl(
        SchemaManager schemaMgr,
        GridDiscoveryManager discoMgr,
        GridQueryProcessor qryProcessor,
        GridCacheProcessor cacheProcessor,
        StatisticsGatheringRequestCrawler statCrawler,
        IgniteThreadPoolExecutor gatMgmtPool,
        Function<Class<?>, IgniteLogger> logSupplier
    ) {
        this.log = logSupplier.apply(StatisticsGatheringImpl.class);
        this.schemaMgr = schemaMgr;
        this.discoMgr = discoMgr;
        this.qryProcessor = qryProcessor;
        this.cacheProcessor = cacheProcessor;
        this.statCrawler = statCrawler;
        this.gatMgmtPool = gatMgmtPool;
    }

    /**
     * Filter columns by specified names.
     *
     * @param cols Columns to filter.
     * @param colNames Column names.
     * @return Column with specified names.
     */
    private Column[] filterColumns(Column[] cols, @Nullable Collection<String> colNames) {
        if (F.isEmpty(colNames))
            return cols;

        Set<String> colNamesSet = new HashSet<>(colNames);

        return Arrays.stream(cols).filter(c -> colNamesSet.contains(c.getName())).toArray(Column[]::new);
    }

    /**
     * Collect single partition level statistics by the given tables.
     *
     * @param targets Table to column collection to collect statistics by. All tables should be in the same cache group.
     * @param partId Partition id to collect statistics by.
     * @param cancelled Supplier to check if collection was cancelled.
     * @return Map of table to Collection of partition level statistics by local primary partitions.
     * @throws IgniteCheckedException in case of error.
     */
    private Map<GridH2Table, ObjectPartitionStatisticsImpl> collectPartitionStatisticsCaches(
            Map<GridH2Table, Column[]> targets,
            int partId,
            Supplier<Boolean> cancelled
    ) {
        Map<GridH2Table, ObjectPartitionStatisticsImpl> res = new HashMap<>(targets.size());

        for (Map.Entry<GridH2Table, Column[]> tblTarger : targets.entrySet()) {
            GridH2Table tbl = tblTarger.getKey();
            Column[] selectedCols = tblTarger.getValue();
            GridH2RowDescriptor desc = tbl.rowDescriptor();
            String tblName = tbl.getName();
            GridDhtPartitionTopology top = tbl.cacheContext().topology();
            AffinityTopologyVersion topVer = top.readyTopologyVersion();

            GridDhtLocalPartition locPart = top.localPartition(partId, topVer, false);
            if (locPart == null)
                continue;

            if (cancelled.get()) {
                if (log.isInfoEnabled())
                    log.info(String.format("Canceled collection of object %s.%s statistics.", tbl.identifier().schema(),
                            tbl.identifier().table()));

                return null;
            }

            final boolean reserved = locPart.reserve();

            try {
                if (!reserved || (locPart.state() != OWNING) || !locPart.primary(discoMgr.topologyVersionEx()))
                    continue;

                long rowsCnt = 0;

                List<ColumnStatisticsCollector> colStatsCollectors = new ArrayList<>(selectedCols.length);

                for (Column col : selectedCols)
                    colStatsCollectors.add(new ColumnStatisticsCollector(col, tbl::compareValues));

                for (CacheDataRow row : tbl.cacheContext().offheap().cachePartitionIterator(tbl.cacheId(), locPart.id(),
                        null, true)) {
                    GridQueryTypeDescriptor typeDesc = qryProcessor.typeByValue(tbl.cacheName(),
                            tbl.cacheContext().cacheObjectContext(), row.key(), row.value(), false);
                    if (!tblName.equals(typeDesc.tableName()))
                        continue;

                    rowsCnt++;

                    H2Row row0 = desc.createRow(row);

                    for (ColumnStatisticsCollector colStat : colStatsCollectors)
                        colStat.add(row0.getValue(colStat.col().getColumnId()));

                }

                Map<String, ColumnStatistics> colStats = colStatsCollectors.stream().collect(Collectors.toMap(
                        csc -> csc.col().getName(), ColumnStatisticsCollector::finish));

                res.put(tbl, new ObjectPartitionStatisticsImpl(locPart.id(), true, rowsCnt,
                        locPart.updateCounter(), colStats));

                if (log.isTraceEnabled())
                    log.trace(String.format("Finished statistics collection on %s.%s:%d",
                            tbl.identifier().schema(), tbl.identifier().table(), locPart.id()));
            }
            catch (IgniteCheckedException e) {
                if (log.isDebugEnabled())
                    log.debug(String.format("Unable to collect statistics by %s.%s:%d due to error %s",
                            tbl.identifier().schema(), tbl.identifier().table(), partId, e.getMessage()));
            }
            finally {
                if (reserved)
                    locPart.release();
            }
        }
        return res;
    }

    /**
     * Collect single partition level statistics by the given tables.
     *
     * @param targets Table to column collection to collect statistics by. All tables should be in the same cache group.
     * @param partId Partition id to collect statistics by.
     * @param cancelled Supplier to check if collection was cancelled.
     * @return Map of table to Collection of partition level statistics by local primary partitions.
     * @throws IgniteCheckedException in case of error.
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

        boolean reserved = locPart.reserve();
        try {
            if (!reserved || (locPart.state() != OWNING) || !locPart.primary(discoMgr.topologyVersionEx()))
                return null;

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
                    if (cancelled.get())
                        return null;

                    GridCacheContext cacheCtx = (row.cacheId() == CU.UNDEFINED_CACHE_ID) ? grp.singleCacheContext() :
                            grp.shared().cacheContext(row.cacheId());

                    if (cacheCtx == null)
                        continue;

                    DynamicCacheDescriptor dcDescr = cacheProcessor.cacheDescriptor(row.cacheId());
                    //GridQueryTypeDescriptor typeDesc = qryProcessor.typeByValue(dcDescr.cacheName(),
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
            catch (IgniteCheckedException igniteCheckedException) {
                igniteCheckedException.printStackTrace();
            }

            for (Map.Entry<GridH2Table, List<ColumnStatisticsCollector>> tblCollectors : collectors.entrySet()) {
                Map<String, ColumnStatistics> colStats = tblCollectors.getValue().stream().collect(Collectors.toMap(
                        csc -> csc.col().getName(), ColumnStatisticsCollector::finish));
                ObjectPartitionStatisticsImpl tblStat = new ObjectPartitionStatisticsImpl(partId, true,
                        colStats.values().iterator().next().total(), locPart.updateCounter(), colStats);
                res.put(tblCollectors.getKey(), tblStat);
            }

            return res;
        }
        finally {
            if (reserved)
                locPart.release();
        }
    }

    /** {@inheritDoc} */
    @Override public ObjectStatisticsImpl aggregateLocalStatistics(
            StatisticsKeyMessage keyMsg,
            Collection<? extends ObjectStatisticsImpl> stats
    ) {
        // For now there can be only tables
        GridH2Table tbl = schemaMgr.dataTable(keyMsg.schema(), keyMsg.obj());

        if (tbl == null) {
            // remove all loaded statistics.
            if (log.isDebugEnabled())
                log.debug(String.format("Removing statistics for object %s.%s cause table doesn't exists.",
                        keyMsg.schema(), keyMsg.obj()));
        }

        return aggregateLocalStatistics(tbl, filterColumns(tbl.getColumns(), keyMsg.colNames()), stats);
    }

    /** {@inheritDoc} */
    @Override public void collectLocalObjectsStatisticsAsync(
        UUID reqId,
        Set<StatisticsKeyMessage> keys,
        int[] parts,
        Supplier<Boolean> cancelled
    ) {
        gatMgmtPool.submit(() -> collectLocalObjectsStatistics(reqId, keys, parts, cancelled));
    }

    /**
     * Collect local statistics by specified keys and partitions.
     *
     * @param reqId Request id.
     * @param keys Keys to collect statistics by.
     * @param parts Partitions to collect statistics from.
     */
    public void collectLocalObjectsStatistics(
            UUID reqId,
            Set<StatisticsKeyMessage> keys,
            int[] parts,
            Supplier<Boolean> cancelled
    ) {

        Map<GridH2Table, Column[]> targets = new HashMap<>(keys.size());
        Map<GridH2Table, StatisticsKeyMessage> tblKey = new HashMap<>(keys.size());
        for (StatisticsKeyMessage key : keys) {
            GridH2Table tbl = schemaMgr.dataTable(key.schema(), key.obj());
            if (tbl == null) {
                if (log.isDebugEnabled())
                    log.debug(String.format("Unable to find table %s.%s to gather its statistics by req %s",
                            key.schema(), key.obj(), reqId));

                // Send empty response may be necessary to cancel original context
                statCrawler.sendGatheringResponseAsync(reqId, Collections.emptyMap(), new int[0]);
            }

            targets.put(tbl, filterColumns(tbl.getColumns(), key.colNames()));

            if (tblKey.put(tbl, key) != null)
                log.info(String.format("Unable to collect statistics by same table %s.%s twice in single gathering task",
                        key.schema(), key.obj()));
        }

        Map<GridH2Table, Collection<ObjectPartitionStatisticsImpl>> tblPartStat = new HashMap<>(keys.size());
        List<Integer> collectedParts = new ArrayList<>(parts.length);
        for (int partId : parts) {
            Map<GridH2Table, ObjectPartitionStatisticsImpl> partStats = collectPartitionStatistics(targets, partId, cancelled);

            if (cancelled.get())
                return;

            if (partStats != null) {
                collectedParts.add(partId);
                for (Map.Entry<GridH2Table, ObjectPartitionStatisticsImpl> tblStat : partStats.entrySet()) {
                    StatisticsKeyMessage key = tblKey.get(tblStat.getKey());

                    tblPartStat.computeIfAbsent(tblStat.getKey(), k -> new ArrayList<>(parts.length)).add(tblStat.getValue());
                }
            }
        }

        Map<StatisticsKeyMessage, ObjectStatisticsImpl> res = new HashMap<>();
        for (Map.Entry<GridH2Table, Collection<ObjectPartitionStatisticsImpl>> partStats : tblPartStat.entrySet()) {
            ObjectStatisticsImpl locStat = aggregateLocalStatistics(partStats.getKey(), targets.get(partStats.getKey()),
                partStats.getValue());

            res.put(tblKey.get(partStats.getKey()), locStat);
        }

        statCrawler.sendGatheringResponseAsync(reqId, res, collectedParts.stream().mapToInt(Integer::intValue).toArray());
    }

    /**
     * Aggregate partition level statistics to local level one or local statistics to global one.
     *
     * @param tbl Table to aggregate statistics by.
     * @param selectedCols Columns to aggregate statistics by.
     * @param stats Collection of partition level or local level statistics to aggregate.
     * @return Local level statistics.
     */
    private ObjectStatisticsImpl aggregateLocalStatistics(
            GridH2Table tbl,
            Column[] selectedCols,
            Collection<? extends ObjectStatisticsImpl> stats
    ) {
        Map<Column, List<ColumnStatistics>> colPartStats = new HashMap<>(selectedCols.length);
        long rowCnt = 0;
        for (Column col : selectedCols)
            colPartStats.put(col, new ArrayList<>());

        for (ObjectStatisticsImpl partStat : stats) {
            for (Column col : selectedCols) {
                ColumnStatistics colPartStat = partStat.columnStatistics(col.getName());
                if (colPartStat != null) {
                    colPartStats.computeIfPresent(col, (k, v) -> {
                        v.add(colPartStat);

                        return v;
                    });
                }
            }
            rowCnt += partStat.rowCount();
        }

        Map<String, ColumnStatistics> colStats = new HashMap<>(selectedCols.length);
        for (Column col : selectedCols) {
            ColumnStatistics stat = ColumnStatisticsCollector.aggregate(tbl::compareValues, colPartStats.get(col));
            colStats.put(col.getName(), stat);
        }

        ObjectStatisticsImpl tblStats = new ObjectStatisticsImpl(rowCnt, colStats);

        return tblStats;
    }

    /**
     * Stop request crawler manager.
     */
    public void stop() {
        if (gatMgmtPool != null) {
            List<Runnable> unfinishedTasks = gatMgmtPool.shutdownNow();
            if (!unfinishedTasks.isEmpty())
                log.warning(String.format("%d statistics collection request cancelled.", unfinishedTasks.size()));
        }
    }
}
