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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.h2.SchemaManager;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2RowDescriptor;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.h2.opt.H2Row;
import org.apache.ignite.internal.util.typedef.F;
import org.gridgain.internal.h2.table.Column;

import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.MOVING;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.OWNING;

/**
 * Statistics manager implementation.
 */
public class IgniteStatisticsManagerImpl implements IgniteStatisticsManager {

    /** Logger. */
    private IgniteLogger log;

    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Schema manager. */
    private final SchemaManager schemaMgr;

    /** Statistics repository. */
    private final IgniteStatisticsRepository statsRepos;

    /**
     * Constructor.
     *
     * @param ctx kernal context.
     * @param schemaMgr schema manager.
     */
    public IgniteStatisticsManagerImpl(GridKernalContext ctx, SchemaManager schemaMgr) {
        this.ctx = ctx;
        this.schemaMgr = schemaMgr;

        log = ctx.log(IgniteStatisticsManagerImpl.class);
        statsRepos = new IgniteStatisticsRepositoryImpl(ctx);
    }

    /**
     * @return statistics repository.
     */
    public IgniteStatisticsRepository statisticsRepository() {
        return statsRepos;
    }

    /** {@inheritDoc} */
    @Override public ObjectStatistics getLocalStatistics(String schemaName, String objName) {
        return statsRepos.getLocalStatistics(new StatsKey(schemaName, objName));
    }

    /** {@inheritDoc} */
    @Override public void clearObjectStatistics(String schemaName, String objName, String... colNames) {
        StatsKey key = new StatsKey(schemaName, objName);
        statsRepos.clearLocalPartitionsStatistics(key, colNames);
        statsRepos.clearLocalStatistics(key, colNames);
        statsRepos.clearGlobalStatistics(key, colNames);
    }

    /**
     * Filter columns by specified names.
     *
     * @param columns columns to filter.
     * @param colNames names.
     * @return column with specified names.
     */
    private Column[] filterColumns(Column[] columns, String... colNames) {
        if (F.isEmpty(colNames))
            return columns;

        Set<String> colNamesSet = new HashSet(Arrays.asList(colNames));
        List<Column> resultList = new ArrayList<>(colNames.length);

        for (Column col : columns)
            if (colNamesSet.contains(col.getName()))
                resultList.add(col);

        return resultList.toArray(new Column[resultList.size()]);
    }

    /** {@inheritDoc} */
    @Override public void collectObjectStatistics(String schemaName, String objName, String... colNames)
            throws IgniteCheckedException {
        GridH2Table tbl = schemaMgr.dataTable(schemaName, objName);
        if (tbl == null)
            throw new IllegalArgumentException(String.format("Can't find table %s.%s", schemaName, objName));

        if (log.isDebugEnabled())
            log.debug(String.format("Starting statistics collection by %s.%s object", schemaName, objName));

        Column[] selectedColumns;
        boolean fullStat;
        if (F.isEmpty(colNames)) {
            fullStat = true;
            selectedColumns = tbl.getColumns();
        } else {
            fullStat = false;
            selectedColumns = filterColumns(tbl.getColumns(), colNames);
        }

        Collection<ObjectPartitionStatisticsImpl> partsStats = collectPartitionStatistics(tbl, selectedColumns);
        StatsKey key = new StatsKey(tbl.identifier().schema(), tbl.identifier().table());
        statsRepos.saveLocalPartitionsStatistics(key, partsStats, fullStat);

        ObjectStatisticsImpl tblStats = aggregateLocalStatistics(tbl, selectedColumns, partsStats);
        statsRepos.saveLocalStatistics(key, tblStats, fullStat);
        if (log.isDebugEnabled())
            log.debug(String.format("Statistics collection by %s.%s object is finished.", schemaName, objName));
    }

    /**
     * Collect partition level statistics.
     *
     * @param tbl table to collect statistics by.
     * @param selectedColumns columns to collect statistics by.
     * @return collection of partition level statistics by local primary partitions.
     * @throws IgniteCheckedException in case of error.
     */
    private Collection<ObjectPartitionStatisticsImpl> collectPartitionStatistics(GridH2Table tbl, Column[] selectedColumns)
            throws IgniteCheckedException {
        List<ObjectPartitionStatisticsImpl> tblPartStats = new ArrayList<>();
        GridH2RowDescriptor desc = tbl.rowDescriptor();
        String tblName = tbl.getName();

        for (GridDhtLocalPartition locPart : tbl.cacheContext().topology().localPartitions()) {
            final boolean reserved = locPart.reserve();

            try {
                if (!reserved || (locPart.state() != OWNING && locPart.state() != MOVING)
                        || !locPart.primary(ctx.discovery().topologyVersionEx()))
                    continue;

                if (locPart.state() == MOVING)
                    tbl.cacheContext().preloader().syncFuture().get();

                long rowsCnt = 0;

                List<ColumnStatisticsCollector> colStatsCollectors = new ArrayList<>(selectedColumns.length);

                for (Column col : selectedColumns)
                    colStatsCollectors.add(new ColumnStatisticsCollector(col, tbl::compareValues));

                for (CacheDataRow row : tbl.cacheContext().offheap().cachePartitionIterator(tbl.cacheId(), locPart.id(),
                        null, true)) {
                    GridQueryTypeDescriptor typeDesc = ctx.query().typeByValue(tbl.cacheName(),
                            tbl.cacheContext().cacheObjectContext(), row.key(), row.value(), false);
                    if (!tblName.equals(typeDesc.tableName()))
                        continue;

                    rowsCnt++;

                    H2Row row0 = desc.createRow(row);

                    for (ColumnStatisticsCollector colStat : colStatsCollectors)
                        colStat.add(row0.getValue(colStat.col().getColumnId()));

                }

                Map<String, ColumnStatistics> colStats = colStatsCollectors.stream().collect(Collectors.toMap(
                        csc -> csc.col().getName(), csc -> csc.finish()
                ));

                tblPartStats.add(new ObjectPartitionStatisticsImpl(locPart.id(), true, rowsCnt,
                        locPart.updateCounter(), colStats));
            }
            finally {
                if (reserved)
                    locPart.release();
            }
        }

        return tblPartStats;
    }

    /**
     * Aggregate specified partition level statistics to local level statistics.
     *
     * @param key aggregation key.
     * @param tblPartStats collection of all local partition level statistics by specified key.
     * @return local level aggregated statistics.
     */
    public ObjectStatisticsImpl aggregateLocalStatistics(StatsKey key, Collection<ObjectPartitionStatisticsImpl> tblPartStats) {
        // For now there can be only tables
        GridH2Table table = schemaMgr.dataTable(key.schema(), key.obj());

        if (table == null) {
            // remove all loaded statistics.
            log.info("Removing statistics for object " + key + " cause table doesn't exists.");
            statsRepos.clearLocalPartitionsStatistics(key);
        }
        return aggregateLocalStatistics(table, table.getColumns(), tblPartStats);
    }

    /**
     * Aggregate partition level statistics to local level one.
     *
     * @param tbl table to aggregate statistics by.
     * @param selectedColumns columns to aggregate statistics by.
     * @param tblPartStats collection of partition level statistics.
     * @return local level statistics.
     */
    private ObjectStatisticsImpl aggregateLocalStatistics(
            GridH2Table tbl,
            Column[] selectedColumns,
            Collection<ObjectPartitionStatisticsImpl> tblPartStats
    ) {
        Map<Column, List<ColumnStatistics>> colPartStats = new HashMap<>(selectedColumns.length);
        long rowCnt = 0;
        for (Column col : selectedColumns)
            colPartStats.put(col, new ArrayList<>());

        for (ObjectPartitionStatisticsImpl partStat : tblPartStats) {
            for (Column col : selectedColumns) {
                ColumnStatistics colPartStat = partStat.columnStatistics(col.getName());
                if (colPartStat != null) {
                    colPartStats.compute(col, (k, v) -> {
                        v.add(colPartStat);
                        return v;
                    });
                }
            }
            rowCnt += partStat.rowCount();
        }

        Map<String, ColumnStatistics> colStats = new HashMap<>(selectedColumns.length);
        for (Column col : selectedColumns) {
            ColumnStatistics stat = ColumnStatisticsCollector.aggregate(tbl::compareValues, colPartStats.get(col));
            colStats.put(col.getName(), stat);
        }

        ObjectStatisticsImpl tblStats = new ObjectStatisticsImpl(rowCnt, colStats);

        return tblStats;
    }
}
