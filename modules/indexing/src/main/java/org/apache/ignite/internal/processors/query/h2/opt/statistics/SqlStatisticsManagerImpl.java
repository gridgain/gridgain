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

package org.apache.ignite.internal.processors.query.h2.opt.statistics;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.query.QueryTable;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2RowDescriptor;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.h2.opt.H2Row;
import org.apache.ignite.resources.LoggerResource;
import org.h2.table.Column;

import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.MOVING;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.OWNING;

public class SqlStatisticsManagerImpl implements SqlStatisticsManager {
    /** Logger. */
    @LoggerResource
    private IgniteLogger log;

    private final GridKernalContext ctx;

    private final SqlStatisticsRepository statsRepos;

    public SqlStatisticsManagerImpl(GridKernalContext ctx) {
        this.ctx = ctx;

        statsRepos = new SqlStatisticsRepositoryImpl(ctx);
    }
    public SqlStatisticsRepository statisticsRepository() {
        return statsRepos;
    }

    public void start() {
        statsRepos.start();
        //ctx.io().addMessageListener(GridTopic.TOPIC_QUERY, );
    }


    @Override
    public ObjectStatistics getLocalStatistics(QueryTable tbl) {
        return statsRepos.getLocalStatistics(tbl, true);
    }

    @Override
    public void clearObjectStatistics(QueryTable tbl, String... colNames) {
        statsRepos.clearLocalPartitionsStatistics(tbl, colNames);
        statsRepos.clearLocalStatistics(tbl, colNames);
        statsRepos.clearGlobalStatistics(tbl, colNames);
    }

    public void onPartEvicted(QueryTable table, int partId) {

        // Send partition stat
        // TODO
        // Remove partition stat
        statsRepos.clearLocalPartitionStatistics(table, partId);
        // Update local stat
        // TODO
    }

    /**
     * Filter columns by specified names.
     *
     * @param columns columns to filter.
     * @param colNames names.
     * @return column with specified names.
     */
    private Column[] filterColumns(Column[] columns, String ... colNames) {
        if (colNames == null || colNames.length == 0) {
            return columns;
        }
        List<Column> resultList = new ArrayList<>(colNames.length);

        for(String colName : colNames)
            for(Column col : columns)

                if (colName.equals(col.getName())) {
                    resultList.add(col);
                    break;
                }

        return resultList.toArray(new Column[resultList.size()]);
    }

    @Override
    public void refreshStatistics(GridH2Table... tbls) {

    }

    @Override
    public void collectObjectStatistics(GridH2Table tbl, String ... colNames) throws IgniteCheckedException {
        assert tbl != null;

        Column[] selectedColumns;
        boolean fullStat;
        if (colNames == null || colNames.length == 0) {
            fullStat = true;
            selectedColumns = tbl.getColumns();
        } else {
            fullStat = false;
            selectedColumns = filterColumns(tbl.getColumns(), colNames);
        }

        Collection<ObjectPartitionStatistics> partsStats = collectPartitionStatistics(tbl, selectedColumns);
        statsRepos.saveLocalPartitionsStatistics(tbl.identifier(), partsStats, fullStat);

        ObjectStatistics tblStats = aggregateLocalStatistics(tbl, selectedColumns, partsStats);
        // TODO support refreshing only part of columns (step to columnar storage and ability to handle lack of some stats)
        statsRepos.saveLocalStatistics(tbl.identifier(), tblStats, fullStat);
    }

    private Collection<ObjectPartitionStatistics> collectPartitionStatistics(GridH2Table tbl, Column[] selectedColumns) throws IgniteCheckedException {
        List<ObjectPartitionStatistics> tblPartStats = new ArrayList<>();
        GridH2RowDescriptor desc = tbl.rowDescriptor();

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
                    // TODO: verify that row belongs to the table, possibly its better to use table scan index here
                    // tbl.getScanIndex(null)...

                    rowsCnt++;

                    H2Row row0 = desc.createRow(row);

                    for (ColumnStatisticsCollector colStat : colStatsCollectors)
                        colStat.add(row0.getValue(colStat.col().getColumnId()));

                }

                long rowsCnt0 = rowsCnt;

                Map<String, ColumnStatistics> colStats = colStatsCollectors.stream().collect(Collectors.toMap(
                        csc -> csc.col().getName(), csc -> csc.finish(rowsCnt0)
                ));


                tblPartStats.add(new ObjectPartitionStatistics(locPart.id(), true, rowsCnt, locPart.updateCounter(),
                        colStats));
            }
            finally {
                if (reserved)
                    locPart.release();
            }
        }

        return tblPartStats;
    }

    private ObjectStatistics aggregateLocalStatistics(GridH2Table tbl, Column[] selectedColumns,
                                                      Collection<ObjectPartitionStatistics> tblPartStats) {

        Map<Column, List<ColumnStatistics>> colPartStats = new HashMap<>(selectedColumns.length);
        Map<Column, Long> colRowCounter = new HashMap<>(selectedColumns.length);
        for(Column col : selectedColumns) {
            colPartStats.put(col, new ArrayList<>());
            colRowCounter.put(col, 0L);
        }

        QueryTable tblId = tbl.identifier();

        for (ObjectPartitionStatistics partStat : tblPartStats)
            for (Column col : selectedColumns) {
                ColumnStatistics colPartStat = partStat.columnStatistics(col.getName());
                if (colPartStat != null) {
                    colPartStats.compute(col, (k, v) -> {
                        v.add(colPartStat);
                        return v;
                    });
                    colRowCounter.compute(col, (k, v) -> v + partStat.rowCount());
                }
            }

        Map<String, ColumnStatistics> colStats = new HashMap<>(selectedColumns.length);
        for(Column col : selectedColumns) {
            colStats.put(col.getName(), ColumnStatisticsCollector.aggregate(
                    tbl::compareValues, colRowCounter.get(col), colPartStats.get(col)));
        }

        long rowCnt = colRowCounter.values().stream().max(Long::compare).orElse(0L);

        ObjectStatistics tblStats = new ObjectStatistics(rowCnt, colStats);

        return tblStats;
    }
}
