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
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.query.QueryTable;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2RowDescriptor;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.h2.opt.H2Row;
import org.h2.table.Column;
import org.h2.value.Value;

import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.MOVING;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.OWNING;

public class SqlStatisticsManager {
    private final Map<QueryTable, Map<Integer, TablePartitionStatistics>> partitionedTblStats = new ConcurrentHashMap<>();

    private final Map<QueryTable, TableStatistics> locTblStats = new ConcurrentHashMap<>();

    private final GridKernalContext ctx;

    public SqlStatisticsManager(GridKernalContext ctx) {
        this.ctx = ctx;
    }

    public TableStatistics localTableStatistic(String schema, String tblName) {
        return locTblStats.get(new QueryTable(schema, tblName));
    }

    public void collectTableStatistics(GridH2Table tbl) throws IgniteCheckedException {
        assert tbl != null;

        GridH2RowDescriptor desc = tbl.rowDescriptor();

        List<TablePartitionStatistics> tblPartStats = new ArrayList<>();

        for (GridDhtLocalPartition locPart : tbl.cacheContext().topology().localPartitions()) {
            final boolean reserved = locPart.reserve();

            try {
                if (!reserved || (locPart.state() != OWNING && locPart.state() != MOVING)
                    || !locPart.primary(ctx.discovery().topologyVersionEx()))
                    continue;

                if (locPart.state() == MOVING)
                    tbl.cacheContext().preloader().syncFuture().get();

                long rowsCnt = 0;

                List<ColumnStatisticsCollector> colStatsCollected = new ArrayList<>(tbl.getColumns().length);

                for (Column col : tbl.getColumns())
                    colStatsCollected.add(new ColumnStatisticsCollector(col, tbl::compareValues));

                for (CacheDataRow row : tbl.cacheContext().offheap().partitionIterator(locPart.id())) {
                    // TODO: verify that row belongs to the table

                    rowsCnt++;

                    H2Row row0 = desc.createRow(row);

                    for (ColumnStatisticsCollector colStat : colStatsCollected)
                        colStat.add(row0.getValue(colStat.col().getColumnId()));

                }

                long rowsCnt0 = rowsCnt;

                Map<String, ColumnStatistics> colStats = colStatsCollected.stream().collect(Collectors.toMap(
                    csc -> csc.col().getName(), csc -> csc.finish(rowsCnt0)
                ));

                tblPartStats.add(new TablePartitionStatistics(locPart.id(), true, rowsCnt, colStats));
            }
            finally {
                if (reserved)
                    locPart.release();
            }
        }

        QueryTable tblId = tbl.identifier();

        Map<Integer, TablePartitionStatistics> tblParts = partitionedTblStats.computeIfAbsent(tblId, k -> new HashMap<>());

        for (TablePartitionStatistics part : tblPartStats)
            tblParts.put(part.partId(), part);

        aggregateLocalStatistics(tbl);
    }

    private void aggregateLocalStatistics(GridH2Table tbl) {
        QueryTable tblId = tbl.identifier();

        if (!partitionedTblStats.containsKey(tblId))
            return;

        long rowCnt = 0;

        Map<String, ColumnStatisticsAggregator> colStats = new HashMap<>();

        for (TablePartitionStatistics part : partitionedTblStats.get(tblId).values()) {
            if (!part.local())
                continue;

            rowCnt += part.rowCount();

            for (Map.Entry<String, ColumnStatistics> colNameToStat : part.getColNameToStat().entrySet()) {
                ColumnStatisticsAggregator statAggregator = colStats.computeIfAbsent(colNameToStat.getKey(),
                    k -> new ColumnStatisticsAggregator(tbl::compareValues));

                statAggregator.collect(colNameToStat.getValue());
            }
        }

        Map<String, ColumnStatistics> aggregatedStats = new HashMap<>(colStats.size());

        for (Map.Entry<String, ColumnStatisticsAggregator> nameToAggr : colStats.entrySet())
            aggregatedStats.put(nameToAggr.getKey(), nameToAggr.getValue().finish());

        TableStatistics tblStats = new TableStatistics(rowCnt, aggregatedStats);

        locTblStats.put(tblId, tblStats);

        tbl.tableStatistics(tblStats);
    }

    private static class ColumnStatisticsAggregator {
        private final Comparator<Value> cmp;

        public ColumnStatisticsAggregator(Comparator<Value> cmp) {
            this.cmp = cmp;
        }

        private Value min;

        private Value max;

        private int nulls;

        private int selectivity;

        private int statsProcessed;

        void collect(ColumnStatistics stat) {
            statsProcessed++;

            nulls += stat.nulls();

            selectivity += stat.selectivity();

            if (stat.min() != null && (min == null || cmp.compare(stat.min(), min) < 0))
                min = stat.min();

            if (stat.max() != null && (max == null || cmp.compare(stat.max(), max) > 0))
                max = stat.max();
        }

        ColumnStatistics finish() {
            return new ColumnStatistics(min, max, nulls, selectivity / statsProcessed);
        }
    }
}
