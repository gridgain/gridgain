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

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.processors.cache.query.QueryTable;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.h2.value.Value;

public class SqlStatisticsManager {
    private final Map<QueryTable, Map<Integer, TablePartitionStatistics>> partitionedTblStats = new ConcurrentHashMap<>();

    private final Map<QueryTable, TableStatistics> locTblStats = new ConcurrentHashMap<>();

    public TableStatistics localTableStatistic(String schema, String tblName) {
        return locTblStats.get(new QueryTable(schema, tblName));
    }

    public synchronized void updateLocalStats(GridH2Table tbl, Collection<TablePartitionStatistics> partStats) {
        QueryTable tblId = tbl.identifier();

        Map<Integer, TablePartitionStatistics> tblParts = partitionedTblStats.computeIfAbsent(tblId, k -> new HashMap<>());

        for (TablePartitionStatistics part : partStats)
            tblParts.put(part.partId(), part);

        aggregateLocalStatistics(tbl);

        tbl.tableStatistics(locTblStats.get(tblId));
    }

    private void aggregateLocalStatistics(GridH2Table tbl) {
        QueryTable tblId = tbl.identifier();

        if (!partitionedTblStats.containsKey(tblId))
            return;

        long rowCount = 0;

        Map<String, ColumnStatisticsAggregator> colStats = new HashMap<>();

        for (TablePartitionStatistics part : partitionedTblStats.get(tblId).values()) {
            if (!part.local())
                continue;

            rowCount += part.rowCount();

            for (Map.Entry<String, ColumnStatistics> colNameToStat : part.getColNameToStat().entrySet()) {
                ColumnStatisticsAggregator statAggregator = colStats.computeIfAbsent(colNameToStat.getKey(),
                    k -> new ColumnStatisticsAggregator(tbl::compareValues));

                statAggregator.collect(colNameToStat.getValue());
            }
        }

        Map<String, ColumnStatistics> aggregatedStats = new HashMap<>(colStats.size());

        for (Map.Entry<String, ColumnStatisticsAggregator> nameToAggr : colStats.entrySet())
            aggregatedStats.put(nameToAggr.getKey(), nameToAggr.getValue().finish());

        locTblStats.put(tblId, new TableStatistics(rowCount, aggregatedStats));
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
