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
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetastorageLifecycleListener;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.ReadOnlyMetastorage;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.ReadWriteMetastorage;
import org.apache.ignite.internal.processors.cache.query.QueryTable;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2RowDescriptor;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.h2.opt.H2Row;
import org.h2.table.Column;
import org.h2.value.Value;

import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.MOVING;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.OWNING;

public class SqlStatisticsManager implements MetastorageLifecycleListener {

    private final static String META_SEPARATOR = ".";
    private final static String META_STAT_PREFIX = "data_stats";
    private final static String META_TABLE_STAT_PREX = META_STAT_PREFIX + "_tbl";
    private final static String META_PART_STAT_PREX = META_STAT_PREFIX + "_part";

    private final Map<QueryTable, Map<Integer, TablePartitionStatistics>> partitionedTblStats = new ConcurrentHashMap<>();

    private final Map<QueryTable, TableStatistics> locTblStats = new ConcurrentHashMap<>();

    private final GridKernalContext ctx;
    private ReadWriteMetastorage metastore;

    public SqlStatisticsManager(GridKernalContext ctx) {
        this.ctx = ctx;
        ctx.internalSubscriptionProcessor().registerMetastorageListener(this);
    }

    public void onReadyForRead(ReadOnlyMetastorage metastorage) throws IgniteCheckedException {
        metastorage.iterate(META_TABLE_STAT_PREX, (key, stat) -> {
            System.out.println(key + stat);
        },true);
    }

    public void onReadyForReadWrite(ReadWriteMetastorage metastorage) throws IgniteCheckedException {
        this.metastore = metastorage;
    }

    public TableStatistics localTableStatistic(String schema, String tblName) {
        return locTblStats.get(new QueryTable(schema, tblName));
    }

    public TableStatistics getLocalStatistics(QueryTable tbl) {
        return locTblStats.get(tbl);
    }

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

    public void collectTableStatistics(GridH2Table tbl, String ... colNames) throws IgniteCheckedException {
        assert tbl != null;

        GridH2RowDescriptor desc = tbl.rowDescriptor();

        Column[] selectedColumns = filterColumns(tbl.getColumns(), colNames);

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

                List<ColumnStatisticsCollector> colStatsCollected = new ArrayList<>(selectedColumns.length);

                for (Column col : selectedColumns)
                    colStatsCollected.add(new ColumnStatisticsCollector(col, tbl::compareValues));

                for (CacheDataRow row : tbl.cacheContext().offheap().cachePartitionIterator(tbl.cacheId(), locPart.id(),
                        null, true)) {
                    // TODO: verify that row belongs to the table, possibly its better to use table scan index here
                    // tbl.getScanIndex(null)...

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
        if (tbl.cacheInfo().affinityNode()) {
            tbl.getScanIndex(null);
        }


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
        //if (metastore != null)
            //TODO metastore.write(getMetaKey(tblId.schema(), tblId.table()), tblStats);
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

    private String getTblName(String metaKey) {
        int idx = metaKey.lastIndexOf(META_SEPARATOR) + 1;

        assert idx < metaKey.length();

        return metaKey.substring(idx);
    }

    private String getSchemaName(String metaKey) {
        int schemaIdx = metaKey.indexOf(META_SEPARATOR) + 1;
        int tableIdx = metaKey.lastIndexOf(META_SEPARATOR);

        return metaKey.substring(schemaIdx, tableIdx);

    }

    private String getMetaKey(String schema, String tblName) {
        return META_TABLE_STAT_PREX + META_SEPARATOR + schema + META_SEPARATOR + tblName;
    }

}
