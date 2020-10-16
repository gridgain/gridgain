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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.ignite.IgniteAuthenticationException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.query.QueryTable;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.IgniteStatisticsRepositoryImpl;
import org.apache.ignite.internal.processors.query.h2.SchemaManager;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2RowDescriptor;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.h2.opt.H2Row;
import org.gridgain.internal.h2.table.Column;

import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.MOVING;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.OWNING;

public class IgniteStatisticsManagerImpl implements  IgniteStatisticsManager {

    /** Logger. */
    private IgniteLogger log;

    private final GridKernalContext ctx;
    private final SchemaManager schemaMgr;

    private final IgniteStatisticsRepository statsRepos;

    public IgniteStatisticsManagerImpl(GridKernalContext ctx, SchemaManager schemaMgr) {
        this.ctx = ctx;
        this.schemaMgr = schemaMgr;

        log = ctx.log(IgniteStatisticsManagerImpl.class);
        statsRepos = new IgniteStatisticsRepositoryImpl(ctx);
    }
    public IgniteStatisticsRepository statisticsRepository() {
        return statsRepos;
    }

    @Override public ObjectStatistics getLocalStatistics(String schemaName, String objName) {
        return statsRepos.getLocalStatistics(new QueryTable(schemaName, objName));
    }

    @Override public void clearObjectStatistics(String schemaName, String objName, String... colNames) {
        QueryTable tbl = new QueryTable(schemaName, objName);
        statsRepos.clearLocalPartitionsStatistics(tbl, colNames);
        statsRepos.clearLocalStatistics(tbl, colNames);
        statsRepos.clearGlobalStatistics(tbl, colNames);
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

    @Override public void collectObjectStatistics(String schemaName, String objName, String ... colNames)
            throws IgniteCheckedException {
        GridH2Table tbl = schemaMgr.dataTable(schemaName, objName);
        if (tbl == null)
            throw new IgniteAuthenticationException(String.format("Can't find table %s.%s", schemaName, objName));

        if (log.isDebugEnabled())
            log.debug(String.format("Starting statistics collection by %s.%s object", schemaName, objName));

        Column[] selectedColumns;
        boolean fullStat;
        if (colNames == null || colNames.length == 0) {
            fullStat = true;
            selectedColumns = tbl.getColumns();
        } else {
            fullStat = false;
            selectedColumns = filterColumns(tbl.getColumns(), colNames);
        }

        Collection<ObjectPartitionStatisticsImpl> partsStats = collectPartitionStatistics(tbl, selectedColumns);
        statsRepos.saveLocalPartitionsStatistics(tbl.identifier(), partsStats, fullStat);

        ObjectStatisticsImpl tblStats = aggregateLocalStatistics(tbl, selectedColumns, partsStats);
        statsRepos.saveLocalStatistics(tbl.identifier(), tblStats, fullStat);
        if (log.isDebugEnabled())
            log.debug(String.format("Statistics collection by %s.%s object is finished.", schemaName, objName));
    }

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


                tblPartStats.add(new ObjectPartitionStatisticsImpl(locPart.id(), true, rowsCnt, locPart.updateCounter(),
                        colStats));
            }
            finally {
                if (reserved)
                    locPart.release();
            }
        }

        return tblPartStats;
    }

    public ObjectStatisticsImpl aggregateLocalStatistics(QueryTable tbl, Collection<ObjectPartitionStatisticsImpl> tblPartStats) {

        GridH2Table table = schemaMgr.dataTable(tbl.schema(), tbl.table());
        if (table == null) {
            // remove all loaded statistics.
            log.info("Removing statistics for table " + tbl + " cause table doesn't exists.");
            statsRepos.clearLocalPartitionsStatistics(tbl);
        }
        return aggregateLocalStatistics(table, table.getColumns(), tblPartStats);
    }

    private ObjectStatisticsImpl aggregateLocalStatistics(GridH2Table tbl, Column[] selectedColumns,
                                                          Collection<ObjectPartitionStatisticsImpl> tblPartStats) {

        Map<Column, List<ColumnStatistics>> colPartStats = new HashMap<>(selectedColumns.length);
        long rowCnt = 0;
        for(Column col : selectedColumns)
            colPartStats.put(col, new ArrayList<>());

        QueryTable tblId = tbl.identifier();

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
        for(Column col : selectedColumns) {
            ColumnStatistics stat = ColumnStatisticsCollector.aggregate(tbl::compareValues, colPartStats.get(col));
            colStats.put(col.getName(), stat);
        }



        ObjectStatisticsImpl tblStats = new ObjectStatisticsImpl(rowCnt, colStats);

        return tblStats;
    }

}
