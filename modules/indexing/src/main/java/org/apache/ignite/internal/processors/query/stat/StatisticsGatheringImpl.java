package org.apache.ignite.internal.processors.query.stat;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
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
import org.apache.ignite.thread.IgniteThreadPoolExecutor;
import org.gridgain.internal.h2.table.Column;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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

    /** Statistics request router. */
    private final StatisticsGatheringRequestRouter statRouter;

    /** Ignite Thread pool executor to do statistics collection tasks. */
    private final IgniteThreadPoolExecutor gatMgmtPool;


    /**
     * Constructor.
     *
     * @param schemaMgr Schema manager.
     * @param discoMgr Discovery manager.
     * @param qryProcessor Query processor.
     * @param statRouter Statistics request router.
     * @param gatMgmtPool Thread pool to gather statistics in.
     * @param logSupplier Log supplier function.
     */
    public StatisticsGatheringImpl(
        SchemaManager schemaMgr,
        GridDiscoveryManager discoMgr,
        GridQueryProcessor qryProcessor,
        StatisticsGatheringRequestRouter statRouter,
        IgniteThreadPoolExecutor gatMgmtPool,
        Function<Class<?>, IgniteLogger> logSupplier
    ) {
        this.log = logSupplier.apply(StatisticsGatheringImpl.class);
        this.schemaMgr = schemaMgr;
        this.discoMgr = discoMgr;
        this.qryProcessor = qryProcessor;
        this.statRouter = statRouter;
        this.gatMgmtPool = gatMgmtPool;
    }

    public Collection<ObjectPartitionStatisticsImpl> collectLocalObjectStatistics(
            StatisticsKeyMessage keyMsg,
            int[] partIds,
            Supplier<Boolean> cancelled
    ) throws IgniteCheckedException {
        GridH2Table tbl = schemaMgr.dataTable(keyMsg.schema(), keyMsg.obj());
        if (tbl == null)
            throw new IgniteCheckedException(String.format("Can't find table %s.%s", keyMsg.schema(), keyMsg.obj()));

        Column[] selectedCols = filterColumns(tbl.getColumns(), keyMsg.colNames());

        return collectPartitionStatistics(tbl, partIds, selectedCols, cancelled);
        // Collection<ObjectPartitionStatisticsImpl> partsStats =
        /*if (partsStats == null) {
            assert cancelled.get() : "Error collecting partition level statistics.";

            return partsStats;
        }*/

        // TODO! Move back to manager
        //sendPartitionStatisticsToBackupNodes(tbl, partsStats);

        //StatisticsKey key = new StatisticsKey(keyMsg.schema(), keyMsg.obj());
        //statsRepos.saveLocalPartitionsStatistics(key, partsStats);

        //ObjectStatisticsImpl tblStats = aggregateLocalStatistics(tbl, selectedCols, partsStats);
        //statsRepos.mergeLocalStatistics(key, tblStats);
// partsStats.stream().map(ObjectPartitionStatisticsImpl::partId)
//                .mapToInt(Integer::intValue).toArray()

        // return partsStats;
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
     * Collect partition level statistics.
     *
     * @param tbl Table to collect statistics by.
     * @param partIds Array of partition ids to collect statistics by.
     * @param selectedCols Columns to collect statistics by.
     * @param cancelled Supplier to check if collection was cancelled.
     * @return Collection of partition level statistics by local primary partitions.
     * @throws IgniteCheckedException in case of error.
     */
    private Collection<ObjectPartitionStatisticsImpl> collectPartitionStatistics(
            GridH2Table tbl,
            int[] partIds,
            Column[] selectedCols,
            Supplier<Boolean> cancelled
    ) {
        List<ObjectPartitionStatisticsImpl> tblPartStats = new ArrayList<>();
        GridH2RowDescriptor desc = tbl.rowDescriptor();
        String tblName = tbl.getName();
        GridDhtPartitionTopology top = tbl.cacheContext().topology();
        AffinityTopologyVersion topVer = top.readyTopologyVersion();

        for (int partId : partIds) {
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

                tblPartStats.add(new ObjectPartitionStatisticsImpl(locPart.id(), true, rowsCnt,
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

        return tblPartStats;
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

            // Just to double check
            //statsRepos.clearLocalPartitionsStatistics(new StatisticsKey(keyMsg.schema(), keyMsg.obj()));
        }

        return aggregateLocalStatistics(tbl, filterColumns(tbl.getColumns(), keyMsg.colNames()), stats);
    }


    /** {@inheritDoc} */
    @Override public void collectLocalObjectsStatisticsAsync(
        UUID reqId,
        Map<StatisticsKeyMessage, int[]> keysParts,
        Supplier<Boolean> cancelled
    ) {
        gatMgmtPool.submit(() -> collectLocalObjectsStatistics(reqId, keysParts, cancelled));
    }

    /**
     * Collect local statistics by specified keys and partitions.
     *
     * @param reqId Request id.
     * @param keysParts Keys to collect statistics by.
     * @return Collected statistics.
     */
    public void collectLocalObjectsStatistics(
            UUID reqId,
            Map<StatisticsKeyMessage, int[]> keysParts,
            Supplier<Boolean> cancelled
    ) {
        Map<ObjectStatisticsImpl, int[]> res = new HashMap<>();
        for (Map.Entry<StatisticsKeyMessage, int[]> keyParts : keysParts.entrySet()) {
            try {
                Collection<ObjectPartitionStatisticsImpl> partsStat = collectLocalObjectStatistics(keyParts.getKey(),
                    keyParts.getValue(), cancelled);
                ObjectStatisticsImpl localStat = aggregateLocalStatistics(keyParts.getKey(), partsStat);
                res.put(localStat, partsStat.stream().map(ObjectPartitionStatisticsImpl::partId)
                    .mapToInt(Integer::intValue).toArray());
            } catch (IgniteCheckedException e) {
                if (log.isDebugEnabled())
                    log.debug(String.format("Unable to collect local object statistics by key %s due to $s",
                            keyParts.getKey(), e.getMessage()));
            }
        }
        statRouter.se
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
}
