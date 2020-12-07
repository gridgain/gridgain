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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheUtils;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.h2.SchemaManager;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2RowDescriptor;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.h2.opt.H2Row;
import org.apache.ignite.internal.processors.query.h2.twostep.ReducePartitionMapper;
import org.apache.ignite.internal.processors.query.stat.messages.StatsClearRequestMessage;
import org.apache.ignite.internal.processors.query.stat.messages.StatsCollectionCancelRequestMessage;
import org.apache.ignite.internal.processors.query.stat.messages.StatsCollectionRequestMessage;
import org.apache.ignite.internal.processors.query.stat.messages.StatsColumnData;
import org.apache.ignite.internal.processors.query.stat.messages.StatsKeyMessage;
import org.apache.ignite.internal.processors.query.stat.messages.StatsObjectData;
import org.apache.ignite.internal.processors.query.stat.messages.StatsPropagationMessage;
import org.apache.ignite.internal.processors.query.stat.messages.StatsRequestMessage;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;
import org.gridgain.internal.h2.table.Column;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.MOVING;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.OWNING;

/**
 * Statistics manager implementation.
 */
public class IgniteStatisticsManagerImpl implements IgniteStatisticsManager, GridMessageListener {
    /** Statistics related messages topic name. */
    private static final Object TOPIC = GridTopic.TOPIC_CACHE.topic("statistics");

    /** Size of statistics collection pool. */
    private static final int STATS_POOL_SIZE = 1;

    /** Node left listener to complete statistics collection tasks without left nodes. */
    private final NodeLeftListener nodeLeftLsnr;

    /** Logger. */
    private final IgniteLogger log;

    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Schema manager. */
    private final SchemaManager schemaMgr;

    /** Statistics repository. */
    private final IgniteStatisticsRepository statsRepos;

    /** Ignite Thread pool executor to do statistics collection tasks. */
    private final IgniteThreadPoolExecutor statMgmtPool;

    /** Current collections, reqId to collection status map. */
    private final Map<UUID, StatCollectionStatus> currCollections = new ConcurrentHashMap<>();

    /** */
    private ReducePartitionMapper mapper;

    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     * @param schemaMgr Schema manager.
     */
    public IgniteStatisticsManagerImpl(GridKernalContext ctx, SchemaManager schemaMgr) {
        this.ctx = ctx;
        this.schemaMgr = schemaMgr;

        log = ctx.log(IgniteStatisticsManagerImpl.class);

        ctx.io().addMessageListener(TOPIC, this);

        boolean storeData = !(ctx.config().isClientMode() || ctx.isDaemon());

        IgniteCacheDatabaseSharedManager db = (GridCacheUtils.isPersistenceEnabled(ctx.config())) ?
                ctx.cache().context().database() : null;

        statsRepos = new IgniteStatisticsRepositoryImpl(storeData, db, ctx.internalSubscriptionProcessor(), this,
                ctx::log);

        mapper = new ReducePartitionMapper(ctx, log);
        nodeLeftLsnr = new NodeLeftListener();

        statMgmtPool = new IgniteThreadPoolExecutor("dr-mgmt-pool",
                ctx.igniteInstanceName(),
                0,
                STATS_POOL_SIZE,
                IgniteConfiguration.DFLT_THREAD_KEEP_ALIVE_TIME,
                new LinkedBlockingQueue<>(),
                GridIoPolicy.UNDEFINED,
                ctx.uncaughtExceptionHandler()
        );
        ctx.event().addLocalEventListener(nodeLeftLsnr, EventType.EVT_NODE_FAILED, EventType.EVT_NODE_LEFT);
    }

    /**
     * @return Statistics repository.
     */
    public IgniteStatisticsRepository statisticsRepository() {
        return statsRepos;
    }

    /** {@inheritDoc} */
    @Override public ObjectStatistics getLocalStatistics(String schemaName, String objName) {
        return statsRepos.getLocalStatistics(new StatsKey(schemaName, objName));
    }

    /** {@inheritDoc} */
    @Override public void clearObjectStatistics(
        String schemaName,
        String objName,
        String... colNames
    ) throws IgniteCheckedException {
        final StatCollectionFutureAdapter doneFut = new StatCollectionFutureAdapter();

        UUID reqId = UUID.randomUUID();
        StatsKey key = new StatsKey(schemaName, objName);

        Collection<UUID> requestNodes = null;
        try {
            requestNodes = nodes(extractGroups(Collections.singletonList(key)));
        } catch (IgniteCheckedException e) {
            // TODO: handle & remove task
        }
        StatsKeyMessage keyMsg = StatisticsUtils.toMessage(schemaName, objName, colNames);
        StatsClearRequestMessage req = new StatsClearRequestMessage(reqId, false, Collections.singletonList(keyMsg));

        sendLocalRequests(reqId, req, requestNodes);

        clearObjectStatisticsLocal(keyMsg);
    }

    /**
     * Actually clear local object statistics by the given key.
     *
     * @param keyMsg Key to clear statistics by.
     */
    private void clearObjectStatisticsLocal(StatsKeyMessage keyMsg) {
        StatsKey key = new StatsKey(keyMsg.schema(), keyMsg.obj());
        String[] colNames = keyMsg.colNames().toArray(new String[0]);

        statsRepos.clearLocalPartitionsStatistics(key, colNames);
        statsRepos.clearLocalStatistics(key, colNames);
        statsRepos.clearGlobalStatistics(key, colNames);
    }

    /**
     * Collect local object statistics by primary partitions of specified object.
     *
     * @param keyMsg Statistic key message to collect statistics by.
     * @param cancelled Supplier to check if operation was cancelled.
     * @throws IgniteCheckedException
     */
    private ObjectStatisticsImpl collectLocalObjectStatistics(
        StatsKeyMessage keyMsg,
        Supplier<Boolean> cancelled
    ) throws IgniteCheckedException {
        GridH2Table tbl = schemaMgr.dataTable(keyMsg.schema(), keyMsg.obj());
        if (tbl == null)
            throw new IgniteCheckedException(String.format("Can't find table %s.%s", keyMsg.schema(), keyMsg.obj()));

        Column[] selectedCols = filterColumns(tbl.getColumns(), keyMsg.colNames());

        Collection<ObjectPartitionStatisticsImpl> partsStats = collectPartitionStatistics(tbl, selectedCols, cancelled);
        sendPartStatsToBU(tbl, partsStats);

        if (partsStats == null) {
            assert cancelled.get() : "Error collecting partition level statistics.";

            return null;
        }
        StatsKey key = new StatsKey(keyMsg.schema(), keyMsg.obj());
        statsRepos.saveLocalPartitionsStatistics(key, partsStats);

        ObjectStatisticsImpl tblStats = aggregateLocalStatistics(tbl, selectedCols, partsStats);
        if (F.isEmpty(keyMsg.colNames()))
            statsRepos.saveLocalStatistics(key, tblStats);
        else
            statsRepos.mergeLocalStatistics(key, tblStats);

        return tblStats;
    }

    /**
     * Send statistics propagation messages with partition statistics to all backups node.
     *
     * @param tbl Table to which statistics should be send.
     * @param objStats Collection of partition statistics to send.
     * @throws IgniteCheckedException In case of errors.
     */
    private void sendPartStatsToBU(
        GridH2Table tbl,
        Collection<ObjectPartitionStatisticsImpl> objStats
    ) throws IgniteCheckedException {
        UUID locNode = ctx.discovery().localNode().id();
        StatsKey key = new StatsKey(tbl.identifier().schema(), tbl.identifier().table());
        GridDhtPartitionTopology topology = tbl.cacheContext().topology();
        Map<UUID, List<StatsObjectData>> statsByNodes = new HashMap<>();
        for (ObjectPartitionStatisticsImpl stat : objStats) {
            StatsObjectData statData = StatisticsUtils.toMessage(key, StatsType.PARTITION, stat);
            for (ClusterNode partNode : topology.nodes(stat.partId(), topology.readyTopologyVersion())) {
                if (locNode.equals(partNode.id()))
                    continue;

                statsByNodes.compute(partNode.id(), (k, v) -> {
                    if (v == null)
                        v = new ArrayList<>();

                    v.add(statData);

                    return v;
                });
            }
        }

        for (Map.Entry <UUID, List<StatsObjectData>> statToNode : statsByNodes.entrySet()) {
            StatsPropagationMessage nodeMsg = StatisticsUtils.toMessage(null, statToNode.getValue());

            ctx.io().sendToCustomTopic(statToNode.getKey(), TOPIC, nodeMsg, GridIoPolicy.QUERY_POOL);
        }
    }

    /**
     * Group local statistics by table and columns.
     *
     * @param status statistics collection status with all necessary local statistics.
     * @return map of tables to columns to list of local column statistics.
     */
    private Map<GridH2Table, Map<String, List<ColumnStatistics>>> groupColumnStatistics(StatCollectionStatus status) {
        Map<GridH2Table, Map<String, List<ColumnStatistics>>> result = new HashMap<>();
        for (StatsPropagationMessage msg : status.locStatistics) {
            for (StatsObjectData msgData : msg.data()) {
                GridH2Table dataTable = schemaMgr.dataTable(msgData.key().schema(), msgData.key().obj());
                if (dataTable == null) {
                    log.warning(String.format("Can't find table for received statistics: %s %s", msgData.key().schema(),
                            msgData.key().obj()));

                    continue;
                }

                result.compute(dataTable, (table, columns) -> {
                    if (columns == null)
                        columns = new HashMap<>();

                    for (Map.Entry<String, StatsColumnData> colData : msgData.data().entrySet())
                        columns.compute(colData.getKey(), (colName, colStatistics) -> {
                            if (colStatistics == null)
                                colStatistics = new ArrayList<>();

                            ColumnStatistics colStat;
                            try {
                                colStat = StatisticsUtils.toColumnStatistics(ctx, colData.getValue());
                                colStatistics.add(colStat);
                            }
                            catch (IgniteCheckedException e) {
                                log.warning(String. format("Error while converting column statistics %s.%s - %s",
                                        msgData.key().schema(), msgData.key().obj(), colName));
                            }
                            return colStatistics;
                        });
                    return columns;
                });
            }
        }
        return result;
    }

    /**
     * Will aggregate all received local statistics and save it.
     *
     * @param request aggregation request with id only.
     */
    private void aggregateCollected(StatCollectionStatus status) {
        // Table -> Column -> List of local column stats
        Map<GridH2Table, Map<String, List<ColumnStatistics>>> tablesData = groupColumnStatistics(status);

        Map<ClusterNode, List<StatsObjectData>> dataByNodes = new HashMap<>();
        for (Map.Entry<GridH2Table, Map<String, List<ColumnStatistics>>> tableStats : tablesData.entrySet()) {
            GridH2Table table = tableStats.getKey();
            long rowCount = 0L;
            Map<String, ColumnStatistics> columnStatistics = new HashMap<>();
            for (Map.Entry<String, List<ColumnStatistics>> columnStats : tableStats.getValue().entrySet()) {
                ColumnStatistics globalStatistics = ColumnStatisticsCollector
                        .aggregate(table::compareValues, columnStats.getValue());
                if (rowCount < globalStatistics.total())
                    rowCount = globalStatistics.total();
                columnStatistics.put(columnStats.getKey(), globalStatistics);
            }

            ObjectStatisticsImpl objectStatistics = new ObjectStatisticsImpl(rowCount, columnStatistics);
            statsRepos.saveGlobalStatistics(new StatsKey(table.identifier().schema(), table.identifier().table()),
                    objectStatistics);
        }
        status.doneFut.onDone();
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

        Set<String> colNamesSet = new HashSet(Arrays.asList(colNames));
        List<Column> resList = new ArrayList<>(colNames.size());

        for (Column col : cols)
            if (colNamesSet.contains(col.getName()))
                resList.add(col);

        return resList.toArray(new Column[resList.size()]);
    }

    /** {@inheritDoc}
     * @return*/
    @Override public void collectObjectStatistics(
        String schemaName,
        String objName,
        String... colNames
    ) throws IgniteCheckedException {
        final StatCollectionFutureAdapter doneFut = new StatCollectionFutureAdapter();

        UUID reqId = UUID.randomUUID();
        StatsKey key = new StatsKey(schemaName, objName);

        Collection<UUID> reqNodes = null;
        try {
            reqNodes = nodes(extractGroups(Collections.singletonList(key)));
        } catch (IgniteCheckedException e) {
            // TODO: handle & remove task
        }
        currCollections.put(reqId, new StatCollectionStatus(reqId, reqNodes, doneFut));
        StatsCollectionRequestMessage req = new StatsCollectionRequestMessage(reqId, true,
                Collections.singletonList(StatisticsUtils.toMessage(schemaName, objName, colNames)));

        sendLocalRequests(reqId, req, reqNodes);

        UUID locNode = ctx.discovery().localNode().id();
        statMgmtPool.submit(() -> processLocal(locNode, req));
        doneFut.get();
    }

    /**
     * TODO
     */
    public void stop() {
        if (statMgmtPool != null) {
            List<Runnable> unfinishedTasks = statMgmtPool.shutdownNow();
            if (!unfinishedTasks.isEmpty())
                log.warning(String.format("%d statistics collection request cancelled.", unfinishedTasks.size()));
        }
    }

    /**
     * Collect partition level statistics.
     *
     * @param tbl Table to collect statistics by.
     * @param selectedCols Columns to collect statistics by.
     * @param cancelled Supplier to check if collection was cancelled.
     * @return Collection of partition level statistics by local primary partitions.
     * @throws IgniteCheckedException in case of error.
     */
    private Collection<ObjectPartitionStatisticsImpl> collectPartitionStatistics(
            GridH2Table tbl,
            Column[] selectedCols,
            Supplier<Boolean> cancelled
    ) throws IgniteCheckedException {
        List<ObjectPartitionStatisticsImpl> tblPartStats = new ArrayList<>();
        GridH2RowDescriptor desc = tbl.rowDescriptor();
        String tblName = tbl.getName();

        for (GridDhtLocalPartition locPart : tbl.cacheContext().topology().localPartitions()) {
            if (cancelled.get()) {
                if (log.isInfoEnabled())
                    log.info(String.format("Canceled collection of object %s.%s statistics.", tbl.identifier().schema(),
                        tbl.identifier().table()));

                return null;
            }

            final boolean reserved = locPart.reserve();

            try {
                if (!reserved || (locPart.state() != OWNING && locPart.state() != MOVING)
                        || !locPart.primary(ctx.discovery().topologyVersionEx()))
                    continue;

                if (locPart.state() == MOVING)
                    tbl.cacheContext().preloader().syncFuture().get();

                long rowsCnt = 0;

                List<ColumnStatisticsCollector> colStatsCollectors = new ArrayList<>(selectedCols.length);

                for (Column col : selectedCols)
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

                if (log.isTraceEnabled())
                    log.trace(String.format("Finished statistics collection on %s.%s:%d",
                            tbl.identifier().schema(), tbl.identifier().table(), locPart.id()));
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
     * @param key Aggregation key.
     * @param tblPartStats Collection of all local partition level statistics by specified key.
     * @return Local level aggregated statistics.
     */
    public ObjectStatisticsImpl aggregateLocalStatistics(
            StatsKey key,
            Collection<ObjectPartitionStatisticsImpl> tblPartStats
    ) {
        // For now there can be only tables
        GridH2Table tbl = schemaMgr.dataTable(key.schema(), key.obj());

        if (tbl == null) {
            // remove all loaded statistics.
            if (log.isDebugEnabled())
                log.debug(String.format("Removing statistics for object %s.%s cause table doesn't exists.",
                        key.schema(), key.obj()));

            statsRepos.clearLocalPartitionsStatistics(key);
        }
        return aggregateLocalStatistics(tbl, tbl.getColumns(), tblPartStats);
    }

    /**
     * Aggregate partition level statistics to local level one.
     *
     * @param tbl Table to aggregate statistics by.
     * @param selectedCols Columns to aggregate statistics by.
     * @param tblPartStats Collection of partition level statistics.
     * @return Local level statistics.
     */
    private ObjectStatisticsImpl aggregateLocalStatistics(
            GridH2Table tbl,
            Column[] selectedCols,
            Collection<ObjectPartitionStatisticsImpl> tblPartStats
    ) {
        Map<Column, List<ColumnStatistics>> colPartStats = new HashMap<>(selectedCols.length);
        long rowCnt = 0;
        for (Column col : selectedCols)
            colPartStats.put(col, new ArrayList<>());

        for (ObjectPartitionStatisticsImpl partStat : tblPartStats) {
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
     * Receive and handle statistics propagation message.
     *
     * @param nodeId Sender node id.
     * @param msg Statistics propagation message to handle.
     * @throws IgniteCheckedException In case of errors.
     */
    private void receiveStatistics(UUID nodeId, StatsPropagationMessage msg) throws IgniteCheckedException {
        if (msg.reqId() == null)
            receivePartitionsStatistics(nodeId, msg);
        else
            receiveLocalStatistics(nodeId, msg);
    }

    /**
     * Receive and handle statistics propagation message with partitions statistics.
     *
     * @param nodeId Sender node id.
     * @param msg Statistics propagation message with partitions statistics to handle.
     * @throws IgniteCheckedException In case of errors.
     */
    private void receivePartitionsStatistics(UUID nodeId, StatsPropagationMessage msg) throws IgniteCheckedException {
        UUID locNode = ctx.discovery().localNode().id();
        for (StatsObjectData partData : msg.data()) {
            StatsKey key = new StatsKey(partData.key().schema(), partData.key().obj());

            assert partData.type() == StatsType.PARTITION : "Got non partition level statistics by " + key
                    + " without request";

            if (log.isTraceEnabled())
                log.trace(String.format("Received partition statistics %s.%s:%d from node %s", key.schema(),
                        key.obj(), partData.partId(), nodeId));

            GridH2Table tbl = schemaMgr.dataTable(key.schema(), key.obj());
            if (tbl == null) {
                if (log.isInfoEnabled())
                    log.info(String.format("Ignoring outdated partition statistics %s.%s:%d from node %s",
                            key.schema(), key.obj(), partData.partId(), nodeId));

                continue;
            }
            GridDhtPartitionState partState = tbl.cacheContext().topology().partitionState(locNode, partData.partId());
            if (partState != OWNING) {
                if (log.isInfoEnabled())
                    log.info(String.format("Ignoring non local partition statistics %s.%s:%d from node %s",
                            key.schema(), key.obj(), partData.partId(), nodeId));

                continue;
            }

            ObjectPartitionStatisticsImpl opStat = StatisticsUtils.toObjectPartitionStatistics(ctx, partData);

            statsRepos.saveLocalPartitionStatistics(key, opStat);
        }
    }

    /**
     * Receive and handle statistics propagation message as response for collection request.
     *
     * @param nodeId Sender node id.
     * @param msg Statistics propagation message with partitions statistics to handle.
     * @throws IgniteCheckedException In case of errors.
     */
    private void receiveLocalStatistics(UUID nodeId, StatsPropagationMessage msg) {
        assert msg.data().stream().noneMatch(pd -> pd.type() == StatsType.PARTITION)
                : "Got partition statistics by request " + msg.reqId();

        currCollections.compute(msg.reqId(), (k, v) -> {
            if (v == null) {
                if (log.isInfoEnabled())
                    log.info(String.format("Ignoring outdated local statistics collection response from node %s to req %s",
                            nodeId, msg.reqId()));

                return null;
            }

            assert msg.reqId().equals(v.reqId);

            boolean removed = v.remainingNodes.remove(nodeId);
            if (!removed) {
                log.warning(String.format("Ignoring statistics propagation message from unexpected node %s by request %s.",
                        nodeId, v.reqId));

                return v;
            }

            v.locStatistics.add(msg);

            if (v.remainingNodes.isEmpty()) {
                aggregateCollected(v);

                return null;
            }

            return v;
        });
    }

    /**
     * Send statistics by request.
     *
     * @param nodeId Node to send statistics to.
     * @param msg Statistics request to process.
     * @throws IgniteCheckedException In case of errors.
     */
    private void supplyStatistics(UUID nodeId, StatsRequestMessage msg) throws IgniteCheckedException {
        List<StatsObjectData> data = new ArrayList<>();
        for (StatsKeyMessage key : msg.keys()) {
            StatsKey statsKey = new StatsKey(key.schema(), key.obj());
            ObjectStatisticsImpl objStats = statsRepos.getGlobalStatistics(statsKey);

            if (objStats != null)
                data.add(StatisticsUtils.toMessage(statsKey, StatsType.GLOBAL, objStats));
        }

        StatsPropagationMessage res = new StatsPropagationMessage(msg.reqId(), data);
        ctx.io().sendToCustomTopic(nodeId, TOPIC, res, GridIoPolicy.QUERY_POOL);
    }

    private void cancelStatisticsCollection(UUID nodeId, StatsCollectionCancelRequestMessage msg) {
        StatCollectionStatus currState = currCollections.remove(msg.reqId());
        if (currState != null) {
            if (log.isDebugEnabled())
                log.debug(String.format("Cancelling statistics collection by reqId = %s from node %s", msg.reqId(),
                        nodeId));

            currState.doneFut().cancel();
        } else
            if (log.isDebugEnabled())
                log.debug(String.format("Unable to cancel staitstics collection by req = %s from node %s", msg.reqId(),
                        nodeId));
    }

    /** {@inheritDoc} */
    @Override public void onMessage(UUID nodeId, Object msg, byte plc) {
        try {
            if (msg instanceof StatsPropagationMessage)
                receiveStatistics(nodeId, (StatsPropagationMessage) msg);
            else if (msg instanceof StatsRequestMessage)
                supplyStatistics(nodeId, (StatsRequestMessage) msg);
            else if (msg instanceof StatsCollectionRequestMessage)
                handleCollectionRequest(nodeId, (StatsCollectionRequestMessage)msg);
            else if (msg instanceof StatsCollectionCancelRequestMessage)
                cancelStatisticsCollection(nodeId, (StatsCollectionCancelRequestMessage) msg);
            else if (msg instanceof StatsClearRequestMessage)
                clearObjectStatistics(nodeId, (StatsClearRequestMessage)msg);
            else
                log.warning("Unknown msg " + msg +  " in statistics topic " + TOPIC + " from node " + nodeId);
        } catch (IgniteCheckedException e) {
            log.warning("Statistic msg from node " + nodeId + " processing failed", e);
        }
    }

    /**
     * Handle statistics clear request.
     *
     * @param nodeId UUID of request sender node.
     * @param msg Clear request message.
     */
    private void clearObjectStatistics(UUID nodeId, StatsClearRequestMessage msg) {
        for (StatsKeyMessage key : msg.keys()) {
            if (log.isTraceEnabled())
                log.trace(String.format("Clearing statistics by request %s from node %s by key %s.%s", msg.reqId(),
                        nodeId, key.schema(), key.obj()));
            statMgmtPool.submit(() -> clearObjectStatisticsLocal(key));
        }
    }

    /**
     * Collect local object statistics by specified request (possibly for a few objects) and send result back to origin
     * node specified. If local node id specified - process result without sending it throw the communication.
     *
     * @param request request to collect statistics by.
     */
    private void processLocal(UUID nodeId, StatsCollectionRequestMessage request) {
        StatCollectionStatus stat = currCollections.get(request.reqId());

        List<StatsObjectData> collected = new ArrayList<>(request.keys().size());
        for (StatsKeyMessage key : request.keys()) {
            try {
                ObjectStatisticsImpl loStat = collectLocalObjectStatistics(key, () -> stat.doneFut.isCancelled());
                StatsKey statsKey = new StatsKey(key.schema(), key.obj());
                statsRepos.mergeLocalStatistics(statsKey, loStat);

                StatsObjectData objData = StatisticsUtils.toMessage(statsKey, StatsType.LOCAL, loStat);
                collected.add(objData);
            }
            catch (IgniteCheckedException e) {
                log.warning(String.format("Unable to complete request %s due to error %s", request.reqId(), e.getMessage()));
                // TODO: send cancel to originator node
            }
        }

        StatsPropagationMessage res = StatisticsUtils.toMessage(request.reqId(), collected);

        UUID locNode = ctx.discovery().localNode().id();
        if (locNode.equals(nodeId)) {
            try {
                receiveStatistics(nodeId, res);
            }
            catch (IgniteCheckedException e) {
                // TODO
            }
        }
        else {
            try {
                ctx.io().sendToCustomTopic(nodeId, TOPIC, res, GridIoPolicy.QUERY_POOL);
            }
            catch (IgniteCheckedException e) {
                log.warning(String.format("Unable to send statistics collection result to node %s", nodeId));
            }
            currCollections.remove(request.reqId());
        }
    }

    /**
     * Schedule statistics collection by specified request.
     *
     * @param nodeId request origin node.
     * @param msg request message.
     */
    private void handleCollectionRequest(UUID nodeId, StatsCollectionRequestMessage msg) {
        assert msg.reqId() != null : "Got statistics collection request without request id";
        StatCollectionFutureAdapter doneFut = new StatCollectionFutureAdapter();
        currCollections.put(msg.reqId(), new StatCollectionStatus(msg.reqId(), null, doneFut));

        statMgmtPool.submit(() -> processLocal(nodeId, msg));
    }

    /**
     * Send "local" version of request to specified nodes except of local one.
     *
     * @param req request to send.
     * @param nodes nodes to send to.
     */
    private void sendLocalRequests(UUID reqId, Message req, Collection<UUID> nodes) {
        UUID locNode = ctx.discovery().localNode().id();

        for (UUID nodeId : nodes) {
            if (locNode.equals(nodeId))
                continue;

            try {
                ctx.io().sendToCustomTopic(nodeId, TOPIC, req, GridIoPolicy.QUERY_POOL);
            }
            catch (IgniteCheckedException e) {
                // TODO cancel collection
                StatCollectionStatus stat = currCollections.remove(reqId);
                if (stat != null) {
                    stat.doneFut().cancel();

                    if (log.isInfoEnabled())
                        log.info(String.format(
                                "Statistics collection by request %s cancelled due to node %s message sending error: %s",
                            stat.reqId(), nodeId, e.getMessage()));
                }
                break;
            }
        }
    }

    /**
     * Extract groups of stats keys.
     *
     * @param keys Statistics key to extract groups from.
     * @return Collection of group ids, which contains all specified objects.
     * @throws IgniteCheckedException In case of lack some of specified objects.
     */
    private Collection<Integer> extractGroups(Collection<StatsKey> keys) throws IgniteCheckedException {
        Collection<Integer> res = new ArrayList<>(keys.size());
        for (StatsKey key : keys) {
            GridH2Table tbl = schemaMgr.dataTable(key.schema(), key.obj());
            if (tbl == null)
                throw new IgniteCheckedException(String.format("Can't find object %s.%s", key.schema(), key.obj()));
        }
        return res;
    }

    /**
     * Get all data cluster nodes for specified cache groups.
     *
     * @param groups
     * @return
     */
    private Collection<UUID> nodes(Collection<Integer> groups) {
        AffinityTopologyVersion topVer = ctx.discovery().topologyVersionEx();

        /*ReducePartitionMapResult nodesParts =
                mapper.nodesForPartitions(cacheIds, topVer, parts, qry.isReplicatedOnly());
        return nodesParts.nodes();
        */

        return ctx.discovery().nodes(ctx.discovery().topologyVersionEx()).stream()
                .filter(node -> !node.isClient() && !node.isDaemon()).map(ClusterNode::id)
                .collect(Collectors.toList());
    }

    /**
     * Handle node left event:
     * 1) Cancel all collection tasks which expect specified node statistics result.
     * 2) Cancel collection task requested by node left.
     *
     * @param nodeId leaved node id.
     */
    private void onNodeLeft(UUID nodeId) {
        currCollections.entrySet().removeIf(entry -> {
            StatCollectionStatus stat = entry.getValue();

            if (stat.remainingNodes.stream().anyMatch(rNode -> nodeId.equals(rNode))) {
                stat.doneFut.cancel();

                if (log.isInfoEnabled())
                    log.info(String.format("Statistics collection by request %s cancelled due to node %d left.",
                            stat.reqId(), nodeId));

                return true;
            }
            return false;
        });
    }

    /**
     * Statistics collection status by local or global request.
     */
    private static class StatCollectionStatus {
        /** Request id. */
        private final UUID reqId;

        /** Set of remaining nodes. */
        private final Set<UUID> remainingNodes;

        /** Collected local statistics. */
        private final List<StatsPropagationMessage> locStatistics;

        /** Done future adapter. */
        private final StatCollectionFutureAdapter doneFut;

        /**
         * Constructor.
         *
         * @param reqId Request id.
         * @param remainingNodes Collection of remaining nodes. If {@code null} - it's local collection.
         * @param doneFut Done future adapter for collection.
         */
        public StatCollectionStatus(UUID reqId, Collection<UUID> remainingNodes, StatCollectionFutureAdapter doneFut) {
            this.reqId = reqId;
            if (remainingNodes == null) {
                this.remainingNodes = Collections.emptySet();
                this.locStatistics = null;
            }
            else {
                this.remainingNodes = new HashSet<>(remainingNodes);
                this.locStatistics = new ArrayList<>(remainingNodes.size());
            }
            this.doneFut = doneFut;
        }

        /**
         * @return Request id.
         */
        public UUID reqId() {
            return reqId;
        }

        /**
         * @return Set of nodes for whom collection is waiting for.
         */
        public Set<UUID> remainingNodes() {
            return remainingNodes;
        }

        /**
         * @return Collected local statistics.
         */
        public List<StatsPropagationMessage> localStatistics() {
            return locStatistics;
        }

        /**
         * @return Collection control future.
         */
        public StatCollectionFutureAdapter doneFut() {
            return doneFut;
        }
    }

    /**
     * Listener to handle nodeLeft/nodeFailed and call onNodeLeft method.
     */
    private class NodeLeftListener implements GridLocalEventListener {

        /** {@inheritDoc} */
        @Override public void onEvent(Event evt) {
            assert evt.type() == EventType.EVT_NODE_FAILED || evt.type() == EventType.EVT_NODE_LEFT;

            final UUID nodeId = ((DiscoveryEvent)evt).eventNode().id();

            ctx.closure().runLocalSafe(() -> onNodeLeft(nodeId), GridIoPolicy.QUERY_POOL);
        }
    }

    /**
     * Cancellable future adapter. TODO: global statistics? void? just statistics?
     */
    private static class StatCollectionFutureAdapter extends GridFutureAdapter {
        /** {@inheritDoc} */
        @Override public boolean cancel() {
            boolean res = onDone(null, null, true);

            if (res)
                onCancelled();

            return res;
        }
    }
}
