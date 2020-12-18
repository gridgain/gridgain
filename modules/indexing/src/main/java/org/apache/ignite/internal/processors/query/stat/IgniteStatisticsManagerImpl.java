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
import org.apache.ignite.internal.processors.query.stat.messages.StatsClearRequest;
import org.apache.ignite.internal.processors.query.stat.messages.CancelStatsCollectionRequest;
import org.apache.ignite.internal.processors.query.stat.messages.StatsCollectionRequest;
import org.apache.ignite.internal.processors.query.stat.messages.StatsCollectionResponse;
import org.apache.ignite.internal.processors.query.stat.messages.StatsKeyMessage;
import org.apache.ignite.internal.processors.query.stat.messages.StatsObjectData;
import org.apache.ignite.internal.processors.query.stat.messages.StatsPropagationMessage;
import org.apache.ignite.internal.processors.query.stat.messages.StatsGetRequest;
import org.apache.ignite.internal.util.lang.GridTuple3;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiTuple;
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

    /** Current statistics collections tasks. */
    private final IgniteStatisticsRequestCollection currCollections;


    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     * @param schemaMgr Schema manager.
     */
    public IgniteStatisticsManagerImpl(GridKernalContext ctx, SchemaManager schemaMgr) {
        this.ctx = ctx;
        this.schemaMgr = schemaMgr;
        currCollections = new IgniteStatisticsRequestCollection(schemaMgr);

        log = ctx.log(IgniteStatisticsManagerImpl.class);

        ctx.io().addMessageListener(TOPIC, this);

        boolean storeData = !(ctx.config().isClientMode() || ctx.isDaemon());

        IgniteCacheDatabaseSharedManager db = (GridCacheUtils.isPersistenceEnabled(ctx.config())) ?
                ctx.cache().context().database() : null;

        statsRepos = new IgniteStatisticsRepositoryImpl(storeData, db, ctx.internalSubscriptionProcessor(), this,
                ctx::log);

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
        final StatsCollectionFutureAdapter doneFut = new StatsCollectionFutureAdapter(null);

        UUID reqId = UUID.randomUUID();
        StatsKeyMessage keyMsg = StatisticsUtils.toMessage(schemaName, objName, colNames);
        Collection<StatsAddrRequest<StatsClearRequest>> reqs = currCollections
                .generateClearRequests(Collections.singletonList(keyMsg));

        sendLocalRequests(reqs);
        Map<UUID, List<Integer>> requestNodes = null;
        /*try {
            requestNodes = nodePartitions(extractGroups(Collections.singletonList(keyMsg)), null);
        } catch (IgniteCheckedException e) {
            // TODO: handle & remove task
        }

        StatsClearRequest req = new StatsClearRequest(reqId, false, Collections.singletonList(keyMsg));

        sendLocalRequests(reqId, req, requestNodes);*/

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

    // TODO
    /**
     * Collect local object statistics by primary partitions of specified object.
     *
     * @param keyMsg Statistic key message to collect statistics by.
     * @param partIds Set of partition ids to collect statistics by.
     * @param cancelled Supplier to check if operation was cancelled.
     * @return Tuple of Collected local statistics with array of successfully collected partitions.
     * @throws IgniteCheckedException
     */
    private IgniteBiTuple<ObjectStatisticsImpl, int[]> collectLocalObjectStatistics(
            StatsKeyMessage keyMsg,
            int[] partIds,
            Supplier<Boolean> cancelled
    ) throws IgniteCheckedException {
        GridH2Table tbl = schemaMgr.dataTable(keyMsg.schema(), keyMsg.obj());
        if (tbl == null)
            throw new IgniteCheckedException(String.format("Can't find table %s.%s", keyMsg.schema(), keyMsg.obj()));

        Column[] selectedCols = filterColumns(tbl.getColumns(), keyMsg.colNames());

        Collection<ObjectPartitionStatisticsImpl> partsStats = collectPartitionStatistics(tbl, partIds, selectedCols,
                cancelled);
        if (partsStats == null) {
            assert cancelled.get() : "Error collecting partition level statistics.";

            return null;
        }

        sendPartStatsToBU(tbl, partsStats);

        StatsKey key = new StatsKey(keyMsg.schema(), keyMsg.obj());
        statsRepos.saveLocalPartitionsStatistics(key, partsStats);

        ObjectStatisticsImpl tblStats = aggregateLocalStatistics(tbl, selectedCols, partsStats);
        statsRepos.mergeLocalStatistics(key, tblStats);

        return new IgniteBiTuple(tblStats, partsStats.stream().map(ObjectPartitionStatisticsImpl::partId).toArray());
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
        StatsKeyMessage keyMsg = new StatsKeyMessage(tbl.identifier().schema(), tbl.identifier().table(), null);
        GridDhtPartitionTopology topology = tbl.cacheContext().topology();
        Map<UUID, List<StatsObjectData>> statsByNodes = new HashMap<>();
        for (ObjectPartitionStatisticsImpl stat : objStats) {
            StatsObjectData statData = StatisticsUtils.toObjectData(keyMsg, StatsType.PARTITION, stat);
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
            //StatsPropagationMessage nodeMsg = StatisticsUtils.toMessage(null, statToNode.getValue());

            //ctx.io().sendToCustomTopic(statToNode.getKey(), TOPIC, nodeMsg, GridIoPolicy.QUERY_POOL);
        }
    }

    /**
     * Group local statistics by table and columns.
     *
     * @param status statistics collection status with all necessary local statistics.
     * @return map of tables to columns to list of local column statistics.
     */
    private Map<GridH2Table, Map<String, List<ColumnStatistics>>> groupColumnStatistics(StatCollectionStatus status) {
        /*Map<GridH2Table, Map<String, List<ColumnStatistics>>> result = new HashMap<>();
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
        */
        return null;
    }

    /**
     * Will aggregate all received local statistics and save it.
     *
     * @param request aggregation request with id only.
     */
    private void aggregateCollected(StatCollectionStatus status) {
        /*// Table -> Column -> List of local column stats
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
        */

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

    /**
     * Collect object statistics prepared status.
     *
     * @param status Collection status to collect statistics by.
     * @throws IgniteCheckedException In case of errors.
     */
    private void collectObjectStatistics(StatCollectionStatus status) throws IgniteCheckedException {
        synchronized (status) {
            currCollections.updateCollection(status.colId(), s -> status);
            Map<StatsKeyMessage, int[]> failedPartitions = null;
            int cnt = 0;
            do {
                Collection<StatsAddrRequest<StatsCollectionRequest>> reqs = currCollections
                        .generateCollectionRequests(status.colId(), status.keys(), failedPartitions);

                //Map<UUID, StatsCollectionRequest> msgs = reqs.stream().collect(Collectors.toMap(
                //    StatsAddrRequest::nodeId, StatsAddrRequest::req));

                Collection<StatsAddrRequest<StatsCollectionRequest>> failedMsgs = sendLocalRequests(reqs);
                Map<UUID, StatsAddrRequest<StatsCollectionRequest>> sendedMsgs;
                if (failedMsgs == null) {
                    sendedMsgs = reqs.stream().collect(Collectors.toMap(r -> r.req().reqId(), r -> r));
                    failedPartitions = null;
                }
                else {
                    Set<UUID> failedIds = failedMsgs.stream().map(r -> r.req().reqId()).collect(Collectors.toSet());
                    sendedMsgs = reqs.stream().filter(r -> !failedIds.contains(r.req().reqId()))
                            .collect(Collectors.toMap(r -> r.req().reqId(), r -> r));
                    failedPartitions = currCollections.extractFailed(failedMsgs.stream().map(StatsAddrRequest::req)
                            .toArray(StatsCollectionRequest[]::new));
                }
                status.remainingCollectionReqs().putAll(sendedMsgs);

                if (cnt++ > 10) {
                    StatsKeyMessage key = status.keys().iterator().next();

                    throw new IgniteCheckedException(String.format(
                            "Unable to send all messages to collect statistics by key %s.%s and the others %d",
                            key.schema(), key.obj(), status.keys().size() - 1));
                }
            }
            while (failedPartitions != null);
        }

        UUID locNode = ctx.discovery().localNode().id();
        StatsAddrRequest<StatsCollectionRequest> locReq = status.remainingCollectionReqs().values().stream()
                .filter(r -> locNode.equals(r.nodeId())).findAny().orElse(null);
        if (locReq != null)
            statMgmtPool.submit(() -> processLocal(ctx.discovery().localNode().id(), locReq.req()));
    }

    /** {@inheritDoc} */
    @Override public void collectObjectStatistics(
            String schemaName,
            String objName,
            String... colNames
    ) throws IgniteCheckedException {
        UUID colId = UUID.randomUUID();
        StatsCollectionFutureAdapter doneFut = new StatsCollectionFutureAdapter(colId);
        StatsKeyMessage keyMsg = new StatsKeyMessage(schemaName, objName, Arrays.asList(colNames));
        StatCollectionStatus status = new StatCollectionStatus(colId, Collections.singletonList(keyMsg),
            Collections.emptyMap(), doneFut);

        collectObjectStatistics(status);

        doneFut.get();
    }

    /** {@inheritDoc} */
    @Override public StatsCollectionFuture<Map<GridTuple3<String, String, String[]>, ObjectStatistics>>
    collectObjectStatisticsAsync(
        GridTuple3<String, String, String[]>... keys
    ) throws IgniteCheckedException {
        UUID colId = UUID.randomUUID();
        StatsCollectionFutureAdapter doneFut = new StatsCollectionFutureAdapter(colId);
        Collection<StatsKeyMessage> keysMsg = Arrays.stream(keys).map(
                k -> new StatsKeyMessage(k.get1(), k.get2(), Arrays.asList(k.get3()))).collect(Collectors.toList());

        StatCollectionStatus status = new StatCollectionStatus(colId, keysMsg, Collections.emptyMap(), doneFut);

        collectObjectStatistics(status);

        return status.doneFut();
    }

    /** {@inheritDoc} */
    @Override public boolean cancelObjectStatisticsCollection(UUID colId) {
        boolean[] res = new boolean[]{true};

        currCollections.updateCollection(colId, s -> {
            if (s == null) {
                res[0] = false;

                return null;
            }

            s.doneFut().cancel();

            Map<UUID, List<UUID>> nodeRequests = new HashMap<>();
            for (StatsAddrRequest<StatsCollectionRequest> req : s.remainingCollectionReqs().values()) {
                nodeRequests.compute(req.nodeId(), (k, v) -> {
                   if (v == null)
                       v = new ArrayList();
                   v.add(req.req().reqId());

                   return v;
                });
            }

            Collection<StatsAddrRequest<CancelStatsCollectionRequest>> cancelReqs = nodeRequests.entrySet().stream().map(
                        targetNode -> new StatsAddrRequest<CancelStatsCollectionRequest>(
                            new CancelStatsCollectionRequest(colId, targetNode.getValue().toArray(new UUID[0])),
                            targetNode.getKey()))
                    .collect(Collectors.toList());

            Collection<StatsAddrRequest<CancelStatsCollectionRequest>> failed = sendLocalRequests(cancelReqs);
            if (failed != null)
                if (log.isDebugEnabled())
                    log.debug(String.format("Unable to cancel all statistics collections requests (%d failed) by colId %s",
                            failed.size(), colId));

            return null;
        });

        return res[0];
    }

    /**
     * Generate and try to send all request for particular status. Should be called inside status lock after putting it
     * into currCollections map. REMOVE!!!!
     *
     * @param status Status to process.
     * @param keys Collection of object keys to collect statistics by.
     * @return {@code true} if all request was successfully sended, {@code false} - otherwise (one should remove
     * status from cullCollections.
     */
    protected boolean doRequests(StatCollectionStatus status, List<StatsKeyMessage> keys) throws IgniteCheckedException {
        /*Map<CacheGroupContext, Collection<StatsKeyMessage>> grpContexts = extractGroups(keys);
        List<Map<UUID, StatsCollectionAddrRequest>> reqsByGrps = new ArrayList<>(grpContexts.size());
        for (Map.Entry<CacheGroupContext, Collection<StatsKeyMessage>> grpEntry : grpContexts.entrySet()) {
            Map<UUID, List<Integer>> reqNodes = nodePartitions(grpEntry.getKey(), null);
            for(StatsKeyMessage keyMsg : grpEntry.getValue())
                reqsByGrps.add(prepareRequests(status.colId(), keyMsg, reqNodes));

        }
        Map<UUID, StatsCollectionAddrRequest> reqs = compressRequests(reqsByGrps);

        status.remainingCollectionReqs().putAll(reqs);

        Map<UUID, StatsCollectionRequest> failedReqs = sendLocalRequests(reqs.values().stream().collect(
                Collectors.toMap(StatsCollectionAddrRequest::nodeId, StatsCollectionAddrRequest::req)));
        // TODO: cycle replanning and sending
        return failedReqs.isEmpty();

         */
        return false;
    }

    /**
     * Group request by target node id. REMOVE!!!!
     *
     * @param reqsByGrps Requests to compress, map.
     * @return Grouped requests.
     */
    protected Map<UUID, StatsAddrRequest<StatsCollectionRequest>> compressRequests(
        List<Map<UUID, StatsAddrRequest<StatsCollectionRequest>>> reqsByGrps) {
        // NodeId to base request map
        Map<UUID, StatsAddrRequest<StatsCollectionRequest>> reqByNode = new HashMap<>();
        for (Map<UUID, StatsAddrRequest<StatsCollectionRequest>> grpReqs : reqsByGrps)
            for (StatsAddrRequest<StatsCollectionRequest> addReq : grpReqs.values())
                reqByNode.compute(addReq.nodeId(), (k, v) -> (v == null) ? addReq : addKey(v, addReq));

        return reqByNode.entrySet().stream().collect(Collectors.toMap(e -> e.getValue().req().reqId(),
            Map.Entry::getValue));
    }

    /**
     * Add keys from add request to the base one and return it.
     *
     * @param base Base request to add to.
     * @param add Add request to add.
     * @return Request with all keys from both specified.
     */
    protected StatsAddrRequest<StatsCollectionRequest> addKey(
        StatsAddrRequest<StatsCollectionRequest> base,
        StatsAddrRequest<StatsCollectionRequest> add
    ) {
        assert base.nodeId().equals(add.nodeId());

        base.req().keys().putAll(add.req().keys());
        return base;
    }


    /**
     * Prepare statistics collection request for each nodes. MOVED!!!!
     *
     * @param colId Collection id.
     * @param keyMsg Key to collect statistics by.
     * @param reqNodes Map of node id to array of partition ids to be collected on that node.
     * @return Map: request id to statistics collection addressed request.
     */
    protected Map<UUID, StatsAddrRequest<StatsCollectionRequest>> _____prepareRequests(
            UUID colId,
            StatsKeyMessage keyMsg,
            Map<UUID, List<Integer>> reqNodes
    ) {
        Map<UUID, StatsAddrRequest<StatsCollectionRequest>> res = new HashMap<>(reqNodes.size());
        for (Map.Entry<UUID, List<Integer>> reqNode : reqNodes.entrySet()) {
            UUID reqId = UUID.randomUUID();
            StatsCollectionRequest colReq = new StatsCollectionRequest(colId, reqId, Collections.singletonMap(keyMsg,
                    reqNode.getValue().stream().mapToInt(Integer::intValue).toArray()));
            StatsAddrRequest<StatsCollectionRequest> addrReq = new StatsAddrRequest(colReq, reqNode.getKey());
            res.put(reqId, addrReq);
        }
        return res;
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
    ) throws IgniteCheckedException {
        List<ObjectPartitionStatisticsImpl> tblPartStats = new ArrayList<>();
        GridH2RowDescriptor desc = tbl.rowDescriptor();
        String tblName = tbl.getName();
        GridDhtPartitionTopology topology = tbl.cacheContext().topology();
        AffinityTopologyVersion topologyVersion = topology.readyTopologyVersion();

        for (int partId : partIds) {
            GridDhtLocalPartition locPart = topology.localPartition(partId, topologyVersion, false);
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
     * @param keyMsg Aggregation key.
     * @param stats Collection of all local partition level or local level statistics by specified key to aggregate.
     * @return Local level aggregated statistics.
     */
    public ObjectStatisticsImpl aggregateLocalStatistics(
            StatsKeyMessage keyMsg,
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
            statsRepos.clearLocalPartitionsStatistics(new StatsKey(keyMsg.schema(), keyMsg.obj()));
        }

        return aggregateLocalStatistics(tbl, filterColumns(tbl.getColumns(), keyMsg.colNames()), stats);
    }

    /**
     * Aggregate partition level statistics to local level one.
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
    private void receiveLocalStatistics(UUID nodeId, StatsCollectionResponse msg) {
        assert msg.data().keySet().stream().noneMatch(pd -> pd.type() == StatsType.PARTITION)
                : "Got partition statistics by request " + msg.reqId();

        currCollections.updateCollection(msg.colId(), stat -> {
            if (stat == null) {
                if (log.isDebugEnabled())
                    log.debug(String.format(
                        "Ignoring outdated local statistics collection response from node %s to col %s req %s",
                            nodeId, msg.colId(), msg.reqId()));

                return stat;
            }

            assert stat.colId().equals(msg.colId());

            // Need syncronization here to avoid races between removing remaining reqs and adding new ones.
            synchronized (stat) {
                StatsAddrRequest<StatsCollectionRequest> req = stat.remainingCollectionReqs().remove(msg.reqId());

                if (req == null) {
                    if (log.isDebugEnabled())
                        log.debug(String.format(
                                "Ignoring unknown local statistics collection response from node %s to col %s req %s",
                                nodeId, msg.colId(), msg.reqId()));

                    return stat;
                }

                stat.localStatistics().add(msg);
                // TODO: reschedule if not all partition collected.

                if (stat.remainingCollectionReqs().isEmpty()) {
                    Map<StatsKey, ObjectStatisticsImpl> mergedGlobal = finishStatCollection(stat);

                    stat.doneFut().onDone(null);

                    return null;
                }
            }
            return stat;
        });
        /**currCollections.compute(msg.reqId(), (k, v) -> {
            if (v == null) {
                if (log.isInfoEnabled())
                    log.info(String.format("Ignoring outdated local statistics collection response from node %s to req %s",
                            nodeId, msg.reqId()));

                return null;
            }

            assert msg.reqId().equals(v.reqId);

            boolean rmv = v.remainingNodes.remove(nodeId);
            if (!rmv) {
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
        });**/
    }

    /**
     * Aggregate local statistics to global one.
     *
     * @param stat Statistics collection status to aggregate.
     * @return Map stats key to merged global statistics.
     */
    private Map<StatsKey, ObjectStatisticsImpl> finishStatCollection(StatCollectionStatus stat) {
        Map<StatsKeyMessage, Collection<ObjectStatisticsImpl>> keysStats = new HashMap<>();
        for (StatsCollectionResponse resp : stat.localStatistics()) {
            for (StatsObjectData objData : resp.data().keySet()) {
                keysStats.compute(objData.key(), (k, v) -> {
                    if (v == null)
                        v = new ArrayList<>();
                    try {
                        ObjectStatisticsImpl objStat = StatisticsUtils.toObjectStatistics(ctx, objData);

                        v.add(objStat);
                    } catch (IgniteCheckedException e) {
                        if (log.isInfoEnabled())
                            log.info(String.format("Unable to parse statistics for object %s from response %s",
                                    objData.key(), resp.reqId()));
                    }

                    return v;
                });
            }
        }

        Map<StatsKey, ObjectStatisticsImpl> res = new HashMap<>();
        for (Map.Entry<StatsKeyMessage, Collection<ObjectStatisticsImpl>> keyStats : keysStats.entrySet()) {
            StatsKeyMessage keyMsg = keyStats.getKey();
            GridH2Table tbl = schemaMgr.dataTable(keyMsg.schema(), keyMsg.obj());
            if (tbl == null) {
                if (log.isInfoEnabled())
                    log.info(String.format("Unable to find object %s.%s to save collected statistics by.",
                            keyMsg.schema(), keyMsg.obj()));

                continue;
            }
            ObjectStatisticsImpl globalStat = aggregateLocalStatistics(keyMsg, keyStats.getValue());
            StatsKey key = new StatsKey(keyMsg.schema(), keyMsg.obj());
            res.put(key, statsRepos.mergeGlobalStatistics(key, globalStat));
        }
        return res;
    }

    /**
     * Send statistics by request.
     *
     * @param nodeId Node to send statistics to.
     * @param msg Statistics request to process.
     * @throws IgniteCheckedException In case of errors.
     */
    private void supplyStatistics(UUID nodeId, StatsGetRequest msg) throws IgniteCheckedException {
        List<StatsObjectData> data = new ArrayList<>(msg.keys().size());
        for (StatsKeyMessage keyMsg : msg.keys()) {
            StatsKey key = new StatsKey(keyMsg.schema(), keyMsg.obj());
            ObjectStatisticsImpl objStats = statsRepos.getGlobalStatistics(key);

            if (objStats != null)
                data.add(StatisticsUtils.toObjectData(keyMsg, StatsType.GLOBAL, objStats));
        }
        StatsPropagationMessage res = new StatsPropagationMessage(data);
        ctx.io().sendToCustomTopic(nodeId, TOPIC, res, GridIoPolicy.QUERY_POOL);

        /**List<StatsObjectData> data = new ArrayList<>();
        for (StatsKeyMessage key : msg.keys()) {
            StatsKey statsKey = new StatsKey(key.schema(), key.obj());
            ObjectStatisticsImpl objStats = statsRepos.getGlobalStatistics(statsKey);

            if (objStats != null)
                data.add(StatisticsUtils.toMessage(statsKey, StatsType.GLOBAL, objStats));
        }

        StatsPropagationMessage res = new StatsPropagationMessage(msg.reqId(), data);
        ctx.io().sendToCustomTopic(nodeId, TOPIC, res, GridIoPolicy.QUERY_POOL);**/
    }

    /**
     * Cacel local statistics collection task.
     *
     * @param nodeId Sender node id.
     * @param msg Cancel request.
     */
    private void cancelStatisticsCollection(UUID nodeId, CancelStatsCollectionRequest msg) {
        for (UUID reqId : msg.reqIds()) {
            currCollections.updateCollection(reqId, stat -> {
                if (stat == null) {
                    if (log.isDebugEnabled())
                        log.debug(String.format("Unable to cancel statistics collection %s by req %s from node %s",
                                msg.colId(), reqId, nodeId));

                    return null;
                }

                stat.doneFut().cancel();

                if (log.isDebugEnabled())
                    log.debug(String.format("Cancelling statistics collection by colId = %s, reqId = %s from node %s",
                            msg.colId(), reqId, nodeId));

                return null;
            });
        }
    }

    /** {@inheritDoc} */
    @Override public void onMessage(UUID nodeId, Object msg, byte plc) {
        try {
            if (msg instanceof StatsPropagationMessage)
                receivePartitionsStatistics(nodeId, (StatsPropagationMessage) msg);
            else if (msg instanceof StatsCollectionResponse)
                receiveLocalStatistics(nodeId, (StatsCollectionResponse) msg);
            else if (msg instanceof StatsGetRequest)
                supplyStatistics(nodeId, (StatsGetRequest) msg);
            else if (msg instanceof StatsCollectionRequest)
                handleCollectionRequest(nodeId, (StatsCollectionRequest)msg);
            else if (msg instanceof CancelStatsCollectionRequest)
                cancelStatisticsCollection(nodeId, (CancelStatsCollectionRequest) msg);
            else if (msg instanceof StatsClearRequest)
                clearObjectStatistics(nodeId, (StatsClearRequest)msg);
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
    private void clearObjectStatistics(UUID nodeId, StatsClearRequest msg) {
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
     * @param req request to collect statistics by.
     */
    private void processLocal(UUID nodeId, StatsCollectionRequest req) {
        UUID locNode = ctx.discovery().localNode().id();

        StatCollectionStatus stat = (nodeId.equals(locNode)) ? currCollections.getCollection(req.colId()) :
            currCollections.getCollection(req.reqId());

        Map<StatsObjectData, int[]> collected = new HashMap<>(req.keys().size());
        for (Map.Entry<StatsKeyMessage, int[]> keyEntry : req.keys().entrySet()) {
            try {
                StatsKeyMessage key = keyEntry.getKey();
                IgniteBiTuple <ObjectStatisticsImpl, int[]> loStat = collectLocalObjectStatistics(key,
                        keyEntry.getValue(), () -> stat.doneFut().isCancelled());
                StatsKey statsKey = new StatsKey(key.schema(), key.obj());

                // TODO?
                statsRepos.mergeLocalStatistics(statsKey, loStat.getKey());

                StatsObjectData objData = StatisticsUtils.toObjectData(key, StatsType.LOCAL, loStat.getKey());
                collected.put(objData, loStat.getValue());
            }
            catch (IgniteCheckedException e) {
                log.warning(String.format("Unable to complete request %s due to error %s", req.reqId(), e.getMessage()));
                // TODO: send cancel to originator node
            }
        }

        StatsCollectionResponse res = new StatsCollectionResponse(req.colId(), req.reqId(), collected);

        if (locNode.equals(nodeId))
            receiveLocalStatistics(nodeId, res);
        else {
            try {
                ctx.io().sendToCustomTopic(nodeId, TOPIC, res, GridIoPolicy.QUERY_POOL);
            }
            catch (IgniteCheckedException e) {
                if (log.isDebugEnabled())
                    log.debug(String.format(
                            "Unable to send statistics collection result to node %s in response to colId %s, reqId %s",
                        nodeId, req.colId(), req.reqId()));
            }

            // Not local collection - remove by its reqId.
            currCollections.updateCollection(req.reqId(), s -> null);
        }
    }

    /**
     * Schedule statistics collection by specified request.
     *
     * @param nodeId request origin node.
     * @param msg request message.
     */
    private void handleCollectionRequest(UUID nodeId, StatsCollectionRequest msg) {
        assert msg.reqId() != null : "Got statistics collection request without request id";
        StatsCollectionFutureAdapter doneFut = new StatsCollectionFutureAdapter(msg.colId());

        currCollections.addCollection(msg.reqId(), new StatCollectionStatus(msg.colId(), msg.keys().keySet(), null, doneFut));

        statMgmtPool.submit(() -> processLocal(nodeId, msg));
    }

    /**
     * Send requests to target nodes except of local one.
     *
     * @param reqs Collection of addressed requests to send.
     * @return Collection of addressed requests that has errors while sending or {@code null} if all requests was send
     * successfully.
     */
    private <T extends Message> Collection<StatsAddrRequest<T>> sendLocalRequests(Collection<StatsAddrRequest<T>> reqs) {
        UUID locNode = ctx.discovery().localNode().id();
        Collection<StatsAddrRequest<T>> res = null;

        for (StatsAddrRequest<T> req : reqs) {
            if (locNode.equals(req.nodeId()))
                continue;

            try {
                ctx.io().sendToCustomTopic(req.nodeId(), TOPIC, req.req(), GridIoPolicy.QUERY_POOL);
            }
            catch (IgniteCheckedException e) {
                if (res == null)
                    res = new ArrayList<>();

                res.add(req);
            }
        }

        return res;
    }

    /**
     * Handle node left event:
     * 1) Cancel all collection tasks which expect specified node statistics result.
     * 2) Cancel collection task requested by node left.
     *
     * @param nodeId leaved node id.
     */
    private void onNodeLeft(UUID nodeId) {
        Map<UUID, Map<StatsKeyMessage, int[]>> failedCollections = new HashMap<>();
        currCollections.updateAllCollections(colStat -> {
            StatsCollectionRequest[] nodeRequests = (StatsCollectionRequest[])colStat.remainingCollectionReqs()
                    .values().stream().filter(
                    addReq -> nodeId.equals(addReq.nodeId())).map(StatsAddrRequest::req).toArray();
            if (!F.isEmpty(nodeRequests)) {
                Map<StatsKeyMessage, int[]> failedKeys = IgniteStatisticsRequestCollection.extractFailed(nodeRequests);
                try {
                    Collection<StatsAddrRequest<StatsCollectionRequest>> reqs = currCollections
                        .generateCollectionRequests(colStat.colId(), colStat.keys(), failedKeys);
                    //Map<UUID, StatsCollectionRequest> msgs = reqs.stream().collect(Collectors.toMap(
                    //        StatsAddrRequest::nodeId, StatsAddrRequest::req));
                    // TODO: resend it
                    sendLocalRequests(reqs);
                } catch (IgniteCheckedException e) {
                    // TODO
                    e.printStackTrace();
                }

            }
            return null;
        });
        /*
        currCollections.entrySet().forEach(entry -> {
            StatCollectionStatus stat = entry.getValue();
            // Single node can had multiple request in case of resending due to previous node left events.
            Collection<StatsCollectionRequest> removedReqs = entry.getValue().removeNodeRequest(nodeId);
            rescheduleFailedPartitions();
            List<StatsCollectionAddressedRequest> removedRequests = new ArrayList<>();

            entry.getValue().remainingCollectionReqs().entrySet().removeIf(colReqEntry -> {
                if (nodeId.equals(colReqEntry.getValue().targetNodeId)) {
                    removedRequests.add(colReqEntry.getValue());

                    return true;
                }

                return false;
            });

            if (!removedRequests.isEmpty()) {
                // Reshedule all partitions, which was on left node.
                nodePartitions()
            }




            if (stat.remainingCollectionReqs().entrySet().stream().anyMatch(rNode -> nodeId.equals(rNode))) {
                stat.doneFut.cancel();

                if (log.isInfoEnabled())
                    log.info(String.format("Statistics collection by request %s cancelled due to node %d left.",
                            stat.reqId(), nodeId));

                return true;
            }
            return false;
        });

         */
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
}
