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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.processors.cache.GridCacheUtils;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.query.h2.SchemaManager;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsClearRequest;
import org.apache.ignite.internal.processors.query.stat.messages.CancelStatisticsGatheringRequest;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsGatheringRequest;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsGatheringResponse;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsKeyMessage;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsObjectData;
import org.apache.ignite.internal.util.lang.GridTuple3;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;

import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.OWNING;

/**
 * Statistics manager implementation.
 */
public class IgniteStatisticsManagerImpl implements IgniteStatisticsManager {
    /** Size of statistics collection pool. */
    private static final int STATS_POOL_SIZE = 1;

    /** Logger. */
    private final IgniteLogger log;

    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Schema manager. */
    private final SchemaManager schemaMgr;

    /** Statistics repository. */
    private final IgniteStatisticsRepository statsRepos;

    /** Current statistics collections tasks. */
    private final IgniteStatisticsHelper helper;

    /** Statistics collector. */
    private final StatisticsGathering statGathering;

    /** Statistics router. */
    private final StatisticsGatheringRequestRouter statRouter;

    /** Current collections, collection id to collection status map. */
    private final Map<UUID, StatisticsGatheringContext> currColls = new ConcurrentHashMap<>();

    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     * @param schemaMgr Schema manager.
     */
    public IgniteStatisticsManagerImpl(GridKernalContext ctx, SchemaManager schemaMgr) {
        this.ctx = ctx;
        this.schemaMgr = schemaMgr;
        helper = new IgniteStatisticsHelper(schemaMgr);

        log = ctx.log(IgniteStatisticsManagerImpl.class);


        //ctx.io().addMessageListener(TOPIC, this);

        boolean storeData = !(ctx.config().isClientMode() || ctx.isDaemon());

        IgniteCacheDatabaseSharedManager db = (GridCacheUtils.isPersistenceEnabled(ctx.config())) ?
                ctx.cache().context().database() : null;

        statsRepos = new IgniteStatisticsRepositoryImpl(storeData, db, ctx.internalSubscriptionProcessor(), this,
                ctx::log);

        // nodeLeftLsnr = new NodeLeftListener();
        IgniteThreadPoolExecutor gatMgmtPool = new IgniteThreadPoolExecutor("stat-gat-mgmt-pool",
                ctx.igniteInstanceName(),
                0,
                STATS_POOL_SIZE,
                IgniteConfiguration.DFLT_THREAD_KEEP_ALIVE_TIME,
                new LinkedBlockingQueue<>(),
                GridIoPolicy.UNDEFINED,
                ctx.uncaughtExceptionHandler()
        );

        IgniteThreadPoolExecutor msgMgmtPool = new IgniteThreadPoolExecutor("stat-msg-mgmt-pool",
                ctx.igniteInstanceName(),
                0,
                1,
                IgniteConfiguration.DFLT_THREAD_KEEP_ALIVE_TIME,
                new LinkedBlockingQueue<>(),
                GridIoPolicy.UNDEFINED,
                ctx.uncaughtExceptionHandler()
        );
        statRouter = new StatisticsGatheringRequestRouterImpl(ctx.localNodeId(), this, ctx.event(), ctx.io(),
                helper, msgMgmtPool, ctx::log);
        statGathering = new StatisticsGatheringImpl(schemaMgr, ctx.discovery(), ctx.query(), gatMgmtPool, statRouter, ctx::log);

    }

    /**
     * @return Statistics repository.
     */
    public IgniteStatisticsRepository statisticsRepository() {
        return statsRepos;
    }

    /** {@inheritDoc} */
    @Override public ObjectStatistics getLocalStatistics(String schemaName, String objName) {
        return statsRepos.getLocalStatistics(new StatisticsKey(schemaName, objName));
    }

    /**
     * Clear object statistics implementation.
     *
     * @param keys Keys to clear statistics by.
     * @throws IgniteCheckedException In case of errors.
     */
    private void clearObjectStatistics(Collection<StatisticsKeyMessage> keys) throws IgniteCheckedException {
        Collection<StatisticsAddrRequest<StatisticsClearRequest>> clearReqs = helper.generateClearRequests(keys);

        Collection<StatisticsAddrRequest<StatisticsClearRequest>> failedReqs = sendRequests(clearReqs);
        if (!F.isEmpty(failedReqs))
            if (log.isInfoEnabled())
                log.info(String.format("Unable to send all statistics clear requests to %d nodes for keys %s",
                    failedReqs.size(), keys));


        UUID locId = ctx.localNodeId();
        StatisticsAddrRequest<StatisticsClearRequest> locMsg = clearReqs.stream().filter(m -> locId.equals(m.nodeId())).findAny()
                .orElse(null);
        if (null != locMsg)
            for (StatisticsKeyMessage locKey : locMsg.req().keys())
                clearObjectStatisticsLocal(locKey);
    }

    /** {@inheritDoc} */
    @Override public void clearObjectStatistics(
            GridTuple3<String, String, String[]>... keys
    ) throws IgniteCheckedException {

        Collection<StatisticsKeyMessage> keyMsgs = Arrays.stream(keys).map(k -> new StatisticsKeyMessage(k.get1(), k.get2(),
                Arrays.asList(k.get3()))).collect(Collectors.toList());

        clearObjectStatistics(keyMsgs);
    }

    /** {@inheritDoc} */
    @Override public void clearObjectStatistics(
            String schemaName,
            String objName,
            String... colNames
    ) throws IgniteCheckedException {
        StatisticsKeyMessage keyMsg = StatisticsUtils.toMessage(schemaName, objName, colNames);

        clearObjectStatistics(Collections.singleton(keyMsg));
    }

    /**
     * Actually clear local object statistics by the given key.
     *
     * @param keyMsg Key to clear statistics by.
     */
    private void clearObjectStatisticsLocal(StatisticsKeyMessage keyMsg) {
        StatisticsKey key = new StatisticsKey(keyMsg.schema(), keyMsg.obj());
        String[] colNames = keyMsg.colNames().toArray(new String[0]);

        statsRepos.clearLocalPartitionsStatistics(key, colNames);
        statsRepos.clearLocalStatistics(key, colNames);
        statsRepos.clearGlobalStatistics(key, colNames);
    }

    /**
     * Collect object statistics prepared status.
     *
     * @param status Collection status to collect statistics by.
     * @throws IgniteCheckedException In case of errors.
     */
    private void collectObjectStatistics(StatisticsGatheringContext status) throws IgniteCheckedException {

        statRouter.sendGatheringRequestsAsync(status.gatId(), status.keys());

    }

    /** {@inheritDoc} */
    @Override public void collectObjectStatistics(
            String schemaName,
            String objName,
            String... colNames
    ) throws IgniteCheckedException {
        StatisticsKeyMessage keyMsg = new StatisticsKeyMessage(schemaName, objName, Arrays.asList(colNames));
        StatisticsGatheringContext status = new StatisticsGatheringContext(Collections.singleton(keyMsg));
        currColls.put(status.gatId(), status);
        collectObjectStatistics(status);

        status.doneFut().get();
    }

    /**
     * Ensure that local gathering context exists and schedule local statistics gathering.
     *
     * @param nodeId Initiator node id.
     * @param gatId Gathering id.
     * @param reqId Request id.
     * @param keys Keys to collect statistics by.
     */
    public void gatherLocalObjectStatisticsAsync(UUID gatId, UUID reqId, Map<StatisticsKeyMessage, int[]> keys) {
        StatisticsGatheringContext[] ctxs = new StatisticsGatheringContext[1];
        currColls.compute(gatId, (k, v) -> {
            if (v == null)
                v = new StatisticsGatheringContext(gatId, keys.keySet());

            ctxs[0] = v;

            return v;
        });

        statGathering.collectLocalObjectStatisticsAsync(reqId, keys);
    }

    /** {@inheritDoc} */
    @Override public StatsCollectionFuture<Map<GridTuple3<String, String, String[]>, ObjectStatistics>>
    collectObjectStatisticsAsync(
        GridTuple3<String, String, String[]>... keys
    ) throws IgniteCheckedException {
        Set<StatisticsKeyMessage> keysMsg = Arrays.stream(keys).map(
                k -> new StatisticsKeyMessage(k.get1(), k.get2(), Arrays.asList(k.get3()))).collect(Collectors.toSet());

        StatisticsGatheringContext status = new StatisticsGatheringContext(keysMsg);

        collectObjectStatistics(status);

        return status.doneFut();
    }

    /**
     * Cancel specified statistics gathering process.
     *
     * @param gatId Gathering id to cancel.
     */
    public void cancelLocalStatisticsGathering(UUID gatId){
       StatisticsGatheringContext ctx = currColls.remove(gatId);
       if (ctx != null)
           ctx.doneFut().cancel();
    }

    /** {@inheritDoc} */
    @Override public boolean cancelObjectStatisticsGathering(UUID colId) {
        boolean[] res = new boolean[]{true};

        helper.updateCollection(colId, s -> {
            if (s == null) {
                res[0] = false;

                return null;
            }

            s.doneFut().cancel();

            Map<UUID, List<UUID>> nodeRequests = new HashMap<>();
            for (StatisticsAddrRequest<StatisticsGatheringRequest> req : s.remainingCollectionReqs().values()) {
                nodeRequests.compute(req.nodeId(), (k, v) -> {
                   if (v == null)
                       v = new ArrayList<>();
                   v.add(req.req().reqId());

                   return v;
                });
            }

            Collection<StatisticsAddrRequest<CancelStatisticsGatheringRequest>> cancelReqs = nodeRequests.entrySet().stream().map(
                        targetNode -> new StatisticsAddrRequest<>(
                            new CancelStatisticsGatheringRequest(colId, targetNode.getValue().toArray(new UUID[0])),
                            targetNode.getKey()))
                    .collect(Collectors.toList());

            Collection<StatisticsAddrRequest<CancelStatisticsGatheringRequest>> failed = sendRequests(cancelReqs);
            if (!F.isEmpty(failed))
                if (log.isDebugEnabled())
                    log.debug(String.format("Unable to cancel all statistics collections requests (%d failed) by colId %s",
                            failed.size(), colId));

            return null;
        });

        return res[0];
    }

    /**
     * Stop statistics manager.
     */
    public void stop() {
        if (statMgmtPool != null) {
            List<Runnable> unfinishedTasks = statMgmtPool.shutdownNow();
            if (!unfinishedTasks.isEmpty())
                log.warning(String.format("%d statistics collection request cancelled.", unfinishedTasks.size()));
        }
    }

    /**
     * Receive and store partition statistics object data for locals backup partition.
     *
     * @param data Collection of partition level statistics of local bacup partitions.
     */
    public void receivePartitionsStatistics(Collection<StatisticsObjectData> data) {
        for (StatisticsObjectData partData : data) {
            StatisticsKey key = new StatisticsKey(partData.key().schema(), partData.key().obj());

            assert partData.type() == StatisticsType.PARTITION : "Got non partition level statistics by " + key
                    + " without request";

            if (log.isTraceEnabled())
                log.trace(String.format("Received partition statistics %s.%s:%d", key.schema(), key.obj(),
                    partData.partId()));

            GridH2Table tbl = schemaMgr.dataTable(key.schema(), key.obj());
            if (tbl == null) {
                if (log.isInfoEnabled())
                    log.info(String.format("Ignoring outdated partition statistics %s.%s:%d", key.schema(), key.obj(),
                        partData.partId()));

                continue;
            }
            GridDhtPartitionState partState = tbl.cacheContext().topology().partitionState(ctx.localNodeId(), partData.partId());
            if (partState != OWNING) {
                if (log.isInfoEnabled())
                    log.info(String.format("Ignoring non local partition statistics %s.%s:%d",
                            key.schema(), key.obj(), partData.partId()));

                continue;
            }

            try {
                ObjectPartitionStatisticsImpl opStat = StatisticsUtils.toObjectPartitionStatistics(ctx, partData);

                statsRepos.saveLocalPartitionStatistics(key, opStat);
            }
            catch (IgniteCheckedException e) {
                if (log.isInfoEnabled())
                    log.info(String.format("Unable to parse partition statistics for %s.%s:%d because of: %s",
                        key.schema(), key.obj(), partData.partId(), e.getMessage()));
            }
        }
    }

    /**
     * Aggregate specified gathered statistics, remove it form local and complete its future.
     * @param gatId Gathering to complete.
     */
    public void finishStatisticsCollection(UUID gatId) {
        StatisticsGatheringContext ctx = currColls.remove(gatId);
        if (ctx == null)
            return;

        Map<StatisticsKeyMessage, ObjectStatisticsImpl> keysStats = new HashMap<>();
        for(Map.Entry<StatisticsKeyMessage, Collection<ObjectStatisticsImpl>> keyStats : ctx.localStatistics().e

    }

    public void registerLocalResult(UUID gatId, Collection<StatisticsObjectData> data) {
        StatisticsGatheringContext ctx = currColls.get(gatId);
        if (ctx == null) {
            // TODO: log
            return;
        }
        ctx.registerCollected(data);
    }

    /**
     * Aggregate local statistics to global one.
     *
     * @param stat Statistics collection status to aggregate.
     * @return Map stats key to merged global statistics.
     */
    private Map<StatisticsKey, ObjectStatisticsImpl> finishStatCollection(StatisticsGatheringContext stat) {
        Map<StatisticsKeyMessage, Collection<ObjectStatisticsImpl>> keysStats = new HashMap<>();
        for (StatisticsGatheringResponse resp : stat.localStatistics()) {
            for (StatisticsObjectData objData : resp.data().keySet()) {
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

        Map<StatisticsKey, ObjectStatisticsImpl> res = new HashMap<>();
        for (Map.Entry<StatisticsKeyMessage, Collection<ObjectStatisticsImpl>> keyStats : keysStats.entrySet()) {
            StatisticsKeyMessage keyMsg = keyStats.getKey();
            GridH2Table tbl = schemaMgr.dataTable(keyMsg.schema(), keyMsg.obj());
            if (tbl == null) {
                if (log.isInfoEnabled())
                    log.info(String.format("Unable to find object %s.%s to save collected statistics by.",
                            keyMsg.schema(), keyMsg.obj()));

                continue;
            }
            ObjectStatisticsImpl globalStat = aggregateLocalStatistics(keyMsg, keyStats.getValue());
            StatisticsKey key = new StatisticsKey(keyMsg.schema(), keyMsg.obj());
            res.put(key, statsRepos.mergeGlobalStatistics(key, globalStat));
        }
        return res;
    }

    /**
     * Get global statistics by key.
     *
     * @param key Key to get statistics by.
     * @return Global statistics or {@code null} if there are no statistics for specified key.
     */
   public ObjectStatisticsImpl getGlobalStatistics(StatisticsKeyMessage key) {
        ObjectStatisticsImpl stat = statsRepos.getGlobalStatistics(new StatisticsKey(key.schema(), key.obj()));
        if (stat != null && !F.isEmpty(key.colNames()))
            stat = IgniteStatisticsHelper.filterColumns(stat, key.colNames());

        return stat;
   }

    /**
     * Clear object statistics by specified keys.
     *
     * @param keys Collection of keys to clean statistics by.
     */
   public void clearObjectStatisticsLocal(Collection<StatisticsKeyMessage> keys) {
       for (StatisticsKeyMessage key : keys)
           clearObjectStatisticsLocal(key);
   }

    /**
     * Collect local object statistics by specified request (possibly for a few objects) and send result back to origin
     * node specified. If local node id specified - process result without sending it throw the communication.
     *
     * @param req request to collect statistics by.
     */
    private void processLocal(UUID nodeId, StatisticsGatheringRequest req) {
        UUID locNode = ctx.localNodeId();

        StatisticsGatheringContext stat = (nodeId.equals(locNode)) ? helper.getCollection(req.gatId()) :
            helper.getCollection(req.reqId());

        if (stat == null)
            return;

        Map<StatisticsObjectData, int[]> collected = new HashMap<>(req.keys().size());
        for (Map.Entry<StatisticsKeyMessage, int[]> keyEntry : req.keys().entrySet()) {
            try {
                StatisticsKeyMessage key = keyEntry.getKey();
                IgniteBiTuple <ObjectStatisticsImpl, int[]> loStat = gatherLocalObjectStatisticsAsync(key,
                        keyEntry.getValue(), () -> stat.doneFut().isCancelled());
                StatisticsKey statsKey = new StatisticsKey(key.schema(), key.obj());

                // TODO?
                statsRepos.mergeLocalStatistics(statsKey, loStat.getKey());

                StatisticsObjectData objData = StatisticsUtils.toObjectData(key, StatisticsType.LOCAL, loStat.getKey());
                collected.put(objData, loStat.getValue());
            }
            catch (IgniteCheckedException e) {
                log.warning(String.format("Unable to complete request %s due to error %s", req.reqId(), e.getMessage()));
                // TODO: send cancel to originator node
            }
        }

        StatisticsGatheringResponse res = new StatisticsGatheringResponse(req.gatId(), req.reqId(), collected);

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
                        nodeId, req.gatId(), req.reqId()));
            }

            // Not local collection - remove by its reqId.
            helper.updateCollection(req.reqId(), s -> null);
        }
    }
}
