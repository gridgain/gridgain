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
import java.util.stream.Collectors;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheUtils;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.query.h2.SchemaManager;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsKeyMessage;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsObjectData;
import org.apache.ignite.internal.util.lang.GridTuple3;
import org.apache.ignite.internal.util.typedef.F;
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

    /** Statistics crawler. */
    private final StatisticsGatheringRequestCrawler statCrawler;

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
        helper = new IgniteStatisticsHelper(ctx.localNodeId(), schemaMgr, ctx::log);

        log = ctx.log(IgniteStatisticsManagerImpl.class);

        IgniteCacheDatabaseSharedManager db = (GridCacheUtils.isPersistenceEnabled(ctx.config())) ?
                ctx.cache().context().database() : null;

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
        statCrawler = new StatisticsGatheringRequestCrawlerImpl(ctx.localNodeId(), this, ctx.event(), ctx.io(),
            helper, msgMgmtPool, ctx::log);
        statGathering = new StatisticsGatheringImpl(schemaMgr, ctx.discovery(), ctx.query(), statCrawler, gatMgmtPool,
            ctx::log);

        boolean storeData = !(ctx.config().isClientMode() || ctx.isDaemon());
        IgniteStatisticsStore store;
        if (!storeData)
            store = new IgniteStatisticsDummyStoreImpl(ctx::log);
        else if (db == null)
            store = new IgniteStatisticsInMemoryStoreImpl(ctx::log);
        else
            store = new IgniteStatisticsPersistenceStoreImpl(ctx.internalSubscriptionProcessor(), db, ctx::log);

        statsRepos = new IgniteStatisticsRepositoryImpl(store, this, statGathering, ctx::log);

        store.repository(statsRepos);

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
        statCrawler.sendClearStatisticsAsync(keys);
    }

    /**
     * Clear local statistics by specified keys.
     *
     * @param keys Keys to clear statistics by.
     */
    public void clearObjectsStatisticsLocal(Collection<StatisticsKeyMessage> keys) {
        for (StatisticsKeyMessage key : keys)
            clearObjectStatisticsLocal(key);
    }

    /**
     * Update counter to mark that statistics by some partitions where collected by remote request.
     *
     * @param gatId Gathering id.
     * @param parts Partitions count.
     */
    public void onRemoteGatheringSend(UUID gatId, int parts) {
        currColls.compute(gatId, (k,v) -> {
           if (v == null) {
               if (log.isDebugEnabled())
                   log.debug(String.format("Unable to mark %d partitions gathered by gathering id %s", parts, gatId));

               return null;
           }

           return v.registerCollected(Collections.emptyMap(), parts) ? null : v;
        });
    }

    /** {@inheritDoc} */
    @Override public void clearObjectStatistics(
            final GridTuple3<String, String, String[]>... keys
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
        statCrawler.sendGatheringRequestsAsync(status.gatId(), status.keys(), null);
    }

    /** {@inheritDoc} */
    @Override public void collectObjectStatistics(
            String schemaName,
            String objName,
            String... colNames
    ) throws IgniteCheckedException {

        StatisticsKeyMessage keyMsg = new StatisticsKeyMessage(schemaName, objName, Arrays.asList(colNames));
        CacheGroupContext grpCtx = helper.getGroupContext(keyMsg);

        StatisticsGatheringContext status = new StatisticsGatheringContext(UUID.randomUUID(),
            Collections.singleton(keyMsg), grpCtx.topology().partitions());

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
     * @param parts Partitions to collect statistics from.
     */
    public void gatherLocalObjectStatisticsAsync(
        UUID gatId,
        UUID reqId,
        Collection<StatisticsKeyMessage> keys,
        int[] parts
    ) {
        int partsCnt = (int)Arrays.stream(parts).count();
        Set<StatisticsKeyMessage> keysSet = new HashSet<>(keys);

        StatisticsGatheringContext gCtx = currColls.computeIfAbsent(gatId, k ->
            new StatisticsGatheringContext(gatId, keysSet, partsCnt));

        statGathering.collectLocalObjectsStatisticsAsync(reqId, keysSet, parts, () -> gCtx.doneFut().isCancelled());
    }

    /** {@inheritDoc} */
    @Override public StatsCollectionFuture<Map<GridTuple3<String, String, String[]>, ObjectStatistics>>[]
        collectObjectStatisticsAsync(
            GridTuple3<String, String, String[]>... keys
    ) throws IgniteCheckedException {
        Set<StatisticsKeyMessage> keysMsg = Arrays.stream(keys).map(
                k -> new StatisticsKeyMessage(k.get1(), k.get2(), Arrays.asList(k.get3()))).collect(Collectors.toSet());
        Map<CacheGroupContext, Collection<StatisticsKeyMessage>> grpsKeys = helper.splitByGroups(keysMsg);

        List<StatsCollectionFuture<Map<GridTuple3<String, String, String[]>, ObjectStatistics>>> res = new ArrayList<>();

        for (Map.Entry<CacheGroupContext, Collection<StatisticsKeyMessage>> grpKeys : grpsKeys.entrySet()) {
            int parts = grpKeys.getKey().topology().partitions();

            StatisticsGatheringContext status = new StatisticsGatheringContext(UUID.randomUUID(),
                    new HashSet<>(grpKeys.getValue()), parts);

            collectObjectStatistics(status);

            res.add(status.doneFut());
        }

        return res.toArray(new StatsCollectionFuture[0]);
    }

    /**
     * Cancel specified statistics gathering process.
     *
     * @param gatId Gathering id to cancel.
     */
    public void cancelLocalStatisticsGathering(UUID gatId) {
       StatisticsGatheringContext stCtx = currColls.remove(gatId);
       if (stCtx != null)
           stCtx.doneFut().cancel();
    }

    /** {@inheritDoc} */
    @Override public boolean cancelObjectStatisticsGathering(UUID gatId) {
        boolean res = false;
        StatisticsGatheringContext stCtx = currColls.remove(gatId);
        if (stCtx != null) {

            res = stCtx.doneFut().cancel();
            if (res)
                statCrawler.sendCancelGatheringAsync(gatId);
        }
        return res;
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
     *
     * @param stCtx Gathering to complete.
     */
    public void finishStatisticsCollection(StatisticsGatheringContext stCtx) {
        currColls.remove(stCtx.gatId());

        Map<StatisticsKeyMessage, ObjectStatisticsImpl> keysGlobalStats = new HashMap<>();
        for (Map.Entry<StatisticsKeyMessage, Collection<ObjectStatisticsImpl>> keyStats : stCtx.collectedStatistics()
                .entrySet()) {
            ObjectStatisticsImpl globalCollectedStat = statGathering.aggregateLocalStatistics(keyStats.getKey(),
                keyStats.getValue());
            StatisticsKey statsKey = new StatisticsKey(keyStats.getKey().schema(), keyStats.getKey().obj());
            ObjectStatisticsImpl globalStat = statsRepos.mergeGlobalStatistics(statsKey, globalCollectedStat);
            keysGlobalStats.put(keyStats.getKey(), globalStat);
        }

        statCrawler.sendGlobalStatAsync(keysGlobalStats);

        stCtx.doneFut().onDone(keysGlobalStats);
    }

    /**
     * Cache global statistics.
     *
     * @param data Global statistics to cache.
     */
    public void saveGlobalStatistics(Collection<StatisticsObjectData> data) {
        for (StatisticsObjectData objData : data) {
            try {
                ObjectStatisticsImpl objStat = StatisticsUtils.toObjectStatistics(this.ctx, objData);

                statsRepos.saveGlobalStatistics(new StatisticsKey(objData.key().schema(), objData.key().obj()), objStat);
            }
            catch (IgniteCheckedException e) {
                if (log.isDebugEnabled())
                    log.debug(String.format("Cannot read global statistics %s", objData.key()));
            }
        };
    }

    /**
     * Register
     *
     * @param gatId Gathering id.
     * @param data Collected statistics.
     * @param partsCount Count of collected partitions.
     */
    public void registerLocalResult(UUID gatId, Collection<StatisticsObjectData> data, int partsCount) {
        StatisticsGatheringContext stCtx = currColls.get(gatId);
        if (stCtx == null) {
            if (log.isDebugEnabled())
                log.debug(String.format("Unable to register outdated statistics collection result %s", gatId));

            return;
        }
        Map<StatisticsKeyMessage, Collection<ObjectStatisticsImpl>> keyStats = new HashMap<>();
        for (StatisticsObjectData objData : data) {
            Collection<ObjectStatisticsImpl> keyObjStats = keyStats.computeIfAbsent(objData.key(), k -> new ArrayList<>());
            try {
                ObjectStatisticsImpl objStat = StatisticsUtils.toObjectStatistics(ctx, objData);

                keyObjStats.add(objStat);
            }
            catch (IgniteCheckedException e) {
                if (log.isDebugEnabled())
                    log.debug(String.format("Cannot read local statistics %s by gathering task %s", objData.key(),
                        gatId));
            }
        }

        if (stCtx.registerCollected(keyStats, partsCount))
            finishStatisticsCollection(stCtx);
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
}
