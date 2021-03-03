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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.BiConsumer;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteFeatures;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.managers.discovery.IgniteDiscoverySpi;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheUtils;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedEnumProperty;
import org.apache.ignite.internal.processors.query.h2.SchemaManager;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.stat.config.StatisticsObjectConfiguration;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;
import org.gridgain.internal.h2.table.Column;

import static org.apache.ignite.internal.processors.query.stat.StatisticsUsageState.NO_UPDATE;
import static org.apache.ignite.internal.processors.query.stat.StatisticsUsageState.OFF;
import static org.apache.ignite.internal.processors.query.stat.StatisticsUsageState.ON;

/**
 * Statistics manager implementation.
 */
public class IgniteStatisticsManagerImpl implements IgniteStatisticsManager {
    /** Size of statistics collection pool. */
    private static final int STATS_POOL_SIZE = 4;

    /** Default statistics usage state. */
    private static final StatisticsUsageState DEFAULT_STATISTICS_USAGE_STATE = StatisticsUsageState.ON;

    /** Interval to check statistics obsolescence in seconds. */
    private static final int OBSOLESCENSE_INTERVAL = 60;

    /** Rows limit to renew partition statistics in percent. */
    private static final int OBSOLESCENSE_MAX_PERCENT = 15;

    /** Logger. */
    private final IgniteLogger log;

    /** Kernal context. */
    private final GridKernalContext ctx;

    /** SchemaManager */
    private final SchemaManager schemaMgr;

    /** Statistics repository. */
    private final IgniteStatisticsRepository statsRepos;

    /** Ignite statistics helper. */
    private final IgniteStatisticsHelper helper;

    /** Statistics collector. */
    private final StatisticsGatherer gatherer;

    /** Statistics configuration manager. */
    private final IgniteStatisticsConfigurationManager statCfgMgr;

    /** Management pool. */
    private final IgniteThreadPoolExecutor mgmtPool;

    /** Gathering pool. */
    private final IgniteThreadPoolExecutor gatherPool;

    /** Cluster wide statistics usage state. */
    private final DistributedEnumProperty<StatisticsUsageState> usageState = new DistributedEnumProperty<>(
        "statistics.usage.state", StatisticsUsageState::fromOrdinal, StatisticsUsageState::index, StatisticsUsageState.class);

    /** Drop columns listener. */
    private final BiConsumer<GridH2Table, List<String>> dropColsLsnr = new BiConsumer<GridH2Table, List<String>>() {
        /**
         * Drop statistics after columns dropped.
         *
         * @param tbl  Table.
         * @param cols Dropped columns.
         */
        @Override
        public void accept(GridH2Table tbl, List<String> cols) {

        }
    };

    /** Drop table listener. */
    private final BiConsumer<String, String> dropTblLsnr = new BiConsumer<String, String>() {
        /**
         * Drop statistics after table dropped.
         *
         * @param schema Schema name.
         * @param name Table name.
         */
        @Override public void accept(String schema, String name) {
            statsRepos.updateObsolescenceInfo();
        }
    };


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

        gatherPool = new IgniteThreadPoolExecutor("stat-gather",
                ctx.igniteInstanceName(),
                0,
                STATS_POOL_SIZE,
                IgniteConfiguration.DFLT_THREAD_KEEP_ALIVE_TIME,
                new LinkedBlockingQueue<>(),
                GridIoPolicy.UNDEFINED,
                ctx.uncaughtExceptionHandler()
        );

        mgmtPool = new IgniteThreadPoolExecutor("stat-mgmt",
                ctx.igniteInstanceName(),
                0,
                1,
                IgniteConfiguration.DFLT_THREAD_KEEP_ALIVE_TIME,
                new LinkedBlockingQueue<>(),
                GridIoPolicy.UNDEFINED,
                ctx.uncaughtExceptionHandler()
        );

        boolean storeData = !(ctx.config().isClientMode() || ctx.isDaemon());
        IgniteStatisticsStore store;
        if (!storeData)
            store = new IgniteStatisticsDummyStoreImpl(ctx::log);
        else if (db == null)
            store = new IgniteStatisticsInMemoryStoreImpl(ctx::log);
        else
            store = new IgniteStatisticsPersistenceStoreImpl(ctx.internalSubscriptionProcessor(), db, ctx::log);

        statsRepos = new IgniteStatisticsRepository(store, helper, ctx::log);

        gatherer = new StatisticsGatherer(
            statsRepos,
            gatherPool,
            ctx::log
        );

        statCfgMgr = new IgniteStatisticsConfigurationManager(
            schemaMgr,
            ctx.internalSubscriptionProcessor(),
            ctx.cache().context().exchange(),
            statsRepos,
            gatherer,
            mgmtPool,
            ctx::log
        );

        ctx.internalSubscriptionProcessor().registerDistributedConfigurationListener(dispatcher -> {
            usageState.addListener((name, oldVal, newVal) -> {
                if (log.isInfoEnabled())
                    log.info(String.format("Statistics usage state was changed from %s to %s", oldVal, newVal));

                if (oldVal == newVal)
                    return;

                switch (newVal) {
                    case OFF:
                        disableOperations();

                        break;
                    case ON:
                    case NO_UPDATE:
                        enableOperations();

                        break;
                }
            });

            dispatcher.registerProperty(usageState);
        });

        StatisticsUsageState currState = usageState();
        if (currState == ON || currState == NO_UPDATE)
            enableOperations();
    }

    /**
     * Enable statistics operations.
     */
    private void enableOperations() {
        statsRepos.start();
        gatherer.start();
        statCfgMgr.start();

        schemaMgr.registerDropColumnsListener(dropColsLsnr);
        schemaMgr.registerDropTableListener(dropTblLsnr);
    }

    /**
     * Disable statistics operations.
     */
    private void disableOperations() {
        schemaMgr.unregisterDropTableListener(dropTblLsnr);
        schemaMgr.unregisterDropColumnsListener(dropColsLsnr);

        statCfgMgr.stop();
        gatherer.stop();
        statsRepos.start();
    }

    /**
     * @return Statistics repository.
     */
    public IgniteStatisticsRepository statisticsRepository() {
        return statsRepos;
    }

    /** {@inheritDoc} */
    @Override public ObjectStatistics getLocalStatistics(StatisticsKey key) {
        StatisticsUsageState currState = usageState();

        return (currState == ON || currState == NO_UPDATE) ? statsRepos.getLocalStatistics(key) : null;
    }

    /** {@inheritDoc} */
    @Override public void collectStatistics(StatisticsTarget... targets) throws IgniteCheckedException {
        checkStatisticsSupport("collect statistics");

        if (usageState() == OFF)
            throw new IgniteException("Can't gather statistics while statistics usage state is OFF.");

        statCfgMgr.updateStatistics(Arrays.asList(targets));
    }

    /** {@inheritDoc} */
    @Override public void dropStatistics(StatisticsTarget... targets) throws IgniteCheckedException {
        checkStatisticsSupport("drop statistics");

        if (usageState() == OFF)
            throw new IgniteException("Can't drop statistics while statistics usage state is OFF.");

        statCfgMgr.dropStatistics(Arrays.asList(targets), true);
    }

    /** {@inheritDoc} */
    @Override public void refreshStatistics(StatisticsTarget... targets) throws IgniteCheckedException {
        checkStatisticsSupport("collect statistics");

        if (usageState() == OFF)
            throw new IgniteException("Can't refresh statistics while statistics usage state is OFF.");

        statCfgMgr.refreshStatistics(Arrays.asList(targets));
    }

    /** {@inheritDoc} */
    @Override public void dropAll() throws IgniteCheckedException {
        checkStatisticsSupport("drop all statistics");

        statCfgMgr.dropAll();
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        statCfgMgr.stop();
        gatherer.stop();

        if (gatherPool != null) {
            List<Runnable> unfinishedTasks = gatherPool.shutdownNow();
            if (!unfinishedTasks.isEmpty())
                log.warning(String.format("%d statistics collection cancelled.", unfinishedTasks.size()));
        }

        if (mgmtPool != null) {
            List<Runnable> unfinishedTasks = mgmtPool.shutdownNow();
            if (!unfinishedTasks.isEmpty())
                log.warning(String.format("%d statistics configuration change handler cancelled.", unfinishedTasks.size()));
        }
    }

   /** */
    public IgniteStatisticsConfigurationManager statisticConfiguration() {
        return statCfgMgr;
    }

    /** {@inheritDoc} */
    @Override public void usageState(StatisticsUsageState state) throws IgniteCheckedException {
        checkStatisticsSupport("clear statistics");

        try {
            usageState.propagate(state);
        }
        catch (IgniteCheckedException e) {
            log.error("Unable to set usage state value due to " + e.getMessage(), e);
        }
    }

    /** {@inheritDoc} */
    @Override public StatisticsUsageState usageState() {
        return usageState.getOrDefault(DEFAULT_STATISTICS_USAGE_STATE);
    }

    /** {@inheritDoc} */
    @Override public void onRowUpdated(String schemaName, String objName, int partId, byte[] keyBytes) {
        try {
            if (statCfgMgr.config(new StatisticsKey(schemaName, objName)) != null)
                statsRepos.addRowsModified(new StatisticsKey(schemaName, objName), partId, keyBytes);
        }
        catch (IgniteCheckedException e) {
            if (log.isInfoEnabled())
                log.info(String.format("Error while obsolescence key in %s.%s due to %s", schemaName, objName,
                    e.getMessage()));
        }
    }

    /**
     *
     */
    private void processObscolescence() {
        Map<StatisticsKey, Map<Integer, ObjectPartitionStatisticsObsolescence>> dirty = statsRepos.saveObsolescenceInfo();
        Map<StatisticsKey, List<Integer>> task = new HashMap<>();

        for (Map.Entry<StatisticsKey, Map<Integer, ObjectPartitionStatisticsObsolescence>> objObs : dirty.entrySet()) {
            StatisticsKey key = objObs.getKey();
            List<Integer> taskParts = new ArrayList<>();

            for (Map.Entry<Integer, ObjectPartitionStatisticsObsolescence> objPartObs : objObs.getValue().entrySet()) {
                ObjectPartitionStatisticsImpl partStat = statsRepos.getLocalPartitionStatistics(key, objPartObs.getKey());
                if (partStat == null || partStat.rowCount() == 0 ||
                    (double)objPartObs.getValue().modified() * 100 / partStat.rowCount() > OBSOLESCENSE_MAX_PERCENT)
                    taskParts.add(objPartObs.getKey());
            }

            if (!taskParts.isEmpty())
                task.put(key, taskParts);
        }

        for (Map.Entry<StatisticsKey, List<Integer>> objTask : task.entrySet()) {
            GridH2Table tbl = schemaMgr.dataTable(objTask.getKey().schema(), objTask.getKey().obj());
            if (tbl == null) {
                if (log.isDebugEnabled())
                    log.debug(String.format("Got obsolescence statistics for unknown table %s", objTask.getKey()));

                continue;
            }
            StatisticsObjectConfiguration objCfg;
            try {
                objCfg = statCfgMgr.config(objTask.getKey());
            } catch (IgniteCheckedException e) {
                log.warning("Unable to load statistics object configuration from global metastore", e);
                continue;
            }
            if (objCfg == null) {
                if (log.isDebugEnabled())
                    log.debug(String.format("Got obsolescence statistics for unknown configuration %s", objTask.getKey()));

                continue;
            }

            GridCacheContext cctx = tbl.cacheContext();

            Set<Integer> parts = cctx.affinity().primaryPartitions(
                cctx.localNodeId(), cctx.affinity().affinityTopologyVersion());

            Column[] cols = IgniteStatisticsHelper.filterColumns(tbl.getColumns(), objCfg.columns());

            LocalStatisticsGatheringContext ctx = gatherer
                .collectLocalObjectsStatisticsAsync(tbl, cols, new HashSet<>(objTask.getValue()), objCfg.version());


            ctx.future().thenAccept((v) -> statsRepos.refreshAggregatedLocalStatistics(parts, objCfg));
        }
    }

    /**
     * Check that all server nodes in the cluster support STATISTICS_COLLECTION feature. Throws IgniteCheckedException
     * in not.
     *
     * @param op Operation name.
     * @throws IgniteCheckedException If at least one server node doesn't support feature.
     */
   private void checkStatisticsSupport(String op) throws IgniteCheckedException {
       if (!isStatisticsSupport()) {
           throw new IgniteCheckedException(String.format(
               "Unable to perform %s due to not all server nodes supports STATISTICS_COLLECTION feature.", op));
       }
   }

    /**
     * Test is statistics collection feature are supported by each server node in cluster.
     *
     * @return {@code true} if all server nodes support STATISTICS_COLLECTION feature, {@code false} - otherwise.
     */
   private boolean isStatisticsSupport() {
       return IgniteFeatures.allNodesSupport(ctx, IgniteFeatures.STATISTICS_COLLECTION, IgniteDiscoverySpi.SRV_NODES);
   }
}
