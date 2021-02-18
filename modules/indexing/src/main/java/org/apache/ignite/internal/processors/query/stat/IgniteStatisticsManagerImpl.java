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

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteFeatures;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.managers.discovery.IgniteDiscoverySpi;
import org.apache.ignite.internal.processors.cache.GridCacheUtils;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.query.h2.SchemaManager;
import org.apache.ignite.internal.util.GridBusyLock;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;

/**
 * Statistics manager implementation.
 */
public class IgniteStatisticsManagerImpl implements IgniteStatisticsManager {
    /** Size of statistics collection pool. */
    private static final int STATS_POOL_SIZE = 4;

    /** Logger. */
    private final IgniteLogger log;

    /** Kernal context. */
    private final GridKernalContext ctx;

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

    /** Busy lock on node stop. */
    private final GridSpinBusyLock stopLock;

    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     * @param schemaMgr Schema manager.
     */
    public IgniteStatisticsManagerImpl(GridKernalContext ctx, SchemaManager schemaMgr) {
        this.ctx = ctx;
        stopLock = new GridSpinBusyLock();

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
            stopLock,
            ctx::log
        );

        statCfgMgr = new IgniteStatisticsConfigurationManager(
            schemaMgr,
            ctx.internalSubscriptionProcessor(),
            ctx.cache().context().exchange(),
            statsRepos,
            gatherer,
            mgmtPool,
            stopLock,
            ctx::log
        );
    }

    /**
     * @return Statistics repository.
     */
    public IgniteStatisticsRepository statisticsRepository() {
        return statsRepos;
    }

    /** {@inheritDoc} */
    @Override public ObjectStatistics getLocalStatistics(StatisticsKey key) {
        return statsRepos.getLocalStatistics(key);
    }

    /** {@inheritDoc} */
    @Override public void updateStatistics(StatisticsTarget... targets) throws IgniteCheckedException {
        checkStatisticsSupport("collect statistics");

        statCfgMgr.updateStatistics(Arrays.asList(targets));
    }

    /** {@inheritDoc} */
    @Override public void dropStatistics(StatisticsTarget... targets) throws IgniteCheckedException {
        checkStatisticsSupport("drop statistics");

        statCfgMgr.dropStatistics(Arrays.asList(targets));
    }

    /** {@inheritDoc} */
    @Override public void dropAll() throws IgniteCheckedException {
        checkStatisticsSupport("drop all statistics");

        statCfgMgr.dropAll();
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        stopLock.block();

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
