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
package org.apache.ignite.internal.processors.query.stat.config;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.PartitionsExchangeAware;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.metastorage.DistributedMetaStorage;
import org.apache.ignite.internal.processors.metastorage.DistributedMetastorageLifecycleListener;
import org.apache.ignite.internal.processors.metastorage.ReadableDistributedMetaStorage;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.h2.SchemaManager;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.stat.IgniteStatisticsHelper;
import org.apache.ignite.internal.processors.query.stat.IgniteStatisticsManager;
import org.apache.ignite.internal.processors.query.stat.IgniteStatisticsRepository;
import org.apache.ignite.internal.processors.query.stat.ObjectPartitionStatisticsImpl;
import org.apache.ignite.internal.processors.query.stat.StatisticsGatherer;
import org.apache.ignite.internal.processors.query.stat.StatisticsKey;
import org.apache.ignite.internal.processors.query.stat.StatisticsTarget;
import org.apache.ignite.internal.processors.subscription.GridInternalSubscriptionProcessor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;
import org.gridgain.internal.h2.table.Column;

/**
 *
 */
public class IgniteStatisticsConfigurationManager {
    /** */
    public static final String[] EMPTY_STR_ARRAY = new String[0];

    /** */
    public static final Column[] EMPTY_COLUMNS_ARR = new Column[0];

    /** */
    private final GridKernalContext ctx;

    /** */
    private static final String STAT_OBJ_PREFIX = "sql.statobj.";

    /** */
    private final IgniteStatisticsRepository localRepo;

    /** */
    private final IgniteStatisticsManager mgr;

    /** Schema manager. */
    private final SchemaManager schemaMgr;

    /** */
    private final StatisticsGatherer gatherer;

    /** */
    private final IgniteThreadPoolExecutor mgmtPool;

    /** Logger. */
    private final IgniteLogger log;

    /** Distributed metastore. */
    private volatile DistributedMetaStorage distrMetaStorage;

    private volatile boolean started;

    /** */
    public IgniteStatisticsConfigurationManager(
        GridKernalContext ctx,
        SchemaManager schemaMgr,
        IgniteStatisticsManager mgr,
        GridInternalSubscriptionProcessor subscriptionProcessor,
        IgniteStatisticsRepository localRepo,
        StatisticsGatherer gatherer,
        IgniteThreadPoolExecutor mgmtPool,
        Function<Class<?>, IgniteLogger> logSupplier
    ) {
        this.ctx = ctx;
        this.schemaMgr = schemaMgr;
        this.mgr = mgr;
        log = logSupplier.apply(IgniteStatisticsConfigurationManager.class);
        this.localRepo = localRepo;
        this.mgmtPool = mgmtPool;
        this.gatherer = gatherer;

        subscriptionProcessor.registerDistributedMetastorageListener(new DistributedMetastorageLifecycleListener() {
            @Override public void onReadyForRead(ReadableDistributedMetaStorage metastorage) {
                distrMetaStorage = (DistributedMetaStorage)metastorage;

                distrMetaStorage.listen(
                    (metaKey) -> metaKey.startsWith(STAT_OBJ_PREFIX),
                    (k, oldV, newV) ->  {
                        // Skip invoke on start node (see 'ReadableDistributedMetaStorage#listen' the second case)
                        // The update statistics on start node is handled by 'scanAndCheckLocalStatistic' method
                        // called on exchange done.
                        if (!started)
                            return;

                        mgmtPool.submit(() -> {
                            try {
                                onUpdateStatisticConfiguration(
                                    (StatisticsObjectConfiguration)oldV,
                                    (StatisticsObjectConfiguration)newV
                                );
                            }
                            catch (Throwable e) {
                                log.warning("Unexpected exception on check local statistic for cache group [old="
                                    + oldV + ", new=" + newV + ']');
                            }
                        });
                    }
                );
            }
        });

        ctx.cache().context().exchange().registerExchangeAwareComponent(
            new PartitionsExchangeAware() {
                @Override public void onDoneAfterTopologyUnlock(GridDhtPartitionsExchangeFuture fut) {
                    started = true;

                    scanAndCheckLocalStatistic(fut.topologyVersion());
                }
            }
        );
    }

    /** */
    private void scanAndCheckLocalStatistic(AffinityTopologyVersion topVer) {
        mgmtPool.submit(() -> {
            try {
                distrMetaStorage.iterate(STAT_OBJ_PREFIX, (k, v) ->
                    checkLocalStatistics((StatisticsObjectConfiguration)v, topVer));
            }
            catch (IgniteCheckedException e) {
                log.warning("Unexpected exception on check local statistic on start", e);
            }
        });
    }

    /** */
    public void updateStatistics(List<StatisticsTarget> targets) {
        Set<Integer> grpIds = new HashSet<>();

        for (StatisticsTarget target : targets) {
            GridH2Table tbl = schemaMgr.dataTable(target.schema(), target.obj());

            validate(target, tbl);

            grpIds.add(tbl.cacheContext().groupId());

            Column[] cols = IgniteStatisticsHelper.filterColumns(tbl.getColumns(), Arrays.asList(target.columns()));

            StatisticsColumnConfiguration[] colCfgs = Arrays.stream(cols)
                .map(c -> new StatisticsColumnConfiguration(c.getName()))
                .collect(Collectors.toList())
                .toArray(new StatisticsColumnConfiguration[cols.length]);

            updateObjectStatisticConfiguration(
                new StatisticsObjectConfiguration(
                    target.key(),
                    colCfgs
                )
            );
        }
    }

    /** */
    private void validate(StatisticsTarget target, GridH2Table tbl) {
        if (tbl == null) {
            throw new IgniteSQLException(
                "Table doesn't exist [schema=" + target.schema() + ", table=" + target.obj() + ']');
        }

        for (String col : target.columns()) {
            if (tbl.getColumn(col) == null) {
                throw new IgniteSQLException(
                    "Column doesn't exist [schema=" + target.schema() + ", table=" + target.obj() + ", column=" + col + ']');
            }
        }
    }

    /**
     *
     */
    private void updateObjectStatisticConfiguration(StatisticsObjectConfiguration statObjCfg) {
        try {
            while (true) {
                String key = key2String(statObjCfg.key());

                StatisticsObjectConfiguration oldCfg = distrMetaStorage.read(key);

                if (oldCfg != null)
                    statObjCfg = StatisticsObjectConfiguration.merge(statObjCfg, oldCfg);

                if (distrMetaStorage.compareAndSet(key, oldCfg, statObjCfg))
                    return;
            }
        }
        catch (IgniteCheckedException ex) {
            throw new IgniteSQLException("Error on get or update statistic schema", IgniteQueryErrorCode.UNKNOWN, ex);
        }
    }

    /** */
    private void checkLocalStatistics(StatisticsObjectConfiguration cfg, AffinityTopologyVersion topVer) {
        try {
            GridH2Table tbl = schemaMgr.dataTable(cfg.key().schema(), cfg.key().obj());
            GridCacheContext cctx = tbl.cacheContext();

            topVer = cctx.affinity().affinityReadyFuture(topVer).get();

            final Set<Integer> parts = cctx.affinity().primaryPartitions(cctx.localNodeId(), topVer);

            if (log.isDebugEnabled())
                log.debug("Check local statistics [key=" + cfg.key() + ", parts=" + parts + ']');

            Collection<ObjectPartitionStatisticsImpl> partStats = localRepo.getLocalPartitionsStatistics(cfg.key());

            Set<Integer> partsToRemove;
            Set<Integer> partsToCollect;

            if (!F.isEmpty(partStats)) {
                Set<Integer> storedParts = partStats.stream()
                    .mapToInt(ObjectPartitionStatisticsImpl::partId)
                    .boxed()
                    .collect(Collectors.toSet());

                partsToRemove = new HashSet<>(storedParts);
                partsToRemove.removeAll(parts);

                partsToCollect = new HashSet<>(parts);
                partsToCollect.removeAll(storedParts);

                for (ObjectPartitionStatisticsImpl pstat : partStats) {
                    if (pstat.version() != cfg.version())
                        partsToCollect.add(pstat.partId());
                }
            }
            else {
                partsToCollect = parts;
                partsToRemove = Collections.emptySet();
            }

            if (log.isDebugEnabled()) {
                log.debug("Remove local partitioned statistics [key=" + cfg.key() +
                    ", part=" + partsToRemove + ']');
            }

            partsToRemove.forEach(p -> localRepo.clearLocalPartitionStatistics(cfg.key(), p));

            Column[] cols = IgniteStatisticsHelper.filterColumns(tbl.getColumns(), cfg.columns());

            if (!F.isEmpty(partsToCollect)) {
                CompletableFuture<Void> f = gatherer.collectLocalObjectsStatisticsAsync(
                    tbl,
                    cols,
                    partsToCollect,
                    cfg.version()
                );

                f.thenAccept((v) -> localRepo.refreshAggregatedLocalStatistics(parts, cfg));
            }
            else {
                // TODO: error handling
                gatherer.submit(() -> localRepo.refreshAggregatedLocalStatistics(parts, cfg));
            }
        }
        catch (IgniteCheckedException ex) {
            log.error("Unexpected error on check local statistics", ex);
        }
    }

    /** */
    private void onUpdateStatisticConfiguration(
        StatisticsObjectConfiguration oldCfg,
        final StatisticsObjectConfiguration newCfg
    ) throws IgniteCheckedException {
        Set<String> newCols = Arrays.stream(newCfg.columns()).map(StatisticsColumnConfiguration::name).collect(Collectors.toSet());
        Set<String> oldCols = oldCfg != null ?
            Arrays.stream(oldCfg.columns()).map(StatisticsColumnConfiguration::name).collect(Collectors.toSet()) :
            Collections.emptySet();

        oldCols.removeAll(newCols);

        String[] rmCols = oldCols.toArray(EMPTY_STR_ARRAY);

        localRepo.clearLocalStatistics(newCfg.key(), rmCols);
        localRepo.clearLocalPartitionsStatistics(newCfg.key(), rmCols);

        ctx.cache().awaitStarted();

        GridH2Table tbl = schemaMgr.dataTable(newCfg.key().schema(), newCfg.key().obj());
        GridCacheContext cctx = tbl.cacheContext();

        cctx.affinity().affinityReadyFuture(cctx.affinity().affinityTopologyVersion()).get();

        Set<Integer> parts = cctx.affinity().primaryPartitions(cctx.localNodeId(), cctx.affinity().affinityTopologyVersion());

        Column[] cols = IgniteStatisticsHelper.filterColumns(tbl.getColumns(), newCfg.columns());

        CompletableFuture<Void> f = gatherer.collectLocalObjectsStatisticsAsync(tbl, cols, parts, newCfg.version());

        f.thenAccept((v) -> {
            localRepo.refreshAggregatedLocalStatistics(parts, newCfg);
        });
    }


    /** */
    private static String key2String(StatisticsKey key) {
        StringBuilder sb = new StringBuilder(STAT_OBJ_PREFIX);

        sb.append(key.schema()).append('.').append(key.obj());

        return sb.toString();
    }
}
