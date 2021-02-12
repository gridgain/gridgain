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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.events.DiscoveryCustomEvent;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.DynamicCacheChangeBatch;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCachePartitionExchangeManager;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.ExchangeType;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.PartitionsExchangeAware;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.metastorage.DistributedMetaStorage;
import org.apache.ignite.internal.processors.metastorage.DistributedMetastorageLifecycleListener;
import org.apache.ignite.internal.processors.metastorage.ReadableDistributedMetaStorage;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.h2.SchemaManager;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.stat.config.StatisticsColumnConfiguration;
import org.apache.ignite.internal.processors.query.stat.config.StatisticsObjectConfiguration;
import org.apache.ignite.internal.processors.subscription.GridInternalSubscriptionProcessor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;
import org.gridgain.internal.h2.table.Column;

/**
 *
 */
public class IgniteStatisticsConfigurationManager {
    /** */
    private static final String STAT_OBJ_PREFIX = "sql.statobj.";

    /** */
    public static final String[] EMPTY_STRING_ARR = new String[0];

    /** */
    private final IgniteStatisticsRepository repo;

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

    /** */
    private volatile boolean started;

    /** Monitor to synchronize changes repository: aggregate after collects and drop statistics. */
    private final Object mux = new Object();

    /** */
    public IgniteStatisticsConfigurationManager(
        SchemaManager schemaMgr,
        GridInternalSubscriptionProcessor subscriptionProcessor,
        GridCachePartitionExchangeManager exchange,
        IgniteStatisticsRepository repo,
        StatisticsGatherer gatherer,
        IgniteThreadPoolExecutor mgmtPool,
        Function<Class<?>, IgniteLogger> logSupplier
    ) {
        this.schemaMgr = schemaMgr;
        log = logSupplier.apply(IgniteStatisticsConfigurationManager.class);
        this.repo = repo;
        this.mgmtPool = mgmtPool;
        this.gatherer = gatherer;

        subscriptionProcessor.registerDistributedMetastorageListener(new DistributedMetastorageLifecycleListener() {
            @Override public void onReadyForRead(ReadableDistributedMetaStorage metastorage) {
                distrMetaStorage = (DistributedMetaStorage)metastorage;

                distrMetaStorage.listen(
                    (metaKey) -> metaKey.startsWith(STAT_OBJ_PREFIX),
                    (k, oldV, newV) -> {
                        // Skip invoke on start node (see 'ReadableDistributedMetaStorage#listen' the second case)
                        // The update statistics on start node is handled by 'scanAndCheckLocalStatistic' method
                        // called on exchange done.
                        if (!started)
                            return;

                        mgmtPool.submit(() -> {
                            try {
                                onChangeStatisticConfiguration(
                                    (StatisticsObjectConfiguration)oldV,
                                    (StatisticsObjectConfiguration)newV
                                );
                            }
                            catch (Throwable e) {
                                log.warning("Unexpected exception on change statistic configuration [old="
                                    + oldV + ", new=" + newV + ']', e);
                            }
                        });
                    }
                );
            }
        });

        exchange.registerExchangeAwareComponent(
            new PartitionsExchangeAware() {
                @Override public void onDoneAfterTopologyUnlock(GridDhtPartitionsExchangeFuture fut) {
                    started = true;

                    // Skip join/left client nodes.
                    if (fut.exchangeType() != ExchangeType.ALL)
                        return;

                    // Skip create/destroy caches.
                    if (fut.firstEvent().type() == DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT) {
                        DiscoveryCustomMessage msg = ((DiscoveryCustomEvent)fut.firstEvent()).customMessage();

                        if (msg instanceof DynamicCacheChangeBatch)
                            return;
                    }

                    scanAndCheckLocalStatistic(fut.topologyVersion());
                }
            }
        );

        schemaMgr.registerDropColumnsListener(this::onDropColumns);
        schemaMgr.registerDropTable(this::onDropTable);
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
        if (log.isDebugEnabled())
            log.debug("Update statistics [targets=" + targets + ']');

        for (StatisticsTarget target : targets) {
            GridH2Table tbl = schemaMgr.dataTable(target.schema(), target.obj());

            validate(target, tbl);

            Column[] cols = IgniteStatisticsHelper.filterColumns(
                tbl.getColumns(),
                target.columns() != null ? Arrays.asList(target.columns()) : Collections.emptyList());

            StatisticsColumnConfiguration[] colCfgs = Arrays.stream(cols)
                .map(c -> new StatisticsColumnConfiguration(c.getName()))
                .toArray(StatisticsColumnConfiguration[]::new);

            updateObjectStatisticConfiguration(
                new StatisticsObjectConfiguration(
                    target.key(),
                    colCfgs
                )
            );
        }
    }

    /** */
    public void dropStatistics(List<StatisticsTarget> targets) {
        if (log.isDebugEnabled())
            log.debug("Drop statistics [targets=" + targets + ']');

        for (StatisticsTarget target : targets) {
            String key = key2String(target.key());

            try {
                while (true) {
                    StatisticsObjectConfiguration oldCfg = distrMetaStorage.read(key);

                    if (oldCfg == null)
                        break;

                    StatisticsColumnConfiguration[] newColsCfg = filterColumns(
                        oldCfg,
                        target.columns() != null ?
                            Arrays.stream(target.columns()).collect(Collectors.toSet()) :
                            Collections.emptySet()
                    );

                    StatisticsObjectConfiguration newCfg = new StatisticsObjectConfiguration(
                        target.key(),
                        newColsCfg,
                        oldCfg.version()
                    );

                    if (distrMetaStorage.compareAndSet(key, oldCfg, newCfg))
                        break;
                }
            }
            catch (IgniteCheckedException ex) {
                throw new IgniteSQLException(
                    "Error on get or update statistic schema", IgniteQueryErrorCode.UNKNOWN, ex);
            }
        }
    }

    /** */
    private StatisticsColumnConfiguration[] filterColumns(
        StatisticsObjectConfiguration oldCfg,
        Set<String> colToRemove
    ) {
        // Drop all columns
        if (colToRemove.isEmpty())
            return StatisticsObjectConfiguration.EMPTY_COLUMN_CFGS_ARR;

        Set<StatisticsColumnConfiguration> cols = Arrays.stream(oldCfg.columns())
            .filter(c -> !colToRemove.contains(c.name())).collect(Collectors.toSet());

        return cols.toArray(StatisticsObjectConfiguration.EMPTY_COLUMN_CFGS_ARR);
    }

    /** Drop All statistics. */
    public void dropAll() {
        mgmtPool.submit(() -> {
            try {
                final List<StatisticsTarget> targetsToRemove = new ArrayList<>();

                distrMetaStorage.iterate(STAT_OBJ_PREFIX, (k, v) -> {
                        StatisticsKey statKey = ((StatisticsObjectConfiguration)v).key();

                        targetsToRemove.add(new StatisticsTarget(statKey.schema(), statKey.obj()));
                    }
                );

                dropStatistics(targetsToRemove);
            }
            catch (IgniteCheckedException e) {
                log.warning("Unexpected exception drop all statistics", e);
            }
        });
    }

    /** */
    private void validate(StatisticsTarget target, GridH2Table tbl) {
        if (tbl == null) {
            throw new IgniteSQLException(
                "Table doesn't exist [schema=" + target.schema() + ", table=" + target.obj() + ']',
                IgniteQueryErrorCode.TABLE_NOT_FOUND);
        }

        if (!F.isEmpty(target.columns())) {
            for (String col : target.columns()) {
                if (!tbl.doesColumnExist(col)) {
                    throw new IgniteSQLException(
                        "Column doesn't exist [schema=" + target.schema() +
                            ", table=" + target.obj() +
                            ", column=" + col + ']',
                        IgniteQueryErrorCode.COLUMN_NOT_FOUND);
                }
            }
        }
    }

    /**
     *
     */
    private void updateObjectStatisticConfiguration(StatisticsObjectConfiguration cfg) {
        try {
            while (true) {
                String key = key2String(cfg.key());

                StatisticsObjectConfiguration oldCfg = distrMetaStorage.read(key);

                if (oldCfg != null)
                    cfg = StatisticsObjectConfiguration.merge(oldCfg, cfg);

                if (distrMetaStorage.compareAndSet(key, oldCfg, cfg))
                    return;
            }
        }
        catch (IgniteCheckedException ex) {
            throw new IgniteSQLException("Error on get or update statistic schema", IgniteQueryErrorCode.UNKNOWN, ex);
        }
    }

    /** */
    private void checkLocalStatistics(StatisticsObjectConfiguration cfg, final AffinityTopologyVersion topVer) {
        try {
            GridH2Table tbl = schemaMgr.dataTable(cfg.key().schema(), cfg.key().obj());

            if (tbl == null) {
                // Drop tables handle by onDropTable
                return;
            }

            GridCacheContext cctx = tbl.cacheContext();

            AffinityTopologyVersion topVer0 = cctx.affinity().affinityReadyFuture(topVer).get();

            final Set<Integer> parts = cctx.affinity().primaryPartitions(cctx.localNodeId(), topVer0);

            if (log.isDebugEnabled())
                log.debug("Check local statistics [key=" + cfg.key() + ", parts=" + parts + ']');

            Collection<ObjectPartitionStatisticsImpl> partStats = repo.getLocalPartitionsStatistics(cfg.key());

            Set<String> targetColumns = Arrays.stream(cfg.columns())
                .map(StatisticsColumnConfiguration::name)
                .collect(Collectors.toSet());

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
                    Set<String> existColumns = pstat.columnsStatistics().keySet();

                    if (pstat.version() != cfg.version() || !existColumns.containsAll(targetColumns)) {
                        partsToCollect.add(pstat.partId());

                        continue;
                    }

                    Set<String> colToRemove = new HashSet<>(existColumns);
                    colToRemove.removeAll(targetColumns);

                    if (!F.isEmpty(colToRemove)) {
                        ObjectPartitionStatisticsImpl newPstat = IgniteStatisticsRepository.subtract(pstat, colToRemove);

                        repo.replaceLocalPartitionStatistics(cfg.key(), newPstat);
                    }
                }
            }
            else {
                partsToCollect = parts;
                partsToRemove = Collections.emptySet();
            }

            if (!F.isEmpty(partsToRemove)) {
                if (log.isDebugEnabled()) {
                    log.debug("Remove local partitioned statistics [key=" + cfg.key() +
                        ", part=" + partsToRemove + ']');
                }

                partsToRemove.forEach(p -> repo.clearLocalPartitionStatistics(cfg.key(), p));
            }

            Column[] cols = IgniteStatisticsHelper.filterColumns(tbl.getColumns(), cfg.columns());

            if (!F.isEmpty(partsToCollect)) {
                LocalStatisticsGatheringContext ctx = gatherer.collectLocalObjectsStatisticsAsync(
                    tbl,
                    cols,
                    partsToCollect,
                    cfg.version()
                );

                ctx.future().thenAccept((v) -> onFinishLocalGathering(cfg.key(), parts));
            }
            else
                gatherer.submit(() -> onFinishLocalGathering(cfg.key(), parts));
        }
        catch (IgniteCheckedException ex) {
            log.error("Unexpected error on check local statistics", ex);
        }
    }

    /** */
    private void onChangeStatisticConfiguration(
        StatisticsObjectConfiguration oldCfg,
        final StatisticsObjectConfiguration newCfg
    ) {
        assert oldCfg == null || oldCfg.version() <= newCfg.version() : "Invalid statistic configuration version: " +
            "[old=" + oldCfg + ", new=" + newCfg + ']';

        synchronized (mux) {
            if (log.isDebugEnabled())
                log.debug("Statistic configuration changed [old=" + oldCfg + ", new=" + newCfg + ']');

            if (oldCfg != null && oldCfg.version() == newCfg.version()) {
                if (F.isEmpty(newCfg.columns())) {
                    LocalStatisticsGatheringContext gctx = gatherer.gatheringInProgress(newCfg.key());

                    if (gctx != null)
                        gctx.future().cancel(false);
                }

                // Drop statistics
                Set<String> newCols = Arrays.stream(newCfg.columns())
                    .map(StatisticsColumnConfiguration::name)
                    .collect(Collectors.toSet());

                Set<String> colsToRemove = oldCfg != null ?
                    Arrays.stream(oldCfg.columns())
                        .map(StatisticsColumnConfiguration::name)
                        .filter(c -> !newCols.contains(c))
                        .collect(Collectors.toSet()) :
                    Collections.emptySet();

                if (log.isDebugEnabled()) {
                    log.debug("Remove local statistics [key=" + newCfg.key() +
                        ", columns=" + colsToRemove + ']');
                }

                LocalStatisticsGatheringContext gCtx = gatherer.gatheringInProgress(newCfg.key());

                if (gCtx != null) {
                    gCtx.future().thenAccept((v) -> {
                        repo.clearLocalStatistics(newCfg.key(), colsToRemove);
                        repo.clearLocalPartitionsStatistics(newCfg.key(), colsToRemove);
                    });
                }
                else {
                    repo.clearLocalStatistics(newCfg.key(), colsToRemove);
                    repo.clearLocalPartitionsStatistics(newCfg.key(), colsToRemove);
                }
            }
            else {
                // Update statistics
                GridH2Table tbl = schemaMgr.dataTable(newCfg.key().schema(), newCfg.key().obj());

                GridCacheContext cctx = tbl.cacheContext();

                Set<Integer> parts = cctx.affinity().primaryPartitions(
                    cctx.localNodeId(), cctx.affinity().affinityTopologyVersion());

                Column[] cols = IgniteStatisticsHelper.filterColumns(tbl.getColumns(), newCfg.columns());

                LocalStatisticsGatheringContext ctx = gatherer
                    .collectLocalObjectsStatisticsAsync(tbl, cols, parts, newCfg.version());

                ctx.future().thenAccept((v) -> {
                    onFinishLocalGathering(newCfg.key(), parts);
                });
            }
        }
    }

    /** */
    private void onDropColumns(GridH2Table tbl, List<String> cols) {
        assert !F.isEmpty(cols);

        dropStatistics(Collections.singletonList(
            new StatisticsTarget(
                tbl.getSchema().getName(),
                tbl.getName(),
                cols.toArray(EMPTY_STRING_ARR)
            )
        ));
    }

    /** */
    private void onDropTable(String schema, String name) {
        assert !F.isEmpty(schema) && !F.isEmpty(name) : schema + ":" + name;

        dropStatistics(Collections.singletonList(new StatisticsTarget(schema, name)));
    }

    /** */
    private void onFinishLocalGathering(StatisticsKey key, Set<Integer> partsToAggregate) {
        synchronized (mux) {
            try {
                StatisticsObjectConfiguration cfg = distrMetaStorage.read(key2String(key));

                repo.refreshAggregatedLocalStatistics(partsToAggregate, cfg);
            }
            catch (IgniteCheckedException e) {
                log.error("Error on aggregate statistic on finish local statistics collection" +
                    " [key=" + key + ", parts=" + partsToAggregate, e);
            }
        }
    }

    /** */
    public StatisticsObjectConfiguration config(StatisticsKey key) throws IgniteCheckedException {
        return distrMetaStorage.read(key2String(key));
    }

    /** */
    public List<StatisticsObjectConfiguration> config() throws IgniteCheckedException {
        List<StatisticsObjectConfiguration> cfgs = new ArrayList<>();

        distrMetaStorage.iterate(STAT_OBJ_PREFIX, (k, v) -> cfgs.add((StatisticsObjectConfiguration)v));

        return cfgs;
    }

    /** */
    private static String key2String(StatisticsKey key) {
        StringBuilder sb = new StringBuilder(STAT_OBJ_PREFIX);

        sb.append(key.schema()).append('.').append(key.obj());

        return sb.toString();
    }
}
