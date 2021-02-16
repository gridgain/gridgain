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
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.NodeStoppingException;
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
import org.apache.ignite.internal.processors.query.h2.SchemaManager;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.stat.config.StatisticsColumnConfiguration;
import org.apache.ignite.internal.processors.query.stat.config.StatisticsObjectConfiguration;
import org.apache.ignite.internal.processors.subscription.GridInternalSubscriptionProcessor;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
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

    /** Busy lock on node stop. */
    private final GridSpinBusyLock stopLock;

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
        GridSpinBusyLock stopLock,
        Function<Class<?>, IgniteLogger> logSupplier
    ) {
        this.schemaMgr = schemaMgr;
        log = logSupplier.apply(IgniteStatisticsConfigurationManager.class);
        this.repo = repo;
        this.mgmtPool = mgmtPool;
        this.gatherer = gatherer;
        this.stopLock = stopLock;

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

                    DiscoveryEvent evt = fut.firstEvent();

                    // Skip create/destroy caches.
                    if (evt.type() == DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT) {
                        DiscoveryCustomMessage msg = ((DiscoveryCustomEvent)evt).customMessage();

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
            if (!stopLock.enterBusy())
                return;

            try {
                distrMetaStorage.iterate(STAT_OBJ_PREFIX, (k, v) ->
                    checkLocalStatistics((StatisticsObjectConfiguration)v, topVer));
            }
            catch (IgniteCheckedException e) {
                log.warning("Unexpected exception on check local statistic on start", e);
            }
            finally {
                stopLock.leaveBusy();
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

            List<StatisticsColumnConfiguration> colCfgs = Arrays.stream(cols)
                .map(c -> new StatisticsColumnConfiguration(c.getName()))
                .collect(Collectors.toList());

            StatisticsObjectConfiguration newCfg = new StatisticsObjectConfiguration(target.key(), colCfgs);

            try {
                while (true) {
                    String key = key2String(target.key());

                    StatisticsObjectConfiguration oldCfg = distrMetaStorage.read(key);

                    if (oldCfg != null)
                        newCfg = StatisticsObjectConfiguration.merge(oldCfg, newCfg);

                    if (distrMetaStorage.compareAndSet(key, oldCfg, newCfg))
                        break;
                }
            }
            catch (IgniteCheckedException ex) {
                throw new IgniteSQLException("Error on get or update statistic schema", IgniteQueryErrorCode.UNKNOWN, ex);
            }
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

                    StatisticsObjectConfiguration newCfg = oldCfg.dropColumns(
                        target.columns() != null ?
                            Arrays.stream(target.columns()).collect(Collectors.toSet()) :
                            Collections.emptySet()
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

    /** */
    private void checkLocalStatistics(StatisticsObjectConfiguration cfg, final AffinityTopologyVersion topVer) {
        if (!stopLock.enterBusy())
            return;

        try {
            GridH2Table tbl = schemaMgr.dataTable(cfg.key().schema(), cfg.key().obj());

            if (tbl == null) {
                // Drop tables handle by onDropTable
                return;
            }

            GridCacheContext cctx = tbl.cacheContext();

            AffinityTopologyVersion topVer0 = cctx.affinity().affinityReadyFuture(topVer).get();

            final Set<Integer> parts = cctx.affinity().primaryPartitions(cctx.localNodeId(), topVer0);

            if (F.isEmpty(parts)) {
                // There is no data on the node for specified cache.
                return;
            }

            final Set<Integer> partsOwn = new HashSet<>(cctx.affinity().backupPartitions(cctx.localNodeId(), topVer0));

            partsOwn.addAll(parts);

            if (log.isDebugEnabled())
                log.debug("Check local statistics [key=" + cfg + ", parts=" + parts + ']');

            Collection<ObjectPartitionStatisticsImpl> partStats = repo.getLocalPartitionsStatistics(cfg.key());

            Set<Integer> partsToRmv = new HashSet<>();
            Set<Integer> partsToCollect = new HashSet<>(parts);
            Map<String, StatisticsColumnConfiguration> colsToCollect = new HashMap<>();
            Set<String> colsToRmv = new HashSet<>();

            if (!F.isEmpty(partStats)) {
                for (ObjectPartitionStatisticsImpl pstat : partStats) {
                    if (!partsOwn.contains(pstat.partId()))
                        partsToRmv.add(pstat.partId());

                    boolean partExists = true;

                    for (StatisticsColumnConfiguration colCfg : cfg.columnsAll().values()) {
                        ColumnStatistics colStat = pstat.columnStatistics(colCfg.name());

                        if (colCfg.tombstone()) {
                            if (colStat != null)
                                colsToRmv.add(colCfg.name());
                        }
                        else {
                            if (colStat == null || colStat.version() < colCfg.version()) {
                                colsToCollect.put(colCfg.name(), colCfg);

                                partsToCollect.add(pstat.partId());

                                partExists = false;
                            }
                        }
                    }

                    if (partExists)
                        partsToCollect.remove(pstat.partId());
                }
            }


            if (!F.isEmpty(partsToRmv)) {
                if (log.isDebugEnabled()) {
                    log.debug("Remove local partitioned statistics [key=" + cfg.key() +
                        ", part=" + partsToRmv + ']');
                }

                partsToRmv.forEach(p -> {
                    assert !partsToCollect.contains(p);

                    repo.clearLocalPartitionStatistics(cfg.key(), p);
                });
            }

            if (!F.isEmpty(colsToRmv)) {
                if (log.isDebugEnabled()) {
                    log.debug("Remove local partitioned statistics [key=" + cfg.key() +
                        ", cols=" + colsToRmv + ']');
                }

                repo.clearLocalPartitionsStatistics(cfg.key(), colsToRmv);
            }

            if (!F.isEmpty(partsToCollect)) {
                if (F.isEmpty(colsToCollect))
                    colsToCollect = cfg.columns();

                Column[] cols = IgniteStatisticsHelper.filterColumns(tbl.getColumns(), colsToCollect.keySet());

                LocalStatisticsGatheringContext ctx = gatherer.collectLocalObjectsStatisticsAsync(
                    tbl,
                    cols,
                    colsToCollect,
                    partsToCollect
                );

                ctx.future().thenAccept((v) -> onFinishLocalGathering(cfg.key(), parts));
            }
            else {
                // All local statistics by partition are available.
                // Only refresh aggregated local statistics.
                gatherer.submit(() -> onFinishLocalGathering(cfg.key(), parts));
            }
        }
        catch (Throwable ex) {
            log.error("Unexpected error on check local statistics", ex);
        }
        finally {
            stopLock.leaveBusy();
        }
    }

    /** */
    private void onChangeStatisticConfiguration(
        StatisticsObjectConfiguration oldCfg,
        final StatisticsObjectConfiguration newCfg
    ) {
        synchronized (mux) {
            if (log.isDebugEnabled())
                log.debug("Statistic configuration changed [old=" + oldCfg + ", new=" + newCfg + ']');

            StatisticsObjectConfiguration.Diff diff = StatisticsObjectConfiguration.diff(oldCfg, newCfg);

            if (!F.isEmpty(diff.dropCols())) {
                if (log.isDebugEnabled()) {
                    log.debug("Remove local statistics [key=" + newCfg.key() +
                        ", columns=" + diff.dropCols() + ']');
                }

                LocalStatisticsGatheringContext gCtx = gatherer.gatheringInProgress(newCfg.key());

                if (gCtx != null) {
                    gCtx.future().thenAccept((v) -> {
                        repo.clearLocalStatistics(newCfg.key(), diff.dropCols());
                        repo.clearLocalPartitionsStatistics(newCfg.key(), diff.dropCols());
                    });
                }
                else {
                    repo.clearLocalStatistics(newCfg.key(), diff.dropCols());
                    repo.clearLocalPartitionsStatistics(newCfg.key(), diff.dropCols());
                }
            }

            if (!F.isEmpty(diff.updateCols())) {
                GridH2Table tbl = schemaMgr.dataTable(newCfg.key().schema(), newCfg.key().obj());

                GridCacheContext cctx = tbl.cacheContext();

                Set<Integer> parts = cctx.affinity().primaryPartitions(
                    cctx.localNodeId(), cctx.affinity().affinityTopologyVersion());

                Column[] cols = IgniteStatisticsHelper.filterColumns(tbl.getColumns(), diff.updateCols().keySet());

                LocalStatisticsGatheringContext ctx = gatherer
                    .collectLocalObjectsStatisticsAsync(tbl, cols, diff.updateCols(), parts);

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
            if (!stopLock.enterBusy())
                return;

            try {
                StatisticsObjectConfiguration cfg = distrMetaStorage.read(key2String(key));

                repo.refreshAggregatedLocalStatistics(partsToAggregate, cfg);
            }
            catch (Throwable e) {
                if (!X.hasCause(e, NodeStoppingException.class)) {
                    log.error("Error on aggregate statistic on finish local statistics collection" +
                        " [key=" + key + ", parts=" + partsToAggregate, e);
                }
            }
            finally {
                stopLock.leaveBusy();
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
