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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCachePartitionExchangeManager;
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
    public static final StatisticsColumnConfiguration[] EMPTY_COLUMN_CFGS_ARR = new StatisticsColumnConfiguration[0];

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
    private AtomicBoolean started = new AtomicBoolean(false);

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
                        if (!started.get())
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
                    Map<StatisticsObjectConfiguration, Set<Integer>> cfg = scanAndCheckLocalStatistic(fut.topologyVersion());

                    if (started.compareAndSet(false,true))
                        repo.loadObsolescenceInfo(cfg);
                    else
                        repo.updateObsolescenceInfo(cfg);
                }
            }
        );

        schemaMgr.registerDropColumnsListener(this::onDropColumns);
        schemaMgr.registerDropTable(this::onDropTable);
    }

    /** */
    private Map<StatisticsObjectConfiguration, Set<Integer>> scanAndCheckLocalStatistic(AffinityTopologyVersion topVer) {
        Map<StatisticsObjectConfiguration, Set<Integer>> res = new HashMap<>();

        mgmtPool.submit(() -> {
            try {
                distrMetaStorage.iterate(STAT_OBJ_PREFIX, (k, v) -> {
                    StatisticsObjectConfiguration cfg = (StatisticsObjectConfiguration)v;
                    Set<Integer> parts = checkLocalStatistics(cfg, topVer);
                    res.put(cfg, parts);
                    });
            }
            catch (IgniteCheckedException e) {
                log.warning("Unexpected exception on check local statistic on start", e);
            }
        });
        return res;
    }

    /** */
    public void updateStatistics(List<StatisticsTarget> targets) {
        if (log.isDebugEnabled())
            log.debug("Update statistics [targets=" + targets + ']');

        for (StatisticsTarget target : targets) {
            GridH2Table tbl = schemaMgr.dataTable(target.schema(), target.obj());

            validate(target, tbl);

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
                        Arrays.stream(target.columns()).collect(Collectors.toSet())
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
                throw new IgniteSQLException("Error on get or update statistic schema", IgniteQueryErrorCode.UNKNOWN, ex);
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
            return EMPTY_COLUMN_CFGS_ARR;

        Set<StatisticsColumnConfiguration> cols = Arrays.stream(oldCfg.columns())
            .filter(c -> !colToRemove.contains(c.name())).collect(Collectors.toSet());

        // All not-hidden fields are dropped -> Drop all columns
        if (cols.size() == 2 && cols.contains(QueryUtils.KEY_FIELD_NAME) && cols.contains(QueryUtils.VAL_FIELD_NAME))
            return EMPTY_COLUMN_CFGS_ARR;

        return cols.toArray(EMPTY_COLUMN_CFGS_ARR);
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
                log.warning("Unexpected exception on check local statistic on start", e);
            }
        });
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
    private Set<Integer> checkLocalStatistics(StatisticsObjectConfiguration cfg, final AffinityTopologyVersion topVer) {
        try {
            GridH2Table tbl = schemaMgr.dataTable(cfg.key().schema(), cfg.key().obj());

            if (tbl == null) {
                // Drop tables handle by onDropTable
                return Collections.emptySet();
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

            if (!partsToRemove.isEmpty()) {
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

            return parts;
        }
        catch (IgniteCheckedException ex) {
            log.error("Unexpected error on check local statistics", ex);
        }
        return Collections.emptySet();
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
