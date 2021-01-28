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
import org.apache.ignite.internal.GridKernalContext;
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
import org.apache.ignite.internal.processors.query.stat.ObjectStatisticsImpl;
import org.apache.ignite.internal.processors.query.stat.StatisticsGatherer;
import org.apache.ignite.internal.processors.query.stat.StatisticsKey;
import org.apache.ignite.internal.processors.query.stat.StatisticsTarget;
import org.apache.ignite.internal.processors.subscription.GridInternalSubscriptionProcessor;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;
import org.gridgain.internal.h2.table.Column;

/**
 *
 */
public class IgniteStatisticsConfigurationManager implements DistributedMetastorageLifecycleListener {
    /** */
    private final GridKernalContext ctx;

    /** */
    private static final String STAT_OBJ_PREFIX = "sql.statobj.";

    /** */
    private static final String STAT_CACHE_GRP_PREFIX = "sql.stat.grp.";

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

        subscriptionProcessor.registerDistributedMetastorageListener(this);
    }

    /** {@inheritDoc} */
    @Override public void onReadyForRead(ReadableDistributedMetaStorage metastorage) {
        distrMetaStorage = (DistributedMetaStorage)metastorage;

        distrMetaStorage.listen(
            (metaKey) -> metaKey.startsWith(STAT_CACHE_GRP_PREFIX),
            (k, oldV, newV) -> onUpdateStatisticSchema(k, (CacheGroupStatisticsVersion)oldV, (CacheGroupStatisticsVersion)newV)
        );
    }

    /**
     *
     */
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

            updateObjectStatisticInfo(
                new StatisticsObjectConfiguration(
                    target.key(),
                    colCfgs
                )
            );
        }

        for (int grpId : grpIds)
            updateCacheGroupStatisticVersion(grpId);
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
    private void updateObjectStatisticInfo(StatisticsObjectConfiguration statObjCfg) {
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

    /**
     *
     */
    private void updateCacheGroupStatisticVersion(int cacheGroupId) {
        try {
            while (true) {
                String key = STAT_CACHE_GRP_PREFIX + cacheGroupId;

                CacheGroupStatisticsVersion oldGrp = distrMetaStorage.read(key);

                long ver = 0;

                if (oldGrp != null)
                    ver = oldGrp.version() + 1;

                if (distrMetaStorage.compareAndSet(key, oldGrp, new CacheGroupStatisticsVersion(cacheGroupId, ver)))
                    return;
            }
        }
        catch (IgniteCheckedException ex) {
            throw new IgniteSQLException("Error on get or update statistic schema", IgniteQueryErrorCode.UNKNOWN, ex);
        }
    }

    /** {@inheritDoc} */
    @Override public void onReadyForWrite(DistributedMetaStorage metastorage) {
        mgmtPool.submit(() -> {
            try {
                // TODO: check all
            }
            catch (IgniteCheckedException e) {
                log.warning("Unexpected exception on check local statistic on start", e);
            }
        });
    }


    /** */
    private void updateLocalStatistics(int cacheGrpId) throws IgniteCheckedException {
        Set<StatisticsObjectConfiguration> updSet = new HashSet<>();

        Map<StatisticsKey, Set<String>> rmvColsMap = new HashMap<>();

        distrMetaStorage.iterate(STAT_OBJ_PREFIX + cacheGrpId + '.', (key, val) -> {
            StatisticsObjectConfiguration objCfg = (StatisticsObjectConfiguration)val;
            ObjectStatisticsImpl localStat = localRepo.getLocalStatistics(objCfg.key());

            updSet.add(objCfg);

            if (localStat != null)
                rmvColsMap.putAll(columnsToRemove(objCfg, localStat));
        });

        rmvColsMap.forEach((key, colSet) -> {
            log.info("+++ DELETE:" + rmvColsMap);
            localRepo.clearLocalStatistics(key, colSet.toArray(new String[colSet.size()]));

            localRepo.clearLocalPartitionsStatistics(key, colSet.toArray(new String[colSet.size()]));
        });

        gatherer.collectLocalObjectsStatisticsAsync(cacheGrpId, updSet);
    }

    /** */
    private void onUpdateStatisticSchema(String key, CacheGroupStatisticsVersion ignore, CacheGroupStatisticsVersion grpStat) {
        mgmtPool.submit(() -> {
            try {
                updateLocalStatistics(grpStat.cacheGroupId());
            }
            catch (IgniteCheckedException e) {
                log.warning("Unexpected exception on check local statistic for cache group [grpId="
                    + grpStat.cacheGroupId() + ']', e);
            }
        });
    }

    /** */
    private Map<StatisticsKey, Set<String>> columnsToRemove(StatisticsObjectConfiguration statObjCfg,
        ObjectStatisticsImpl localStat) {
        Set<String> cols = localStat.columnsStatistics().keySet();

        for (StatisticsColumnConfiguration colCfg : statObjCfg.columns())
            cols.remove(colCfg.name());

        return cols.isEmpty() ? Collections.emptyMap() : Collections.singletonMap(statObjCfg.key(), cols);
    }

    /** */
    private static String key2String(StatisticsKey key) {
        StringBuilder sb = new StringBuilder(STAT_OBJ_PREFIX);

        sb.append(key.schema()).append('.').append(key.obj());

        return sb.toString();
    }
}
