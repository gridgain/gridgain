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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.metastorage.DistributedMetaStorage;
import org.apache.ignite.internal.processors.metastorage.DistributedMetastorageLifecycleListener;
import org.apache.ignite.internal.processors.metastorage.ReadableDistributedMetaStorage;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.h2.SchemaManager;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.stat.IgniteStatisticsManager;
import org.apache.ignite.internal.processors.query.stat.IgniteStatisticsRepository;
import org.apache.ignite.internal.processors.query.stat.ObjectStatisticsImpl;
import org.apache.ignite.internal.processors.query.stat.StatisticsKey;
import org.apache.ignite.internal.processors.query.stat.StatisticsTarget;
import org.apache.ignite.internal.processors.subscription.GridInternalSubscriptionProcessor;

/**
 *
 */
public class IgniteStatisticsConfigurationManager implements DistributedMetastorageLifecycleListener {
    /** */
    private static final String STAT_OBJ_PREFIX = "sql.statobj.";

    /** */
    private static final String STAT_CACHE_GRP_PREFIX = "sql.stat.grp.";

    /** */
    private final GridKernalContext ctx;

    /** */
    private final IgniteStatisticsRepository localRepo;

    /** */
    private final IgniteStatisticsManager mgr;

    /** Schema manager. */
    private final SchemaManager schemaMgr;

    /** Logger. */
    private final IgniteLogger log;

    /** Distributed metastore. */
    private volatile DistributedMetaStorage distrMetaStorage;

    /**
     *
     */
    public IgniteStatisticsConfigurationManager(
        GridKernalContext ctx,
        SchemaManager schemaMgr,
        IgniteStatisticsManager mgr,
        GridInternalSubscriptionProcessor subscriptionProcessor,
        IgniteStatisticsRepository localRepo,
        Function<Class<?>, IgniteLogger> logSupplier
    ) {
        this.ctx = ctx;
        this.schemaMgr = schemaMgr;
        this.mgr = mgr;
        this.distrMetaStorage = distrMetaStorage;
        log = logSupplier.apply(IgniteStatisticsConfigurationManager.class);
        this.localRepo = localRepo;

        subscriptionProcessor.registerDistributedMetastorageListener(this);
    }

    /** {@inheritDoc} */
    @Override public void onReadyForRead(ReadableDistributedMetaStorage metastorage) {
        distrMetaStorage = ctx.distributedMetastorage();

        distrMetaStorage.listen(
            (metaKey) -> metaKey.startsWith(STAT_CACHE_GRP_PREFIX),
            (k, oldV, newV) -> onUpdateStatisticSchema(k, (CacheGroupStatisticsVersion)oldV, (CacheGroupStatisticsVersion)newV)
        );
    }

    /**
     *
     */
    public void updateStatistics(List<StatisticsTarget> targets, StatisticsCollectConfiguration cfg) {
        Set<Integer> grpIds = new HashSet<>();

        for (StatisticsTarget target : targets) {
            GridH2Table tbl = schemaMgr.dataTable(target.schema(), target.obj());

            validate(target, tbl);

            grpIds.add(tbl.cacheContext().groupId());

            updateObjectStatisticInfo(
                new ObjectStatisticsConfiguration(
                    tbl.cacheContext().groupId(),
                    target.key(),
                    Arrays.stream(target.columns())
                        .map(ColumnStatisticsConfiguration::new)
                        .collect(Collectors.toList())
                        .toArray(new ColumnStatisticsConfiguration[target.columns().length]),
                    cfg
                )
            );
        }

        for (int grpId : grpIds)
            updateCacheGroupStatisticVersion(grpId);
    }

    /**
     *
     */
    private int cacheGroupId(StatisticsTarget target) {
        GridH2Table tbl = schemaMgr.dataTable(target.schema(), target.obj());

        validate(target, tbl);

        return tbl.cacheContext().groupId();
    }

    /**
     *
     */
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
    private void updateObjectStatisticInfo(ObjectStatisticsConfiguration info) {
        try {
            while (true) {
                String key = key2String(info.cacheGroupId(), info.key());

                ObjectStatisticsConfiguration oldInfo = distrMetaStorage.read(key);

                if (oldInfo != null)
                    info = ObjectStatisticsConfiguration.merge(info, oldInfo);

                if (distrMetaStorage.compareAndSet(key, oldInfo, info))
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
        ctx.closure().callLocalSafe(
            () -> {
                updateStatQQQ(STAT_OBJ_PREFIX);

                return null;
            },
            GridIoPolicy.MANAGEMENT_POOL
        );
    }

    /**
     *
     */
    private boolean isNeedToUpdateLocalStatistics(ObjectStatisticsConfiguration tblStatInfo) {
        ObjectStatisticsImpl localStat = localRepo.getLocalStatistics(tblStatInfo.key());

        return localStat == null || localStat != null && localStat.version() != tblStatInfo.version();
    }

    /** */
    private void onUpdateStatisticSchema(String key, CacheGroupStatisticsVersion ignore, CacheGroupStatisticsVersion grpStat) {
        ctx.closure().callLocalSafe(
            () -> {
                updateStatQQQ(STAT_OBJ_PREFIX + grpStat.cacheGroupId() + '.');

                return null;
            },
            GridIoPolicy.MANAGEMENT_POOL
        );
    }

    /***/
    private void updateStatQQQ(String keyPrefix) throws IgniteCheckedException {
        Set<ObjectStatisticsConfiguration> updSet = new HashSet<>();

        distrMetaStorage.iterate(keyPrefix, (s, tblStatInfo) -> {
            if(isNeedToUpdateLocalStatistics((ObjectStatisticsConfiguration)tblStatInfo))
                updSet.add((ObjectStatisticsConfiguration)tblStatInfo);
        });

        log.info("+++ NEED TO UPDATE:" + updSet);
    }

    /** */
    private static String key2String(int grpId, StatisticsKey key) {
        StringBuilder sb = new StringBuilder(STAT_OBJ_PREFIX);

        sb.append(grpId).append('.').append(key.schema()).append('.').append(key.obj());

        return sb.toString();
    }
}
