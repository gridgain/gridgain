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
package org.apache.ignite.internal.processors.query.stat.schema;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
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
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class IgniteStatisticsSchemaManager implements DistributedMetastorageLifecycleListener {
    /**
     *
     */
    private static final String STAT_OBJ_PREFIX = "sql.statobj.";

    /**
     *
     */
    private static final String STAT_CACHE_GRP_PREFIX = "sql.stat.grp.";

    /** Distributed metastore. */
    private final DistributedMetaStorage distrMetaStorage;

    /**
     *
     */
    private final IgniteStatisticsRepository localRepo;

    /**
     *
     */
    private final IgniteStatisticsManager mgr;

    /** Schema manager. */
    private final SchemaManager schemaMgr;

    /** Logger. */
    private final IgniteLogger log;

    /**
     *
     */
    public IgniteStatisticsSchemaManager(
        SchemaManager schemaMgr,
        IgniteStatisticsManager mgr,
        DistributedMetaStorage distrMetaStorage,
        GridInternalSubscriptionProcessor subscriptionProcessor,
        IgniteStatisticsRepository localRepo,
        Function<Class<?>, IgniteLogger> logSupplier
    ) {
        this.schemaMgr = schemaMgr;
        this.mgr = mgr;
        this.distrMetaStorage = distrMetaStorage;
        log = logSupplier.apply(IgniteStatisticsSchemaManager.class);
        this.localRepo = localRepo;

        distrMetaStorage.listen(
            (metaKey) -> metaKey.startsWith(STAT_OBJ_PREFIX),
            (k, oldV, newV) -> onUpdateStatisticSchema(k, (ObjectStatisticsInfo)oldV, (ObjectStatisticsInfo)newV)
        );

        distrMetaStorage.listen(
            (metaKey) -> metaKey.startsWith(STAT_CACHE_GRP_PREFIX),
            (k, oldV, newV) -> onUpdateStatisticSchema(k, (ObjectStatisticsInfo)oldV, (ObjectStatisticsInfo)newV)
        );

        subscriptionProcessor.registerDistributedMetastorageListener(this);
    }

    /**
     *
     */
    private static String key2String(StatisticsKey key) {
        return STAT_OBJ_PREFIX + key.schema() + "." + key.obj();
    }

    /**
     *
     */
    private void onUpdateStatisticSchema(
        @NotNull String s,
        @Nullable ObjectStatisticsInfo oldInfo,
        @Nullable ObjectStatisticsInfo newInfo
    ) {

    }

    /**
     *
     */
    public void updateStatistics(List<StatisticsTarget> targets, StatisticConfiguration cfg) {
        List<Integer> grpIds = targets.stream().mapToInt(this::cacheGroupId).boxed().collect(Collectors.toList());

        for (StatisticsTarget target : targets) {
            updateObjectStatisticInfo(
                new ObjectStatisticsInfo(
                    target.key(),
                    Arrays.stream(target.columns())
                        .map(ColumnStatisticsInfo::new)
                        .collect(Collectors.toList())
                        .toArray(new ColumnStatisticsInfo[target.columns().length]),
                    cfg
                )
            );
        }

        for (int grpId : grpIds) {

        }

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
    private void updateObjectStatisticInfo(ObjectStatisticsInfo info) {
        try {
            String key = key2String(info.key());

            ObjectStatisticsInfo oldInfo = distrMetaStorage.read(key);

            if (oldInfo != null)
                info = ObjectStatisticsInfo.merge(info, oldInfo);

            distrMetaStorage.compareAndSet(key, oldInfo, info);
        }
        catch (IgniteCheckedException ex) {
            throw new IgniteSQLException("Error on get or update statistic schema", IgniteQueryErrorCode.UNKNOWN, ex);
        }
    }

    /** {@inheritDoc} */
    @Override public void onReadyForRead(ReadableDistributedMetaStorage metastorage) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onReadyForWrite(DistributedMetaStorage metastorage) {
        try {
            distrMetaStorage.iterate(STAT_OBJ_PREFIX, (s, tblStatInfo) -> {
                updateLocalStatisticsIfNeed((ObjectStatisticsInfo)tblStatInfo);
            });
        }
        catch (IgniteCheckedException ex) {
            log.error("Unexpected error on read statistics schema.", ex);

            // TODO: remove local statistics and continue or throw new IgniteException(ex)?
            throw new IgniteException("Unexpected error on read statistics schema", ex);
        }
    }

    /**
     *
     */
    private void updateLocalStatisticsIfNeed(ObjectStatisticsInfo tblStatInfo) {
        ObjectStatisticsImpl localStat = localRepo.getLocalStatistics(tblStatInfo.key());

        if (localStat != null && localStat.version() != tblStatInfo.version()) {
        }
    }

    /**
     *
     */
    public ObjectStatisticsInfo tableStatisticInfo(StatisticsKey key) {
        try {
            return distrMetaStorage.read(key2String(key));
        }
        catch (IgniteCheckedException ex) {
            throw new IgniteSQLException("Error on get local statistic", IgniteQueryErrorCode.UNKNOWN, ex);
        }
    }
}
