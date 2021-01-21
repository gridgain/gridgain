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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.metastorage.DistributedMetaStorage;
import org.apache.ignite.internal.processors.metastorage.DistributedMetastorageLifecycleListener;
import org.apache.ignite.internal.processors.metastorage.ReadableDistributedMetaStorage;
import org.apache.ignite.internal.processors.query.stat.IgniteStatisticsRepository;
import org.apache.ignite.internal.processors.query.stat.IgniteStatisticsRepositoryImpl;
import org.apache.ignite.internal.processors.query.stat.ObjectStatisticsImpl;
import org.apache.ignite.internal.processors.query.stat.StatisticsKey;
import org.apache.ignite.internal.processors.subscription.GridInternalSubscriptionProcessor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.function.Function;

/** */
public class IgniteStatisticsSchemaManager implements DistributedMetastorageLifecycleListener {
    /** */
    private static final String STAT_OBJ_PREFIX = "sql.statobj.";

    /** Distributed metastore. */
    private final DistributedMetaStorage distrMetaStorage;

    /** */
    private final IgniteStatisticsRepository localRepo;

    /** Logger. */
    private final IgniteLogger log;

    /** */
    public IgniteStatisticsSchemaManager(
        DistributedMetaStorage distrMetaStorage,
        GridInternalSubscriptionProcessor subscriptionProcessor,
        IgniteStatisticsRepository localRepo,
        Function<Class<?>, IgniteLogger> logSupplier
    ) {
        this.distrMetaStorage = distrMetaStorage;
        log = logSupplier.apply(IgniteStatisticsSchemaManager.class);
        this.localRepo = localRepo;

        distrMetaStorage.listen(
            (metaKey) -> metaKey.startsWith(STAT_OBJ_PREFIX),
            (k, oldV, newV) -> onUpdateStatisticSchema(k, (TableStatisticsInfo)oldV, (TableStatisticsInfo)newV)
        );

        subscriptionProcessor.registerDistributedMetastorageListener(this);
    }

    /** */
    private void onUpdateStatisticSchema(
        @NotNull String s,
        @Nullable TableStatisticsInfo oldInfo,
        @Nullable TableStatisticsInfo newInfo
    ) {

    }

    /** */
    public void updateStatistic(TableStatisticsInfo info) throws IgniteCheckedException {
        String key = key2String(info.key());

        TableStatisticsInfo oldInfo = distrMetaStorage.read(key);

        if (oldInfo != null)
            info = TableStatisticsInfo.merge(info, oldInfo);

        distrMetaStorage.compareAndSet(key, oldInfo, info);
    }

    /** {@inheritDoc} */
    @Override public void onReadyForRead(ReadableDistributedMetaStorage metastorage) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onReadyForWrite(DistributedMetaStorage metastorage) {
        try {
            distrMetaStorage.iterate(STAT_OBJ_PREFIX, (s, tblStatInfo) -> {
                updateLocalStatisticsIfNeed((TableStatisticsInfo)tblStatInfo);
            });
        }
        catch (IgniteCheckedException ex) {
            log.error("Unexpected error on read statistics schema.", ex);

            // TODO: remove local statistics and continue or throw new IgniteException(ex)?
            throw new IgniteException("Unexpected error on read statistics schema", ex);
        }
    }

    /** */
    private void updateLocalStatisticsIfNeed(TableStatisticsInfo tblStatInfo) {
        ObjectStatisticsImpl localStat = localRepo.getLocalStatistics(tblStatInfo.key());

        if (localStat != null && localStat.version() != tblStatInfo.version()) {
            // TODO: update local
        }
    }

    /** */
    private static String key2String(StatisticsKey key) {
        return key.schema() + "." + key.obj();
    }
}
