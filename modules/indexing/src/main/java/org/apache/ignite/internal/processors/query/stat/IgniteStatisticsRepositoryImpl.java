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

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.subscription.GridInternalSubscriptionProcessor;
import org.apache.ignite.internal.util.typedef.F;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Statistics repository implementation.
 */
public class IgniteStatisticsRepositoryImpl implements IgniteStatisticsRepository {
    /** Logger. */
    private final IgniteLogger log;

    /** Statistics store. */
    private final IgniteStatisticsStore store;

    /** Statistics manager. */
    private final IgniteStatisticsManagerImpl statisticsMgr;

    /** Local (for current node) object statistics. */
    private final Map<StatsKey, ObjectStatisticsImpl> locStats;

    /** Global (for whole cluster) object statistics. */
    private final Map<StatsKey, ObjectStatisticsImpl> globalStats = new ConcurrentHashMap<>();

    /**
     * Constructor.
     *
     * @param storeData If {@code true} - node stores data locally, {@code false} - otherwise.
     * @param db Database to use in storage if persistence enabled.
     * @param subscriptionProcessor Subscription processor.
     * @param statisticsMgr Ignite statistics manager.
     * @param logSupplier Ignite logger supplier to get logger from.
     */
    public IgniteStatisticsRepositoryImpl(
            boolean storeData,
            IgniteCacheDatabaseSharedManager db,
            GridInternalSubscriptionProcessor subscriptionProcessor,
            IgniteStatisticsManagerImpl statisticsMgr,
            Function<Class<?>, IgniteLogger> logSupplier
    ) {
        if (storeData) {
            // Persistence store
            store = (db == null) ? new IgniteStatisticsInMemoryStoreImpl(logSupplier) :
                    new IgniteStatisticsPersistenceStoreImpl(subscriptionProcessor, db, this, logSupplier);

            locStats = new ConcurrentHashMap<>();
        }
        else {
            // Cache only global statistics, no store
            store = null;
            locStats = null;
        }
        this.statisticsMgr = statisticsMgr;
        this.log = logSupplier.apply(IgniteStatisticsRepositoryImpl.class);
    }

    /** {@inheritDoc} */
    @Override public void saveLocalPartitionsStatistics(
            StatsKey key,
            Collection<ObjectPartitionStatisticsImpl> statistics
    ) {
        if (store == null) {
            log.warning("Unable to save partition level statistics on non server node.");

            return;
        }

        store.replaceLocalPartitionsStatistics(key, statistics);
    }

    /** {@inheritDoc} */
    @Override public void mergeLocalPartitionsStatistics(
            StatsKey key,
            Collection<ObjectPartitionStatisticsImpl> statistics
    ) {
        if (store == null) {
            log.warning("Unable to save partition level statistics on non server node.");

            return;
        }

        for (ObjectPartitionStatisticsImpl newStat : statistics) {
            ObjectPartitionStatisticsImpl oldStat = store.getLocalPartitionStatistics(key, newStat.partId());
            if (oldStat != null)
                newStat = add(oldStat, newStat);

            store.saveLocalPartitionStatistics(key, newStat);
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<ObjectPartitionStatisticsImpl> getLocalPartitionsStatistics(StatsKey key) {
        if (store == null) {
            log.warning("Unable to get local partition statistics by " + key + " on non server node.");

            return Collections.emptyList();
        }

        return store.getLocalPartitionsStatistics(key);
    }

    /** {@inheritDoc} */
    @Override public void clearLocalPartitionsStatistics(StatsKey key, String... colNames) {
        if (store == null) {
            log.warning("Unable to clear local partitions by " + key + " on non server node.");

            return;
        }
        if (F.isEmpty(colNames))
                store.clearLocalPartitionsStatistics(key);
        else {
            Collection<ObjectPartitionStatisticsImpl> oldStatistics = store.getLocalPartitionsStatistics(key);
            if (oldStatistics.isEmpty())
                return;

            Collection<ObjectPartitionStatisticsImpl> newStatistics = new ArrayList<>(oldStatistics.size());
            Collection<Integer> partitionsToRemove = new ArrayList<>();
            for (ObjectPartitionStatisticsImpl oldStat : oldStatistics) {
                ObjectPartitionStatisticsImpl newStat = subtract(oldStat, colNames);
                if (!newStat.columnsStatistics().isEmpty())
                    newStatistics.add(newStat);
                else
                    partitionsToRemove.add(oldStat.partId());
            }

            if (newStatistics.isEmpty())
                store.clearLocalPartitionsStatistics(key);
            else {
                if (!partitionsToRemove.isEmpty())
                    store.clearLocalPartitionsStatistics(key, partitionsToRemove);

                store.replaceLocalPartitionsStatistics(key, newStatistics);
            }

        }
    }

    /** {@inheritDoc} */
    @Override public void saveLocalPartitionStatistics(StatsKey key, ObjectPartitionStatisticsImpl statistics) {
        if (store == null) {
            log.warning("Unable to save local partition statistics for " + key + " on non server node.");

            return;
        }
        ObjectPartitionStatisticsImpl oldPartStat = store.getLocalPartitionStatistics(key, statistics.partId());
        if (oldPartStat == null)
            store.saveLocalPartitionStatistics(key, statistics);
        else {
            ObjectPartitionStatisticsImpl combinedStats = add(oldPartStat, statistics);
            store.saveLocalPartitionStatistics(key, combinedStats);
        }
    }

    /** {@inheritDoc} */
    @Override public ObjectPartitionStatisticsImpl getLocalPartitionStatistics(StatsKey key, int partId) {
        if (store == null) {
            log.warning("Unable to get local partition statistics for " + key + " on non server node.");

            return null;
        }

        return store.getLocalPartitionStatistics(key, partId);
    }

    /** {@inheritDoc} */
    @Override public void clearLocalPartitionStatistics(StatsKey key, int partId) {
        if (store == null) {
            log.warning("Unable to clear local partition statistics for " + key + " on non server node.");

            return;
        }

        store.clearLocalPartitionStatistics(key, partId);
    }

    /** {@inheritDoc} */
    @Override public void saveLocalStatistics(StatsKey key, ObjectStatisticsImpl statistics) {
        if (locStats == null) {
            log.warning("Unable to save local statistics for " + key + " on non server node.");

            return;
        }

        locStats.put(key, statistics);
    }

    /** {@inheritDoc} */
    @Override public void mergeLocalStatistics(StatsKey key, ObjectStatisticsImpl statistics) {
        if (locStats == null) {
            log.warning("Unable to merge local statistics for " + key + " on non server node.");

            return;
        }

        locStats.compute(key, (k, v) -> (v == null) ? statistics : add(v, statistics));
    }

    /** {@inheritDoc} */
    @Override public void cacheLocalStatistics(StatsKey key, Collection<ObjectPartitionStatisticsImpl> statistics) {
        if (locStats == null) {
            log.warning("Unable to cache local statistics for " + key + " on non server node.");

            return;
        }

        locStats.put(key, statisticsMgr.aggregateLocalStatistics(key, statistics));
    }

    /** {@inheritDoc} */
    @Override public ObjectStatisticsImpl getLocalStatistics(StatsKey key) {
        if (locStats == null)
            return null;

        return locStats.get(key);
    }

    /** {@inheritDoc} */
    @Override public void clearLocalStatistics(StatsKey key, String... colNames) {
        if (locStats == null) {
            log.warning("Unable to clear local statistics for " + key + " on non server node.");

            return;
        }

        if (F.isEmpty(colNames))
            locStats.remove(key);
        else
            locStats.computeIfPresent(key, (k, v) -> {
                ObjectStatisticsImpl locStatNew = subtract(v, colNames);
                return locStatNew.columnsStatistics().isEmpty() ? null : locStatNew;
            });
    }

    /** {@inheritDoc} */
    @Override public void saveGlobalStatistics(StatsKey key, ObjectStatisticsImpl statistics) {
        globalStats.put(key, statistics);
    }

    /** {@inheritDoc} */
    @Override public void mergeGlobalStatistics(StatsKey key, ObjectStatisticsImpl statistics) {
        globalStats.compute(key, (k,v) -> (v == null) ? statistics : add(v, statistics));
    }

    /** {@inheritDoc} */
    @Override public ObjectStatisticsImpl getGlobalStatistics(StatsKey key) {
        return globalStats.get(key);
    }

    /** {@inheritDoc} */
    @Override public void clearGlobalStatistics(StatsKey key, String... colNames) {
        if (F.isEmpty(colNames))
            globalStats.remove(key);
        else
            globalStats.computeIfPresent(key, (k, v) -> subtract(v, colNames));
    }

    /**
     * @return Ignite statistics store.
     */
    public IgniteStatisticsStore statisticsStore() {
        return store;
    }

    /**
     * Add new statistics into base one (with overlapping of existing data).
     *
     * @param base Old statistics.
     * @param add Updated statistics.
     * @param <T> Statistics type (partition or object one).
     * @return Combined statistics.
     */
    private <T extends ObjectStatisticsImpl> T add(T base, T add) {
        T res = (T)add.clone();
        for (Map.Entry<String, ColumnStatistics> entry : base.columnsStatistics().entrySet())
            res.columnsStatistics().putIfAbsent(entry.getKey(), entry.getValue());

        return res;
    }

    /**
     * Remove specified columns from clone of base ObjectStatistics object.
     *
     * @param base ObjectStatistics to remove columns from.
     * @param cols Columns to remove.
     * @return Cloned object without specified columns statistics.
     */
    private <T extends ObjectStatisticsImpl> T subtract(T base, String[] cols) {
        T res = (T)base.clone();
        for (String col : cols)
            res.columnsStatistics().remove(col);

        return res;
    }
}
