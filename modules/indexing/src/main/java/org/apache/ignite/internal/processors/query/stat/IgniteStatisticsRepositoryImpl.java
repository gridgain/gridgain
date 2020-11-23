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
import java.util.stream.Collectors;

/**
 * Statistics repository implementation.
 */
public class IgniteStatisticsRepositoryImpl implements IgniteStatisticsRepository {
    /** Logger. */
    private IgniteLogger log;

    /** Statistics store. */
    private final IgniteStatisticsStore store;

    /** Statistics manager. */
    private final IgniteStatisticsManagerImpl statisticsManager;

    /** Table -> Partition -> Partition Statistics map, populated only on server nodes without persistence enabled. */
    private final Map<StatsKey, Map<Integer, ObjectPartitionStatisticsImpl>> partsStats;

    /** Local (for current node) object statistics. */
    private final Map<StatsKey, ObjectStatisticsImpl> localStats;

    /** Global (for whole cluster) object statistics. */
    private final Map<StatsKey, ObjectStatisticsImpl> globalStats = new ConcurrentHashMap<>();

    /**
     * Constructor.
     *
     * @param storeData If {@code true} - node stores data locally, {@code false} - otherwise.
     * @param persistence If {@code true} - node have persistence store, {@code false} - otherwise.
     * @param subscriptionProcessor Subscription processor.
     * @param statisticsManager Ignite statistics manager.
     * @param log Ignite logger to use.
     */
    public IgniteStatisticsRepositoryImpl(
            boolean storeData,
            boolean persistence,
            IgniteCacheDatabaseSharedManager database,
            GridInternalSubscriptionProcessor subscriptionProcessor,
            IgniteStatisticsManagerImpl statisticsManager,
            Function<Class<?>, IgniteLogger> logSupplier
    ) {
        if (storeData) {
            // Persistence store
            if (persistence) {
                store = new IgniteStatisticsStoreImpl(subscriptionProcessor, database,this, logSupplier);
                partsStats = null;
            }
            else {
                store = null;
                partsStats = new ConcurrentHashMap<>();
            }
            localStats = new ConcurrentHashMap<>();
        }
        else {
            // Cache only global statistics, no store
            store = null;
            partsStats = null;
            localStats = null;
        }
        this.statisticsManager = statisticsManager;
        this.log = logSupplier.apply(IgniteStatisticsRepositoryImpl.class);
    }

    /**
     * Convert collection of partition level statistics into map(partId->partStatistics).
     *
     * @param key Object key.
     * @param statistics Collection of tables partition statistics.
     * @return Partition id to statistics map.
     */
    private Map<Integer, ObjectPartitionStatisticsImpl> buildStatisticsMap(
            StatsKey key,
            Collection<ObjectPartitionStatisticsImpl> statistics
    ) {
        Map<Integer, ObjectPartitionStatisticsImpl> statisticsMap = new ConcurrentHashMap<>();
        for (ObjectPartitionStatisticsImpl s : statistics) {
            if (statisticsMap.put(s.partId(), s) != null)
                log.warning(String.format("Trying to save more than one %s.%s partition statistics for partition %d",
                        key.schema(), key.obj(), s.partId()));
        }
        return statisticsMap;
    }


    /** {@inheritDoc} */
    @Override public void saveLocalPartitionsStatistics(
            StatsKey key,
            Collection<ObjectPartitionStatisticsImpl> statistics
    ) {
        if (partsStats != null) {
            Map<Integer, ObjectPartitionStatisticsImpl> statisticsMap = buildStatisticsMap(key, statistics);

            partsStats.put(key, statisticsMap);
        }
        if (store != null) {
            Map<Integer, ObjectPartitionStatisticsImpl> oldStatistics = store.getLocalPartitionsStatistics(key).stream()
                    .collect(Collectors.toMap(s -> s.partId(), s -> s));
            Collection<ObjectPartitionStatisticsImpl> combinedStats = new ArrayList<>(statistics.size());
            for (ObjectPartitionStatisticsImpl newPartStat : statistics) {
                ObjectPartitionStatisticsImpl oldPartStat = oldStatistics.get(newPartStat.partId());
                if (oldPartStat == null)
                    combinedStats.add(newPartStat);
                else {
                    ObjectPartitionStatisticsImpl combinedPartStats = add(oldPartStat, newPartStat);
                    combinedStats.add(combinedPartStats);
                }
            }
            store.saveLocalPartitionsStatistics(key, combinedStats);
        }
    }

    /** {@inheritDoc} */
    @Override public void mergeLocalPartitionsStatistics(
            StatsKey key,
            Collection<ObjectPartitionStatisticsImpl> statistics
    ) {
        if (partsStats != null) {
            Map<Integer, ObjectPartitionStatisticsImpl> statisticsMap = buildStatisticsMap(key, statistics);

            partsStats.compute(key, (k, v) -> {
                if (v != null) {
                    for (Map.Entry<Integer, ObjectPartitionStatisticsImpl> partStat : v.entrySet()) {
                        ObjectPartitionStatisticsImpl newStat = statisticsMap.get(partStat.getKey());
                        if (newStat != null) {
                            ObjectPartitionStatisticsImpl combinedStat = add(partStat.getValue(), newStat);
                            statisticsMap.put(partStat.getKey(), combinedStat);
                        }
                    }
                }
                return statisticsMap;
            });
        }
    }


    /** {@inheritDoc} */
    @Override public Collection<ObjectPartitionStatisticsImpl> getLocalPartitionsStatistics(StatsKey key) {
        if (partsStats != null) {
            Map<Integer, ObjectPartitionStatisticsImpl> objectStatisticsMap = partsStats.get(key);

            return (objectStatisticsMap == null) ? Collections.emptyList() : objectStatisticsMap.values();
        }

        if (store != null)
            return store.getLocalPartitionsStatistics(key);

        return Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Override public void clearLocalPartitionsStatistics(StatsKey key, String... colNames) {
        if (F.isEmpty(colNames)) {
            if (partsStats != null)
                partsStats.remove(key);

            if (store != null)
                store.clearLocalPartitionsStatistics(key);
        }
        else {
            if (partsStats != null) {
                partsStats.computeIfPresent(key, (tblKey, partMap) -> {
                    partMap.replaceAll((partId, partStat) -> {
                        ObjectPartitionStatisticsImpl partStatNew = subtract(partStat, colNames);
                        return (partStatNew.columnsStatistics().isEmpty()) ? null : partStat;

                    });
                    partMap.entrySet().removeIf(e -> e.getValue() == null);
                    return partMap.isEmpty() ? null : partMap;
                });
            }

            if (store != null) {
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

                    store.saveLocalPartitionsStatistics(key, newStatistics);
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void saveLocalPartitionStatistics(StatsKey key, ObjectPartitionStatisticsImpl statistics) {
        if (partsStats != null) {
            partsStats.compute(key, (k,v) -> {
                if (v == null)
                    v = new ConcurrentHashMap<>();
                ObjectPartitionStatisticsImpl oldPartStat = v.get(statistics.partId());
                if (oldPartStat == null)
                    v.put(statistics.partId(), statistics);
                else {
                    ObjectPartitionStatisticsImpl combinedStats = add(oldPartStat, statistics);
                    v.put(statistics.partId(), combinedStats);
                }
                return v;
            });
        }

        if (store != null) {
            ObjectPartitionStatisticsImpl oldPartStat = store.getLocalPartitionStatistics(key, statistics.partId());
            if (oldPartStat == null)
                store.saveLocalPartitionStatistics(key, statistics);
            else {
                ObjectPartitionStatisticsImpl combinedStats = add(oldPartStat, statistics);
                store.saveLocalPartitionStatistics(key, combinedStats);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public ObjectPartitionStatisticsImpl getLocalPartitionStatistics(StatsKey key, int partId) {
        if (partsStats != null) {
            Map<Integer, ObjectPartitionStatisticsImpl> objectPartStats = partsStats.get(key);
            return objectPartStats == null ? null : objectPartStats.get(partId);
        }

        if (store != null)
            return store.getLocalPartitionStatistics(key, partId);

        return null;
    }

    /** {@inheritDoc} */
    @Override public void clearLocalPartitionStatistics(StatsKey key, int partId) {
        if (partsStats != null) {
            partsStats.computeIfPresent(key, (k, v) -> {
                v.remove(partId);
                return v.isEmpty() ? null : v;
            });
        }

        if (store != null)
            store.clearLocalPartitionStatistics(key, partId);
    }

    /** {@inheritDoc} */
    @Override public void saveLocalStatistics(StatsKey key, ObjectStatisticsImpl statistics) {
        if (localStats != null)
            localStats.put(key, statistics);
    }

    /** {@inheritDoc} */
    @Override public void mergeLocalStatistics(StatsKey key, ObjectStatisticsImpl statistics) {
        if (localStats != null) {
            localStats.compute(key, (k, v) -> {
                if (v == null)
                    return statistics;
                return add(v, statistics);
            });
        }
    }

    /** {@inheritDoc} */
    @Override public void cacheLocalStatistics(StatsKey key, Collection<ObjectPartitionStatisticsImpl> statistics) {
        if (localStats != null)
            localStats.put(key, statisticsManager.aggregateLocalStatistics(key, statistics));
    }

    /** {@inheritDoc} */
    @Override public ObjectStatisticsImpl getLocalStatistics(StatsKey key) {
        return localStats == null ? null : localStats.get(key);
    }

    /** {@inheritDoc} */
    @Override public void clearLocalStatistics(StatsKey key, String... colNames) {
        if (F.isEmpty(colNames)) {
            if (localStats != null)
                localStats.remove(key);
        }
        else {
            if (localStats != null) {
                localStats.computeIfPresent(key, (k, v) -> {
                    ObjectStatisticsImpl locStatNew = subtract(v, colNames);
                    return locStatNew.columnsStatistics().isEmpty() ? null : locStatNew;
                });
            }
        }
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
        T result = (T)add.clone();
        for (Map.Entry<String, ColumnStatistics> entry : base.columnsStatistics().entrySet())
            result.columnsStatistics().putIfAbsent(entry.getKey(), entry.getValue());

        return result;
    }

    /**
     * Remove specified columns from clone of base ObjectStatistics object.
     *
     * @param base ObjectStatistics to remove columns from.
     * @param columns Columns to remove.
     * @return Cloned object without specified columns statistics.
     */
    private <T extends ObjectStatisticsImpl> T subtract(T base, String[] columns) {
        T result = (T)base.clone();
        for (String col : columns)
            result.columnsStatistics().remove(col);

        return result;
    }
}
