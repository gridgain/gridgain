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

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Statistics repository implementation.
 */
public class IgniteStatisticsRepositoryImpl implements IgniteStatisticsRepository {
    /** Logger. */
    private IgniteLogger log;

    /** Statistics manager. */
    private final IgniteStatisticsManagerImpl statisticsManager;

    /** Table->Partition->Partition Statistics map, populated only on server nodes without persistence enabled. */
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
     * @param statisticsManager Ignite statistics manager.
     * @param log Ignite logger to use.
     */
    public IgniteStatisticsRepositoryImpl(
            boolean storeData,
            boolean persistence,
            IgniteStatisticsManagerImpl statisticsManager,
            IgniteLogger log) {
        if (storeData) {
            // Persistence store
            partsStats = (persistence) ? null : new ConcurrentHashMap<>();
            localStats = new ConcurrentHashMap<>();
        }
        else {
            // Cache only global statistics, no store
            partsStats = null;
            localStats = null;
        }
        this.statisticsManager = statisticsManager;
        this.log = log;
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

            partsStats.compute(key, (k, v) -> {
                if (v == null)
                    v = statisticsMap;
                else
                    v.putAll(statisticsMap);

                return v;
            });
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

        return Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Override public void clearLocalPartitionsStatistics(StatsKey key, String... colNames) {
        if (colNames == null || colNames.length == 0) {
            if (partsStats != null)
                partsStats.remove(key);
        }
        else {
            if (partsStats != null) {
                partsStats.computeIfPresent(key, (tblKey, partMap) -> {
                    partMap.replaceAll((partId, partStat) -> {
                        ObjectPartitionStatisticsImpl partStatNew = substract(partStat, colNames);
                        return (partStatNew.columnsStatistics().isEmpty()) ? null : partStat;

                    });
                    partMap.entrySet().removeIf(e -> e.getValue() == null);
                    return partMap.isEmpty() ? null : partMap;
                });
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
    }

    /** {@inheritDoc} */
    @Override public ObjectPartitionStatisticsImpl getLocalPartitionStatistics(StatsKey key, int partId) {
        if (partsStats != null) {
            Map<Integer, ObjectPartitionStatisticsImpl> objectPartStats = partsStats.get(key);
            return objectPartStats == null ? null : objectPartStats.get(partId);
        }
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
        if (colNames == null || colNames.length == 0) {
            if (localStats != null)
                localStats.remove(key);
        }
        else {
            if (localStats != null) {
                localStats.computeIfPresent(key, (k, v) -> {
                    ObjectStatisticsImpl locStatNew = substract(v, colNames);
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
        if (colNames == null || colNames.length == 0)
            globalStats.remove(key);
        else
            globalStats.computeIfPresent(key, (k, v) -> substract(v, colNames));
    }

    /**
     * Add new statistics into base one (with overlapping of existing data).
     *
     * @param base Old statistics.
     * @param add Updated statistics.
     * @param <T> Statistics type (partition or object one)
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
    private <T extends ObjectStatisticsImpl> T substract(T base, String[] columns) {
        T result = (T)base.clone();
        for (String col : columns)
            result.columnsStatistics().remove(col);

        return result;
    }
}
