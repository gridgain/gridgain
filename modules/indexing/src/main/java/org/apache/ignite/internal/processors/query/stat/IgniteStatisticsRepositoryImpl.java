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
package org.apache.ignite.internal.processors.query;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.GridCacheUtils;
import org.apache.ignite.internal.processors.query.stat.ColumnStatistics;
import org.apache.ignite.internal.processors.query.stat.IgniteStatisticsManagerImpl;
import org.apache.ignite.internal.processors.query.stat.IgniteStatisticsRepository;
import org.apache.ignite.internal.processors.query.stat.ObjectPartitionStatisticsImpl;
import org.apache.ignite.internal.processors.query.stat.ObjectStatisticsImpl;
import org.apache.ignite.internal.processors.query.stat.StatsKey;

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

    /** */
    private final GridKernalContext ctx;

    /** Table->Partition->Partition Statistics map, populated only on server nodes without persistence enabled.  */
    private final Map<StatsKey, Map<Integer, ObjectPartitionStatisticsImpl>> partsStats;

    /** Local (for current node) object statistics. */
    private final Map<StatsKey, ObjectStatisticsImpl> localStats;

    /** Global (for whole cluster) object statistics. */
    private final Map<StatsKey, ObjectStatisticsImpl> globalStats = new ConcurrentHashMap<>();

    /**
     * Constructor.
     */
    public IgniteStatisticsRepositoryImpl(GridKernalContext ctx) {
        this.ctx = ctx;

        if (ctx.config().isClientMode() || ctx.isDaemon()) {
            // Cache only global statistics, no store
            partsStats = null;
            localStats = null;
        } else {
            // Persistence store
            partsStats = GridCacheUtils.isPersistenceEnabled(ctx.config()) ? null : new ConcurrentHashMap<>();

            localStats = new ConcurrentHashMap<>();
        }
        log = ctx.log(IgniteStatisticsRepositoryImpl.class);
    }

    @Override public void saveLocalPartitionsStatistics(StatsKey key,
                                                        Collection<ObjectPartitionStatisticsImpl> statistics,
                                                        boolean fullStat) {
        if (partsStats != null) {
            Map<Integer, ObjectPartitionStatisticsImpl> statisticsMap = new ConcurrentHashMap<>();
            for (ObjectPartitionStatisticsImpl s : statistics) {
                if (statisticsMap.put(s.partId(), s) != null)
                    log.warning(String.format("Trying to save more than one %s.%s partition statistics for partition %d",
                            key.schema(), key.obj(), s.partId()));
            }

            if (fullStat) {
                partsStats.compute(key, (k, v) -> {
                    if (v == null)
                        v = statisticsMap;
                    else
                        v.putAll(statisticsMap);

                    return v;
                });
            } else {
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
    }

    public Collection<ObjectPartitionStatisticsImpl> getLocalPartitionsStatistics(StatsKey key) {
        if (partsStats != null) {
            Map<Integer, ObjectPartitionStatisticsImpl> objectStatisticsMap = partsStats.get(key);

            return (objectStatisticsMap == null) ? null : objectStatisticsMap.values();
        }

        return Collections.emptyList();
    }

    @Override public void clearLocalPartitionsStatistics(StatsKey key, String... colNames) {
        if (colNames == null || colNames.length == 0) {
            if (partsStats != null)
                partsStats.remove(key);
        } else {
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

    @Override public ObjectPartitionStatisticsImpl getLocalPartitionStatistics(StatsKey key, int partId) {
        if (partsStats != null) {
            Map<Integer, ObjectPartitionStatisticsImpl> objectPartStats = partsStats.get(key);
            return objectPartStats == null ? null : objectPartStats.get(partId);
        }
        return null;
    }

    @Override public void clearLocalPartitionStatistics(StatsKey key, int partId) {
        if (partsStats != null) {
            partsStats.computeIfPresent(key, (k, v) -> {
                v.remove(partId);
                return v.isEmpty() ? null : v;
            });
        }
    }

    @Override public void saveLocalStatistics(StatsKey key, ObjectStatisticsImpl statistics, boolean fullStat) {
        if (localStats != null) {
            if (fullStat)
                localStats.put(key, statistics);
            else {
                localStats.compute(key, (k, v) -> {
                    if (v == null)
                        return statistics;
                    return add(v, statistics);
                });
            }
        }
    }

    @Override public void cacheLocalStatistics(StatsKey key, Collection<ObjectPartitionStatisticsImpl> statistics) {
        IgniteStatisticsManagerImpl statManager = (IgniteStatisticsManagerImpl)ctx.query().getIndexing().statsManager();
        if (localStats != null)
            localStats.put(key, statManager.aggregateLocalStatistics(key, statistics));
    }

    @Override public ObjectStatisticsImpl getLocalStatistics(StatsKey key) {
        return localStats == null ? null : localStats.get(key);
    }

    @Override public void clearLocalStatistics(StatsKey key, String... colNames) {
        if (colNames == null || colNames.length == 0) {
            if (localStats != null)
                localStats.remove(key);
        } else {
            if (localStats != null) {
                localStats.computeIfPresent(key, (k, v) -> {
                    ObjectStatisticsImpl locStatNew = substract(v, colNames);
                    return locStatNew.columnsStatistics().isEmpty() ? null : locStatNew;
                });
            }
        }
    }

    @Override public void saveGlobalStatistics(StatsKey key, ObjectStatisticsImpl statistics, boolean fullStat) {
        globalStats.put(key, statistics);
    }

    public ObjectStatisticsImpl getGlobalStatistics(StatsKey key) {
        return globalStats.get(key);
    }

    @Override public void clearGlobalStatistics(StatsKey key, String... colNames) {
        if (colNames == null || colNames.length == 0)
            globalStats.remove(key);
        else
            globalStats.computeIfPresent(key, (k, v) -> substract(v, colNames));
    }

    /**
     * Add new statistics into base one (with overlapping of existing data).
     *
     * @param base old statistics.
     * @param add updated statistics.
     * @param <T> statistics type (partition or object one)
     * @return combined statistics.
     */
    private <T extends ObjectStatisticsImpl> T add(T base, T add) {
        T result = (T)add.clone();
        for (Map.Entry<String, ColumnStatistics> entry : base.columnsStatistics().entrySet()) {
            result.columnsStatistics().putIfAbsent(entry.getKey(), entry.getValue());
        }
        return result;
    }

    /**
     * Remove specified columns from clone of base ObjectStatistics object.
     *
     * @param base ObjectStatistics to remove columns from.
     * @param columns columns to remove.
     * @return cloned object without specified columns statistics.
     */
    private <T extends ObjectStatisticsImpl> T substract(T base, String[] columns) {
        T result = (T)base.clone();
        for (String col : columns)
            result.columnsStatistics().remove(col);

        return result;
    }
}
