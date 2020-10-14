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
import org.apache.ignite.internal.processors.cache.query.QueryTable;
import org.apache.ignite.internal.processors.query.stat.ColumnStatistics;
import org.apache.ignite.internal.processors.query.stat.IgniteStatisticsManagerImpl;
import org.apache.ignite.internal.processors.query.stat.IgniteStatisticsRepository;
import org.apache.ignite.internal.processors.query.stat.ObjectPartitionStatistics;
import org.apache.ignite.internal.processors.query.stat.ObjectStatistics;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class IgniteStatisticsRepositoryImpl implements IgniteStatisticsRepository {
    /** Logger. */
    private IgniteLogger log;

    /** */
    private final GridKernalContext ctx;

    /** Table->Partition->Partition Statistics map, populated only on server nodes without persistence enabled.  */
    private final Map<QueryTable, Map<Integer, ObjectPartitionStatistics>> partsStats;

    /** Local (for current node) object statistics. */
    private final Map<QueryTable, ObjectStatistics> localStats;

    /** Global (for whole cluster) object statistics. */
    private final Map<QueryTable, ObjectStatistics> globalStats = new ConcurrentHashMap<>();


    public IgniteStatisticsRepositoryImpl(GridKernalContext ctx) {
        this.ctx = ctx;

        if (ctx.config().isClientMode() || ctx.isDaemon()) {
            // Cache only global statistics, no store
            partsStats = null;
            localStats = null;
        } else {
            if (GridCacheUtils.isPersistenceEnabled(ctx.config())) {
                // Persistence store
                partsStats = null;
            } else {
                // Cache partitions statistics, no store
                partsStats = new ConcurrentHashMap<>();
            }
            localStats = new ConcurrentHashMap<>();
        }
        log = ctx.log(IgniteStatisticsRepositoryImpl.class);
    }

    @Override public void saveLocalPartitionsStatistics(QueryTable tbl, Collection<ObjectPartitionStatistics> statistics,
                                                        boolean fullStat) {
        if (partsStats != null) {
            Map<Integer, ObjectPartitionStatistics> statisticsMap = new ConcurrentHashMap<>();
            for (ObjectPartitionStatistics s : statistics) {
                if (statisticsMap.put(s.partId(), s) != null) {
                    log.warning(String.format("Trying to save more than one %s.%s partition statistics for partition %d",
                            tbl.schema(), tbl.table(), s.partId()));
                }
            }

            if (fullStat)
                partsStats.compute(tbl, (k, v) -> {
                    if (v == null)
                        v = statisticsMap;
                    else
                        v.putAll(statisticsMap);

                    return v;
                });
            else
                partsStats.compute(tbl, (k, v) -> {
                    if (v != null)
                        for (Map.Entry<Integer, ObjectPartitionStatistics> partStat : v.entrySet()) {
                            ObjectPartitionStatistics newStat = statisticsMap.get(partStat.getKey());
                            if (newStat != null) {
                                ObjectPartitionStatistics combinedStat = add(partStat.getValue(), newStat);
                                statisticsMap.put(partStat.getKey(), combinedStat);
                            }
                        }
                    return statisticsMap;
                });

        }
    }

    public Collection<ObjectPartitionStatistics> getLocalPartitionsStatistics(QueryTable tbl){
        if (partsStats != null) {
            Map<Integer, ObjectPartitionStatistics> objectStatisticsMap = partsStats.get(tbl);

            return (objectStatisticsMap == null) ? null : objectStatisticsMap.values();
        }

        return Collections.emptyList();
    }

    @Override public void clearLocalPartitionsStatistics(QueryTable tbl, String... colNames) {
        if (colNames == null || colNames.length == 0)
            if (partsStats != null)
                partsStats.remove(tbl);
        else
            if (partsStats != null)
                partsStats.computeIfPresent(tbl, (tblKey, partMap) -> {
                    partMap.replaceAll((partId, partStat) -> {
                        ObjectPartitionStatistics partStatNew = substract(partStat, colNames);
                        return (partStatNew.columnsStatistics().isEmpty()) ? null : partStat;

                    });
                    partMap.entrySet().removeIf(e -> e.getValue() == null);
                    return partMap.isEmpty() ? null : partMap;
                });
    }

    @Override public void saveLocalPartitionStatistics(QueryTable tbl, ObjectPartitionStatistics statistics) {
        if (partsStats != null) {
            partsStats.compute(tbl, (k,v) -> {
                if(v == null)
                    v = new ConcurrentHashMap<>();
                ObjectPartitionStatistics oldPartStat = v.get(statistics.partId());
                if (oldPartStat == null)
                    v.put(statistics.partId(), statistics);
                else {
                    ObjectPartitionStatistics combinedStats = add(oldPartStat, statistics);
                    v.put(statistics.partId(), combinedStats);
                }
                return v;
            });
        }
    }

    @Override public ObjectPartitionStatistics getLocalPartitionStatistics(QueryTable tbl, int partId) {
        if (partsStats != null) {
            Map<Integer, ObjectPartitionStatistics> objectPartStats = partsStats.get(tbl);
            return objectPartStats == null ? null : objectPartStats.get(partId);
        }
        return null;
    }

    @Override public void clearLocalPartitionStatistics(QueryTable tbl, int partId) {
        if (partsStats != null)
            partsStats.computeIfPresent(tbl, (k,v) -> {
                v.remove(partId);
                return v.isEmpty() ? null : v;
            });
    }

    @Override public void saveLocalStatistics(QueryTable tbl, ObjectStatistics statistics, boolean fullStat) {
        if (localStats != null)
            if (fullStat)
                localStats.put(tbl, statistics);
            else
                localStats.compute(tbl, (k,v) -> {
                    if (v == null)
                        return statistics;
                    return add(v, statistics);
                });
    }

    @Override public void cacheLocalStatistics(QueryTable tbl, Collection<ObjectPartitionStatistics> statistics) {
        IgniteStatisticsManagerImpl statManager = (IgniteStatisticsManagerImpl)ctx.query().getIndexing().statsManager();
        if (localStats != null)
            localStats.put(tbl, statManager.aggregateLocalStatistics(tbl, statistics));
    }

    @Override public ObjectStatistics getLocalStatistics(QueryTable tbl) {
        return localStats == null ? null : localStats.get(tbl);
    }

    @Override public void clearLocalStatistics(QueryTable tbl, String ... colNames) {
        if (colNames == null || colNames.length == 0)
            if (localStats != null)
                localStats.remove(tbl);
        else
            if (localStats != null)
                localStats.computeIfPresent(tbl, (k,v) -> {
                   ObjectStatistics locStatNew = substract(v, colNames);
                   return locStatNew.columnsStatistics().isEmpty() ? null : locStatNew;
                });
    }

    @Override public void saveGlobalStatistics(QueryTable tbl, ObjectStatistics statistics, boolean fullStat) {
        globalStats.put(tbl, statistics);
    }

    public ObjectStatistics getGlobalStatistics(QueryTable tbl) {
        return globalStats.get(tbl);
    }

    @Override public void clearGlobalStatistics(QueryTable tbl, String... colNames) {
        if (colNames == null || colNames.length == 0)
            globalStats.remove(tbl);
        else
            globalStats.computeIfPresent(tbl, (k, v) -> substract(v, colNames));
    }

    /**
     * Add new statistics into base one (with overlapping of existing data).
     *
     * @param base old statistics.
     * @param add updated statistics.
     * @param <T> statistics type (partition or object one)
     * @return combined statistics.
     */
    private <T extends ObjectStatistics> T add(T base, T add) {
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
    private <T extends ObjectStatistics> T substract(T base, String[] columns) {
        T result = (T)base.clone();
        for (String col : columns) {
            result.columnsStatistics().remove(col);
        }
        return result;
    }
}
