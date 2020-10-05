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
import org.apache.ignite.internal.processors.query.stat.IgniteStatisticsRepository;
import org.apache.ignite.internal.processors.query.stat.ObjectPartitionStatistics;
import org.apache.ignite.internal.processors.query.stat.ObjectStatistics;
import org.apache.ignite.resources.LoggerResource;


import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class IgniteStatisticsRepositoryImpl implements IgniteStatisticsRepository {
    /** Logger. */
    @LoggerResource
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
                partsStats.compute(tbl, (k,v) -> {
                    if (v == null)
                        v = statisticsMap;
                    else
                        v.putAll(statisticsMap);

                    return v;
                });
            else
                throw new UnsupportedOperationException();
        }
    }

    public Collection<ObjectPartitionStatistics> getLocalPartitionsStatistics(QueryTable tbl){
        if (partsStats != null) {
            Map<Integer, ObjectPartitionStatistics> objectStatisticsMap = partsStats.get(tbl);

            return objectStatisticsMap == null ? null : objectStatisticsMap.values();
        }

        return Collections.emptyList();
    }

    @Override public void clearLocalPartitionsStatistics(QueryTable tbl, String ... colNames) {
        if (colNames == null || colNames.length == 0)
            if (partsStats != null)
                partsStats.remove(tbl);
        else
            throw new UnsupportedOperationException();
    }

    @Override public void saveLocalPartitionStatistics(QueryTable tbl, int partId, ObjectPartitionStatistics statistics,
                                             boolean fullStat) {
        if (partsStats != null) {
            partsStats.compute(tbl, (k,v) -> {
                if(v == null)
                    v = new ConcurrentHashMap<>();
                v.put(statistics.partId(), statistics);

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
            localStats.put(tbl, statistics);
    }

    @Override public void cacheLocalStatistics(QueryTable tbl, ObjectStatistics statistics) {
        if (localStats != null)
            localStats.put(tbl, statistics);
    }

    @Override public ObjectStatistics getLocalStatistics(QueryTable tbl) {
        return localStats == null ? null : localStats.get(tbl);
    }

    @Override public void clearLocalStatistics(QueryTable tbl, String ... colNames) {
        if (colNames == null || colNames.length == 0)
            if (localStats != null)
                localStats.remove(tbl);
        else
            throw new UnsupportedOperationException();
    }

    @Override public void saveGlobalStatistics(QueryTable tbl, ObjectStatistics statistics, boolean fullStat) {
        if (!fullStat)
            throw new UnsupportedOperationException();

        globalStats.put(tbl, statistics);
    }

    @Override public void cacheGlobalStatistics(QueryTable tbl, ObjectStatistics statistics) {
        globalStats.put(tbl, statistics);
    }

    public ObjectStatistics getGlobalStatistics(QueryTable tbl) {
        return globalStats.get(tbl);
    }

    @Override public void clearGlobalStatistics(QueryTable tbl, String ... colNames) {
        if (colNames == null || colNames.length == 0)
            globalStats.remove(tbl);
        else
            throw new UnsupportedOperationException();
    }
}
