package org.apache.ignite.internal.processors.query.h2.opt.statistics;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.GridCacheUtils;
import org.apache.ignite.internal.processors.cache.query.QueryTable;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.resources.LoggerResource;


import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class SqlStatisticsRepositoryImpl implements SqlStatisticsRepository {

    /** Logger. */
    @LoggerResource
    private IgniteLogger log;

    private final GridKernalContext ctx;

    private final SqlStatisticsStore store;

    /** Table->Partition->Partition Statistics map, populated only on server nodes without persistence enabled.  */
    private final Map<QueryTable, Map<Integer, ObjectPartitionStatistics>> partsStats;

    /** Local (for current node) object statistics. */
    private final Map<QueryTable, ObjectStatistics> localStats;

    /** Global (for whole cluster) object statistics. */
    private final Map<QueryTable, ObjectStatistics> globalStats = new ConcurrentHashMap<>();


    public SqlStatisticsRepositoryImpl(GridKernalContext ctx) {
        this.ctx = ctx;

        if (ctx.config().isClientMode() || ctx.isDaemon()) {
            // Cache only global statistics, no store
            store = null;
            partsStats = null;
            localStats = null;
        } else {
            if (GridCacheUtils.isPersistenceEnabled(ctx.config())) {
                // Persistence store
                store = new SqlStatisticsStoreImpl(ctx);
                partsStats = null;
            } else {
                // Cache partitions statistics, no store
                store = null;
                partsStats = new ConcurrentHashMap<>();
            }
            localStats = new ConcurrentHashMap<>();
        }
    }

    public void start() {
        if (store != null)
            store.start(this);
    }

    @Override
    public void saveLocalPartitionsStatistics(QueryTable tbl, Collection<ObjectPartitionStatistics> statistics,
                                              boolean fullStat) {
        if (store != null)
            if (fullStat)
                store.saveLocalPartitionsStatistics(tbl, statistics);
            else
                // TODO implement me
                throw new UnsupportedOperationException();
        if (partsStats != null) {
            Map<Integer, ObjectPartitionStatistics> statisticsMap = new ConcurrentHashMap<>();
            for (ObjectPartitionStatistics s : statistics) {
                if (statisticsMap.put(s.partId(), s) != null) {
                    // TODO throw new IllegalStateException("Duplicate key"); or just log warning
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
                // TODO implement me
                throw new UnsupportedOperationException();
        }
    }

    public Collection<ObjectPartitionStatistics> getLocalPartitionsStatistics(QueryTable tbl){
        if (partsStats != null) {
            Map<Integer, ObjectPartitionStatistics> objectStatisticsMap = partsStats.get(tbl);

            return objectStatisticsMap == null ? null : objectStatisticsMap.values();
        }
        return store == null ? Collections.emptyList() : store.getLocalPartitionsStatistics(tbl);
    }

    @Override
    public void clearLocalPartitionsStatistics(QueryTable tbl, String ... colNames) {
        if (colNames == null || colNames.length == 0) {
            if (store != null)
                store.clearLocalPartitionsStatistics(tbl);
            if (partsStats != null) {
                partsStats.remove(tbl);
            }
        } else {
            // TODO implement me
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public void saveLocalPartitionStatistics(QueryTable tbl, int partId, ObjectPartitionStatistics statistics,
                                             boolean fullStat) {
        if (store != null)
            if (fullStat)
                store.saveLocalPartitionStatistics(tbl, partId, statistics);
            else
                // TODO implement me
                throw new UnsupportedOperationException();
        if (partsStats != null) {
            partsStats.compute(tbl, (k,v) -> {
                if(v == null)
                    v = new ConcurrentHashMap<>();
                v.put(statistics.partId(), statistics);

                return v;
            });
        }
    }

    @Override
    public ObjectPartitionStatistics getLocalPartitionStatistics(QueryTable tbl, int partId) {
        if (partsStats != null) {
            Map<Integer, ObjectPartitionStatistics> objectPartStats = partsStats.get(tbl);
            return objectPartStats == null ? null : objectPartStats.get(partId);
        }
        return (store == null) ? null : store.getLocalPartitionStatistics(tbl, partId);
    }

    @Override
    public void clearLocalPartitionStatistics(QueryTable tbl, int partId) {
        if (store != null)
            store.clearLocalPartitionStatistics(tbl, partId);
        if (partsStats != null) {
            partsStats.computeIfPresent(tbl, (k,v) -> {
                v.remove(partId);
                return v.isEmpty() ? null : v;
            });
        }
    }

    @Override
    public void saveLocalStatistics(QueryTable tbl, ObjectStatistics statistics, boolean fullStat) {
        if (localStats != null)
            localStats.put(tbl, statistics);

        if (store != null)
            if (fullStat)
                store.saveLocalStatistics(tbl, statistics);
            else
                // TODO implement me
                throw new UnsupportedOperationException();
    }

    @Override
    public void cacheLocalStatistics(QueryTable tbl, ObjectStatistics statistics) {
        if (localStats != null)
            localStats.put(tbl, statistics);
    }

    @Override
    public ObjectStatistics getLocalStatistics(QueryTable tbl, boolean tryLoad) {
        // TODO tryLoad

        return localStats == null ? null : localStats.get(tbl);
    }

    @Override
    public void clearLocalStatistics(QueryTable tbl, String ... colNames) {
        if (colNames == null || colNames.length == 0) {
            if (localStats != null)
                localStats.remove(tbl);
            if (store != null)
                store.clearLocalStatistics(tbl);
        } else {
            // TODO implement me
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public void saveGlobalStatistics(QueryTable tbl, ObjectStatistics statistics, boolean fullStat) {
        if (!fullStat)
            // TODO implement me
            throw new UnsupportedOperationException();

        globalStats.put(tbl, statistics);
        if (store != null)
            store.saveGlobalStatistics(tbl, statistics);
    }

    @Override
    public void cacheGlobalStatistics(QueryTable tbl, ObjectStatistics statistics) {
        globalStats.put(tbl, statistics);
    }

    public ObjectStatistics getGlobalStatistics(QueryTable tbl, boolean tryLoad) {
        // TODO tryLoad
        return globalStats.get(tbl);
    }

    @Override
    public void clearGlobalStatistics(QueryTable tbl, String ... colNames) {
        if (colNames == null || colNames.length == 0) {
            globalStats.remove(tbl);
            if (store != null)
                store.clearGlobalStatistics(tbl);
        } else {
            // TODO implement me
            throw new UnsupportedOperationException();
        }
    }
}
