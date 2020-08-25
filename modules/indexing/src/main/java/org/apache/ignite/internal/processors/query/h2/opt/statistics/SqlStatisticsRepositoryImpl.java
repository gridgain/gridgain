package org.apache.ignite.internal.processors.query.h2.opt.statistics;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.query.QueryTable;
import org.apache.ignite.resources.LoggerResource;


import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SqlStatisticsRepositoryImpl implements SqlStatisticsRepository {

    /** Logger. */
    @LoggerResource
    private IgniteLogger log;

    private final GridKernalContext ctx;

    private final SqlStatisticsStore store;

    private Map<QueryTable, ObjectStatistics> localStats = new ConcurrentHashMap<>();
    private Map<QueryTable, ObjectStatistics> globalStats = new ConcurrentHashMap<>();


    public SqlStatisticsRepositoryImpl(GridKernalContext ctx, SqlStatisticsStore store) {
        this.ctx = ctx;
        this.store = store;
    }

    @Override
    public void saveLocalPartitionsStatistics(QueryTable tbl, Collection<ObjectPartitionStatistics> statistics) {
        store.saveLocalPartitionsStatistics(tbl, statistics);
    }

    public Collection<ObjectPartitionStatistics> getLocalPartitionsStatistics(QueryTable tbl){
        return store.getLocalPartitionsStatistics(tbl);
    }

    public void clearLocalPartitionsStatistics(QueryTable tbl) {
        store.clearLocalPartitionsStatistics(tbl);
    }

    @Override
    public void saveLocalPartitionStatistics(QueryTable tbl, int partId, ObjectPartitionStatistics statistics) {
        store.saveLocalPartitionStatistics(tbl, partId, statistics);
    }

    public ObjectPartitionStatistics getLocalPartitionStatistics(QueryTable tbl, int partId) {
        return store.getLocalPartitionStatistics(tbl, partId);
    }

    public void clearLocalPartitionStatistics(QueryTable tbl, int partId) {
        store.clearLocalPartitionStatistics(tbl, partId);
    }

    @Override
    public void saveLocalStatistics(QueryTable tbl, ObjectStatistics statistics) {
        localStats.put(tbl, statistics);
        store.saveLocalStatistics(tbl, statistics);
    }

    @Override
    public void cacheLocalStatistics(QueryTable tbl, ObjectStatistics statistics) {
        localStats.put(tbl, statistics);
    }

    @Override
    public ObjectStatistics getLocalStatistics(QueryTable tbl, boolean tryLoad) {
        // TODO tryLoad
        return localStats.get(tbl);
    }

    @Override
    public void clearLocalStatistics(QueryTable tbl) {
        localStats.remove(tbl);
        store.clearLocalStatistics(tbl);
    }

    @Override
    public void saveGlobalStatistics(QueryTable tbl, ObjectStatistics statistics) {
        globalStats.put(tbl, statistics);
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

    public void clearGlobalStatistics(QueryTable tbl) {
        globalStats.remove(tbl);
        store.clearGlobalStatistics(tbl);
    }
}
