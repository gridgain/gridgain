package org.apache.ignite.internal.processors.query.h2.opt.statistics;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.query.QueryTable;
import org.apache.ignite.resources.LoggerResource;


import java.util.Collection;
import java.util.Collections;
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
    public void saveLocalPartitionsStatistics(QueryTable tbl, Collection<ObjectPartitionStatistics> statistics,
                                              boolean fullStat) {
        if (store != null)
            if (fullStat)
                store.saveLocalPartitionsStatistics(tbl, statistics);
            else
                // TODO implement me
                throw new UnsupportedOperationException();
    }

    public Collection<ObjectPartitionStatistics> getLocalPartitionsStatistics(QueryTable tbl){
        return store == null ? Collections.emptyList() : store.getLocalPartitionsStatistics(tbl);
    }

    @Override
    public void clearLocalPartitionsStatistics(QueryTable tbl, String ... colNames) {
        if (colNames == null || colNames.length == 0) {
            if (store != null)
                store.clearLocalPartitionsStatistics(tbl);
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
    }

    @Override
    public ObjectPartitionStatistics getLocalPartitionStatistics(QueryTable tbl, int partId) {
        return (store == null) ? null : store.getLocalPartitionStatistics(tbl, partId);
    }

    @Override
    public void clearLocalPartitionStatistics(QueryTable tbl, int partId) {
        if (store != null)
            store.clearLocalPartitionStatistics(tbl, partId);
    }

    @Override
    public void saveLocalStatistics(QueryTable tbl, ObjectStatistics statistics, boolean fullStat) {
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
        localStats.put(tbl, statistics);
    }

    @Override
    public ObjectStatistics getLocalStatistics(QueryTable tbl, boolean tryLoad) {
        // TODO tryLoad
        return localStats.get(tbl);
    }

    @Override
    public void clearLocalStatistics(QueryTable tbl, String ... colNames) {
        if (colNames == null || colNames.length == 0) {
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
