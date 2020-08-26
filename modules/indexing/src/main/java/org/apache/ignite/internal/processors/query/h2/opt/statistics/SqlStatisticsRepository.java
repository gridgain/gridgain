package org.apache.ignite.internal.processors.query.h2.opt.statistics;

import org.apache.ignite.internal.processors.cache.query.QueryTable;

import java.util.Collection;

/**
 * Repository to store all necessary statistics. Can request absent ones from cluster and store to
 * {@link SqlStatisticsStoreImpl}.
 */
public interface SqlStatisticsRepository {

    /**
     * Replace all table statistics with specified ones.
     *
     * @param tbl table.
     * @param statistics collection of tables partition statistics
     * @param fullStat if {@code True} - replace whole statistics, try to merge with existing - otherwise.
     */
    void saveLocalPartitionsStatistics(QueryTable tbl, Collection<ObjectPartitionStatistics> statistics, boolean fullStat);

    /**
     * Get local partition statistics by specified table.
     *
     * @param tbl table to get statistics by.
     * @return collection of partitions statistics.
     */
    Collection<ObjectPartitionStatistics> getLocalPartitionsStatistics(QueryTable tbl);

    /**
     * Clear partition statistics for specified table.
     *
     * @param tbl table to clear statistics by.
     * @param colNames if specified - only statistics by specified columns will be cleared.
     */
    void clearLocalPartitionsStatistics(QueryTable tbl, String ... colNames);

    /**
     * Save specified local partition statistics.
     *
     * @param tbl table.
     * @param partId partition id.
     * @param statistics statistics to save.
     * @param fullStat if {@code True} - replace whole statistics, try to merge with existing - otherwise.
     */
    void saveLocalPartitionStatistics(QueryTable tbl, int partId, ObjectPartitionStatistics statistics, boolean fullStat);

    /**
     * Get partition statistics.
     *
     * @param tbl table.
     * @param partId partition id.
     * @return object partition statistics or {@code null} if there are no statistics collected for such partition.
     */
    ObjectPartitionStatistics getLocalPartitionStatistics(QueryTable tbl, int partId);

    /**
     * Clear partition statistics.
     *
     * @param tbl table.
     * @param partId partiton id.
     */
    void clearLocalPartitionStatistics(QueryTable tbl, int partId);

    /**
     * Save local object statistics.
     *
     * @param tbl object.
     * @param statistics statistics to save.
     * @param fullStat if {@code True} - replace whole statistics, try to merge with existing - otherwise.
     */
    void saveLocalStatistics(QueryTable tbl, ObjectStatistics statistics, boolean fullStat);

    /**
     * Cache saved local statistics.
     *
     * @param tbl object.
     * @param statistics local statistics.
     */
    void cacheLocalStatistics(QueryTable tbl, ObjectStatistics statistics);

    /**
     *
     * @param tbl
     * @param tryLoad
     * @return
     */
    ObjectStatistics getLocalStatistics(QueryTable tbl, boolean tryLoad);

    /**
     * Clear local object statistics.
     *
     * @param tbl object to clear local statistics by.
     * @param colNames if specified - only statistics by specified columns will be cleared.
     */
    void clearLocalStatistics(QueryTable tbl, String ... colNames);

    /**
     * Save global statistics.
     *
     * @param tbl table.
     * @param statistics statistics to save.
     * @param fullStat if {@code True} - replace whole statistics, try to merge with existing - otherwise.
     */
    void saveGlobalStatistics(QueryTable tbl, ObjectStatistics statistics, boolean fullStat);

    /**
     * Cache saved global statistics.
     *
     * @param tbl table.
     * @param statistics statistics to save.
     */
    void cacheGlobalStatistics(QueryTable tbl, ObjectStatistics statistics);

    /**
     * Get global statistics by object.
     *
     * @param tbl table
     * @param tryLoad load flag ?? TODO: TBD
     * @return
     */
    ObjectStatistics getGlobalStatistics(QueryTable tbl, boolean tryLoad);

    /**
     * Clear global statistics by object.
     *
     * @param tbl table
     * @param colNames if specified - only statistics by specified columns will be cleared.
     */
    void clearGlobalStatistics(QueryTable tbl, String ... colNames);

}
