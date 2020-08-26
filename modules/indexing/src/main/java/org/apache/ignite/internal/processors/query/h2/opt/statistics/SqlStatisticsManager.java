package org.apache.ignite.internal.processors.query.h2.opt.statistics;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.query.QueryTable;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;

public interface SqlStatisticsManager {

    /**
     * Refresh statistics by specified tables or refresh whole available statistics.
     *
     * @param tbls tables to refresh statistics by.
     */
    void refreshStatistics(GridH2Table ... tbls);

    /**
     * Collect object
     *
     * @param tbl object to collect statistics by.
     * @param colNames columns to collect statistics by.
     * @throws IgniteCheckedException
     */
    void collectObjectStatistics(GridH2Table tbl, String ... colNames) throws IgniteCheckedException;

    /**
     * Get local statistics by object.
     *
     * @param tbl object to get statistics by.
     * @return object statistics or {@code null} if there are no available statistics by specified object.
     */
    ObjectStatistics getLocalStatistics(QueryTable tbl);

    /**
     * Clear object statistics.
     *
     * @param tbl object to clear statistics by.
     * @param colNames columns to remove statistics by.
     */
    void clearObjectStatistics(QueryTable tbl, String ... colNames);

    /**
     * Send partition statistics to its new data node, remove local statistics and update local statistics.
     *
     * @param table
     * @param partId
     */
    void onPartEvicted(QueryTable table, int partId);
}
