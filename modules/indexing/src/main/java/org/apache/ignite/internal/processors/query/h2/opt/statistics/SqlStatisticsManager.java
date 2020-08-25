package org.apache.ignite.internal.processors.query.h2.opt.statistics;

import org.apache.ignite.internal.processors.cache.query.QueryTable;

public interface SqlStatisticsManager {
    ObjectStatistics getLocalStatistics(QueryTable tbl);

    /**
     * Send partition statistics to its new data node, remove local statistics and update local statistics.
     *
     * @param table
     * @param partId
     */
    void onPartEvicted(QueryTable table, int partId);

    /**
     * Get sql statistics repository.
     *
     * @return
     */
    SqlStatisticsRepository statisticsRepository();

}
