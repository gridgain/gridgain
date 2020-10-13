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

import org.apache.ignite.internal.processors.cache.query.QueryTable;

import java.util.Collection;

/**
 * Repository to store all necessary statistics. Can request absent ones from cluster and store to
 * {@link SqlStatisticsStoreImpl}.
 */
public interface IgniteStatisticsRepository {

    /**
     * Replace all table statistics with specified ones.
     *
     * @param tbl table.
     * @param statistics collection of tables partition statistics.
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
    void clearLocalPartitionsStatistics(QueryTable tbl, String... colNames);

    /**
     * Save specified local partition statistics.
     *
     * @param tbl table.
     * @param statistics statistics to save.
     */
    void saveLocalPartitionStatistics(QueryTable tbl, ObjectPartitionStatistics statistics);

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
     * @param partId partition id.
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
     * Calculate and cache saved local statistics.
     *
     * @param tbl object.
     * @param statistics collection of partitions statistics.
     */
    void cacheLocalStatistics(QueryTable tbl, Collection<ObjectPartitionStatistics> statistics);

    /**
     * Get local statistics.
     *
     * @param tbl object to load statistics by.
     * @return object local statistics or {@code null} if there are no statistics collected for such object.
     */
    ObjectStatistics getLocalStatistics(QueryTable tbl);

    /**
     * Clear local object statistics.
     *
     * @param tbl object to clear local statistics by.
     * @param colNames if specified - only statistics by specified columns will be cleared.
     */
    void clearLocalStatistics(QueryTable tbl, String... colNames);

    /**
     * Save global statistics.
     *
     * @param tbl table.
     * @param statistics statistics to save.
     * @param fullStat if {@code True} - replace whole statistics, try to merge with existing - otherwise.
     */
    void saveGlobalStatistics(QueryTable tbl, ObjectStatistics statistics, boolean fullStat);

    /**
     * Get global statistics by object.
     *
     * @param tbl table.
     * @return object statistics of {@code null} if there are no global statistics for specified object.
     */
    ObjectStatistics getGlobalStatistics(QueryTable tbl);

    /**
     * Clear global statistics by object.
     *
     * @param tbl table.
     * @param colNames if specified - only statistics by specified columns will be cleared.
     */
    void clearGlobalStatistics(QueryTable tbl, String ... colNames);
}