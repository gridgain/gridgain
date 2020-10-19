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

import java.util.Collection;

/**
 * Repository to store all necessary statistics. Can request absent ones from cluster and store to
 * {@link SqlStatisticsStoreImpl}.
 */
public interface IgniteStatisticsRepository {

    /**
     * Replace all object statistics with specified ones if fillStat is {@code true} or merge it otherwise.
     *
     * @param key object name.
     * @param statistics collection of tables partition statistics.
     * @param fullStat if {@code true} - replace whole statistics, try to merge with existing - otherwise.
     */
    void saveLocalPartitionsStatistics(StatsKey key, Collection<ObjectPartitionStatisticsImpl> statistics, boolean fullStat);

    /**
     * Get local partition statistics by specified object.
     *
     * @param key object to get statistics by.
     * @return collection of partitions statistics.
     */
    Collection<ObjectPartitionStatisticsImpl> getLocalPartitionsStatistics(StatsKey key);

    /**
     * Clear partition statistics for specified object.
     *
     * @param key object to clear statistics by.
     * @param colNames if specified - only statistics by specified columns will be cleared.
     */
    void clearLocalPartitionsStatistics(StatsKey key, String... colNames);

    /**
     * Save specified local partition statistics.
     *
     * @param tbl object key.
     * @param statistics statistics to save.
     */
    void saveLocalPartitionStatistics(StatsKey key, ObjectPartitionStatisticsImpl statistics);

    /**
     * Get partition statistics.
     *
     * @param key object key.
     * @param partId partition id.
     * @return object partition statistics or {@code null} if there are no statistics collected for such partition.
     */
    ObjectPartitionStatisticsImpl getLocalPartitionStatistics(StatsKey key, int partId);

    /**
     * Clear partition statistics.
     *
     * @param key object key.
     * @param partId partition id.
     */
    void clearLocalPartitionStatistics(StatsKey key, int partId);

    /**
     * Save local object statistics.
     *
     * @param key object key.
     * @param statistics statistics to save.
     * @param fullStat if {@code True} - replace whole statistics, try to merge with existing - otherwise.
     */
    void saveLocalStatistics(StatsKey key, ObjectStatisticsImpl statistics, boolean fullStat);

    /**
     * Calculate and cache saved local statistics.
     *
     * @param key object.
     * @param statistics collection of partitions statistics.
     */
    void cacheLocalStatistics(StatsKey key, Collection<ObjectPartitionStatisticsImpl> statistics);

    /**
     * Get local statistics.
     *
     * @param key object to load statistics by.
     * @return object local statistics or {@code null} if there are no statistics collected for such object.
     */
    ObjectStatisticsImpl getLocalStatistics(StatsKey key);

    /**
     * Clear local object statistics.
     *
     * @param key object to clear local statistics by.
     * @param colNames if specified - only statistics by specified columns will be cleared.
     */
    void clearLocalStatistics(StatsKey key, String... colNames);

    /**
     * Save global statistics.
     *
     * @param key object key.
     * @param statistics statistics to save.
     * @param fullStat if {@code True} - replace whole statistics, try to merge with existing - otherwise.
     */
    void saveGlobalStatistics(StatsKey key, ObjectStatisticsImpl statistics, boolean fullStat);

    /**
     * Get global statistics by object.
     *
     * @param key to get global statistics by.
     * @return object statistics of {@code null} if there are no global statistics for specified object.
     */
    ObjectStatisticsImpl getGlobalStatistics(StatsKey key);

    /**
     * Clear global statistics by object.
     *
     * @param key object key.
     * @param colNames if specified - only statistics by specified columns will be cleared.
     */
    void clearGlobalStatistics(StatsKey key, String... colNames);
}
