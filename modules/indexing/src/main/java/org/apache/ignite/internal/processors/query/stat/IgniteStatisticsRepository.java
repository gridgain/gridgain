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
 * Repository to store all necessary statistics. Can request absent ones from cluster and store to repository.
 */
public interface IgniteStatisticsRepository {
    /**
     * Replace all object statistics with specified ones.
     *
     * @param key Object key.
     * @param statistics Collection of tables partition statistics.
     */
    public void saveLocalPartitionsStatistics(StatisticsKey key, Collection<ObjectPartitionStatisticsImpl> statistics);

    /**
     * Merge existing statistics with specified ones.
     *
     * @param key Object key.
     * @param statistics Collection of tables partition statistics.
     * @return Merged statistics by same partitions.
     */
    public Collection<ObjectPartitionStatisticsImpl> mergeLocalPartitionsStatistics(
        StatisticsKey key,
        Collection<ObjectPartitionStatisticsImpl> statistics
    );

    /**
     * Get local partition statistics by specified object.
     *
     * @param key Object to get statistics by.
     * @return Collection of partitions statistics.
     */
    public Collection<ObjectPartitionStatisticsImpl> getLocalPartitionsStatistics(StatisticsKey key);

    /**
     * Clear partition statistics for specified object.
     *
     * @param key Object to clear statistics by.
     * @param colNames if specified - only statistics by specified columns will be cleared.
     */
    public void clearLocalPartitionsStatistics(StatisticsKey key, String... colNames);

    /**
     * Save specified local partition statistics.
     *
     * @param key Object key.
     * @param statistics Statistics to save.
     */
    public void saveLocalPartitionStatistics(StatisticsKey key, ObjectPartitionStatisticsImpl statistics);

    /**
     * Get partition statistics.
     *
     * @param key Object key.
     * @param partId Partition id.
     * @return Object partition statistics or {@code null} if there are no statistics collected for such partition.
     */
    public ObjectPartitionStatisticsImpl getLocalPartitionStatistics(StatisticsKey key, int partId);

    /**
     * Clear partition statistics.
     *
     * @param key Object key.
     * @param partId Partition id.
     */
    public void clearLocalPartitionStatistics(StatisticsKey key, int partId);

    /**
     * Save local object statistics.
     *
     * @param key Object key.
     * @param statistics Statistics to save.
     */
    public void saveLocalStatistics(StatisticsKey key, ObjectStatisticsImpl statistics);

    /**
     * Merge local object statistics.
     *
     * @param key Object key.
     * @param statistics Statistics to merge.
     * @return Merged statistics.
     */
    public ObjectStatisticsImpl mergeLocalStatistics(StatisticsKey key, ObjectStatisticsImpl statistics);

    /**
     * Calculate and cache saved local statistics.
     *
     * @param key Object key.
     * @param statistics Collection of partitions statistics.
     */
    public void cacheLocalStatistics(StatisticsKey key, Collection<ObjectPartitionStatisticsImpl> statistics);

    /**
     * Get local statistics.
     *
     * @param key Object key to load statistics by.
     * @return Object local statistics or {@code null} if there are no statistics collected for such object.
     */
    public ObjectStatisticsImpl getLocalStatistics(StatisticsKey key);

    /**
     * Clear local object statistics.
     *
     * @param key Object key to clear local statistics by.
     * @param colNames If specified - only statistics by specified columns will be cleared.
     */
    public void clearLocalStatistics(StatisticsKey key, String... colNames);

    /**
     * Save global statistics.
     *
     * @param key Object key.
     * @param statistics Statistics to save.
     */
    public void saveGlobalStatistics(StatisticsKey key, ObjectStatisticsImpl statistics);

    /**
     * Merge global statistics.
     *
     * @param key Object key.
     * @param statistics Statistics to merge.
     * @return Merged statistics.
     */
    public ObjectStatisticsImpl mergeGlobalStatistics(StatisticsKey key, ObjectStatisticsImpl statistics);

    /**
     * Get global statistics by object.
     *
     * @param key To get global statistics by.
     * @return Object statistics of {@code null} if there are no global statistics for specified object.
     */
    public ObjectStatisticsImpl getGlobalStatistics(StatisticsKey key);

    /**
     * Clear global statistics by object.
     *
     * @param key Object key.
     * @param colNames If specified - only statistics by specified columns will be cleared.
     */
    public void clearGlobalStatistics(StatisticsKey key, String... colNames);
}
