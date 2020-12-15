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
 * Statistics store interface.
 */
public interface IgniteStatisticsStore {
    /**
     * Clear statistics of any type for any objects;
     */
    public void clearAllStatistics();

    /**
     * Replace all table statistics with specified ones ().
     *
     * @param key Statistics key to replace statistics by.
     * @param statistics Collection of partition level statistics.
     */
    public void replaceLocalPartitionsStatistics(StatsKey key, Collection<ObjectPartitionStatisticsImpl> statistics);

    /**
     * Get local partition statistics by specified object.
     *
     * @param key Key to get statistics by.
     * @return Collection of partitions statistics.
     */
    public Collection<ObjectPartitionStatisticsImpl> getLocalPartitionsStatistics(StatsKey key);

    /**
     * Clear partition statistics for specified object.
     *
     * @param key Key to clear statistics by.
     */
    public void clearLocalPartitionsStatistics(StatsKey key);

    /**
     * Get partition statistics.
     *
     * @param key Key to get partition statistics by.
     * @param partId Partition id.
     * @return Object partition statistics or {@code null} if there are no statistics collected for such partition.
     */
    public ObjectPartitionStatisticsImpl getLocalPartitionStatistics(StatsKey key, int partId);

    /**
     * Clear partition statistics.
     *
     * @param key Object which statistics needs to be cleaned.
     * @param partId Partition id.
     */
    public void clearLocalPartitionStatistics(StatsKey key, int partId);

    /**
     * Clear partitions statistics.
     *
     * @param key Object which statistics need to be cleaned.
     * @param partIds Collection of partition ids.
     */
    public void clearLocalPartitionsStatistics(StatsKey key, Collection<Integer> partIds);

    /**
     * Save partition statistics.
     *
     * @param key Object which partition statistics belongs to.
     * @param statistics Statistics to save.
     */
    public void saveLocalPartitionStatistics(StatsKey key, ObjectPartitionStatisticsImpl statistics);
}
