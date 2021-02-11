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

import org.apache.ignite.IgniteCheckedException;

/**
 * Statistics manager. Coordinate statistics collection and act as source of statistics.
 */
public interface IgniteStatisticsManager {
    /**
     * Gather object statistics.
     *
     * @param target Target to gather statistics by.
     * @throws IgniteCheckedException  Throws in case of errors.
     */
    public void updateStatistics(StatisticsTarget... target) throws IgniteCheckedException;

    /**
     * Clear object statistics.
     *
     * @param targets Collection of target to collect statistics by (schema, obj, columns).
     * @throws IgniteCheckedException In case of errors (for example: unsupported feature)
     */
    public void dropStatistics(StatisticsTarget... targets) throws IgniteCheckedException;

    /** Drop all statistics. */
    void dropAll() throws IgniteCheckedException;

    /**
     * Get local statistics by object.
     *
     * @param schemaName Schema name.
     * @param objName Object to collect statistics by.
     * @return Object statistics or {@code null} if there are no available statistics by specified object.
     */
    public ObjectStatistics getLocalStatistics(String schemaName, String objName);

    /**
     * To track statistics invalidation. Skip value if no statistics for the given table exists.
     *
     * @param schemaName Schema name.
     * @param objName Object name.
     * @param partId Partition id.
     * @param keyBytes Row key bytes.
     */
    public void onRowUpdated(String schemaName, String objName, int partId, byte[] keyBytes);

    /**
     * Stop statistic manager.
     */
    public void stop();
}
