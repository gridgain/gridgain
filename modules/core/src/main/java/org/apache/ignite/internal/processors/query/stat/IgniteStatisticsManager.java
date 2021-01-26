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

import java.util.Map;
import java.util.UUID;

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
    public void gatherObjectStatistics(StatisticsTarget target) throws IgniteCheckedException;

    /**
     * Gather objects statistics.
     *
     * @param targets Gathering of targets to collect statistics by (schema, obj, columns).
     * @return Array of futures, to track progress and cancel collection on each of specified cache group.
     */
    public StatisticsGatheringFuture<Map<StatisticsTarget, ObjectStatistics>>[] gatherObjectStatisticsAsync(
        StatisticsTarget... targets
    );

    /**
     * Cancel object statistics gathering.
     *
     * @param gatId Gathering id.
     * @return {@code true} if gathering was cancelled, {@code false} if specified gathering wasn't found.
     * @throws IgniteCheckedException In case of errors (for example: unsupported feature)
     */
    public boolean cancelObjectStatisticsGathering(UUID gatId) throws IgniteCheckedException;

    /**
     * Get local statistics by object.
     *
     * @param schemaName Schema name.
     * @param objName Object to collect statistics by.
     * @return Object statistics or {@code null} if there are no available statistics by specified object.
     */
    public ObjectStatistics getLocalStatistics(String schemaName, String objName);

    /**
     * Get global statistics by object.
     *
     * @param schemaName Schema name.
     * @param objName Object to collect statistics by.
     * @return Object statistics or {@code null} if there are no available statistics by specified object.
     * @throws IgniteCheckedException In case of errors (for example: unsupported feature)
     */
    public ObjectStatistics getGlobalStatistics(String schemaName, String objName) throws IgniteCheckedException;

    /**
     * Clear object statistics.
     *
     * @param targets Collection of target to collect statistics by (schema, obj, columns).
     * @throws IgniteCheckedException In case of errors (for example: unsupported feature)
     */
    public void clearObjectStatistics(StatisticsTarget... targets) throws IgniteCheckedException;
}
