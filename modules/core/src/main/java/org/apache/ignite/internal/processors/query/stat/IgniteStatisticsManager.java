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
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.internal.util.lang.GridTriple;
import org.apache.ignite.internal.util.lang.GridTuple3;

import java.util.Map;
import java.util.UUID;

/**
 * Statistics manager.
 */
public interface IgniteStatisticsManager {
    /**
     * Collect object statistics.
     *
     * @param schemaName Schema name.
     * @param objName Object to collect statistics by.
     * @param colNames Columns to collect statistics by.
     * @throws IgniteCheckedException  Throws in case of errors.
     * @return future to get fini
     */
    public void collectObjectStatistics(String schemaName, String objName, String... colNames) throws IgniteCheckedException;

    /**
     * Collect objects statistics.
     *
     * @param keys Collection of keys to collect statistics by (schema, obj, columns).
     * @return Future to track progress and cancel collection.
     * @throws IgniteCheckedException In case of errors.
     */
    public StatsCollectionFuture<Map<GridTuple3<String, String, String[]>, ObjectStatistics>> collectObjectStatisticsAsync(
        GridTuple3<String, String, String[]>... keys
    ) throws IgniteCheckedException;

    /**
     * Cancel object statistics collection.
     *
     * @param colId Collection id.
     * @return {@code true} if collection was cancelled, {@code false} if specified collection wasn't found.
     */
    public boolean cancelObjectStatisticsGathering(UUID colId);

    /**
     * Get local statistics by object.
     *
     * @param schemaName Schema name.
     * @param objName Object to collect statistics by.
     * @return Object statistics or {@code null} if there are no available statistics by specified object.
     */
    public ObjectStatistics getLocalStatistics(String schemaName, String objName);

    /**
     * Clear object statistics.
     *
     * @param schemaName Schema name.
     * @param objName Object to collect statistics by.
     * @param colNames Columns to remove statistics by.
     * @throws IgniteCheckedException In case of errors.
     */
    public void clearObjectStatistics(String schemaName, String objName, String... colNames) throws IgniteCheckedException;

    /**
     * Clear object statistics.
     *
     * @param keys Collection of keys to collect statistics by (schema, obj, columns).
     * @throws IgniteCheckedException In case of errors.
     */
    public void clearObjectStatistics(GridTuple3<String, String, String[]>... keys) throws IgniteCheckedException;
}
