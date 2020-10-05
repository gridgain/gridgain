/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
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
 * Statistics manager.
 */
public interface IgniteStatisticsManager {
    /**
     * Collect object statistics.
     *
     * @param schemaName schema name.
     * @param objName object to collect statistics by.
     * @param colNames columns to collect statistics by.
     * @throws IgniteCheckedException  in case of errors.
     */
    void collectObjectStatistics(String schemaName, String objName, String... colNames) throws IgniteCheckedException;

    /**
     * Get local statistics by object.
     *
     * @param schemaName schema name.
     * @param objName object to collect statistics by.
     * @return object statistics or {@code null} if there are no available statistics by specified object.
     */
    ObjectStatistics getLocalStatistics(String schemaName, String objName);

    /**
     * Clear object statistics.
     *
     * @param schemaName schema name.
     * @param objName object to collect statistics by.
     * @param colNames columns to remove statistics by.
     */
    void clearObjectStatistics(String schemaName, String objName, String... colNames);
}
