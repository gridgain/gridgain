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

package org.apache.ignite.internal.processors.query.h2.twostep;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.cache.CacheException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.query.h2.H2MemoryTracker;
import org.gridgain.internal.h2.index.Cursor;
import org.gridgain.internal.h2.index.Index;
import org.gridgain.internal.h2.result.SearchRow;
import org.jetbrains.annotations.Nullable;

/**
 * Reducer for remote index lookup results.
 */
interface Reducer {
    /**
     * Check if node with given nodeId is data source node for the index.
     *
     * @param nodeId Node ID.
     * @return {@code true} If this index needs data from the given source node, {@code false} otherwise.
     */
    boolean hasSource(UUID nodeId);

    /**
     * Set source nodes.
     *
     * @param nodesToSegmentsCnt Mapping of nodes to expected count of segments to scan.
     */
    void setSources(Map<ClusterNode, Integer> nodesToSegmentsCnt);

    /**
     * @return Return source nodes for this merge index.
     */
    Set<UUID> sources();

    /**
     * Offer result page for reduce.
     *
     * @param page Page.
     */
    void addPage(ReduceResultPage page);

    /**
     * @param pageSize Page size.
     */
    void setPageSize(int pageSize);

    /**
     * Check if all rows has been fetched from all sources.
     *
     * @return {@code true} If all rows has been fetched, {@code false} otherwise.
     */
    boolean fetchedAll();

    /**
     * Find a rows and create a cursor to iterate over the result.
     *
     * @param first Lower bound.
     * @param last Upper bound.
     * @return Cursor instance.
     */
    Cursor find(@Nullable SearchRow first, @Nullable SearchRow last);

    /**
     * Fail cursor callback.
     *
     * @param nodeId Node ID.
     * @param e Exception.
     */
    void onFailure(UUID nodeId, CacheException e);

    /**
     * Rows comparator.
     * See {@link Index}
     */
    interface RowComparator {
        /**
         * Compare two rows.
         *
         * @param rowData the first row
         * @param compare the second row
         * @return 0 if both rows are equal, -1 if the first row is smaller,
         *         otherwise 1
         */
        int compareRows(SearchRow rowData, SearchRow compare);
    }

    /**
     * Set memory tracker for reducer.
     */
    void memoryTracker(H2MemoryTracker memoryTracker);

    /**
     * @return Memory tracker.
     */
    H2MemoryTracker memoryTracker();
}
