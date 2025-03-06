/*
 * Copyright 2024 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.cache.tree.updatelog;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.util.lang.GridCursor;

/**
 * Partition update log.
 */
public interface UpdateLog {
    /**
     * Returns a cursor from lower to upper bounds inclusive.
     */
    GridCursor<UpdateLogRow> find(UpdateLogRow lower, UpdateLogRow upper) throws IgniteCheckedException;

    /**
     * Put a row to the log.
     */
    void put(UpdateLogRow row) throws IgniteCheckedException;

    /**
     * Removes a row from the log.
     */
    void remove(UpdateLogRow row) throws IgniteCheckedException;

    /**
     * Destroys the log.
     */
    void destroy() throws IgniteCheckedException;

    /**
     * Returns the tree or {@code null} if not initialized.
     */
    PartitionLogTree tree() throws IgniteCheckedException;

    /**
     * Returns {@code true} if the tree structure was initialized, {@code false} otherwise.
     */
    boolean hasTree();
}
