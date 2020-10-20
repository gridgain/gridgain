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

package org.apache.ignite.internal.processors.cache.persistence.checkpoint;

import java.util.concurrent.Executor;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.persistence.partstate.PartitionAllocationMap;
import org.jetbrains.annotations.Nullable;

/**
 * Listener which methods will be called in a corresponded checkpoint life cycle period.
 */
public interface CheckpointListener {
    /**
     * Context with information about current snapshots.
     */
    public interface Context {
        /**
         * @return Checkpoint progress callback.
         */
        public CheckpointProgress progress();

        /**
         * @return {@code True} if a snapshot have to be created after.
         */
        public boolean nextSnapshot();

        /**
         * @return Partition allocation statistic map
         */
        public PartitionAllocationMap partitionStatMap();

        /**
         * @param cacheOrGrpName Cache or group name.
         */
        public boolean needToSnapshot(String cacheOrGrpName);

        /**
         * @return Context executor.
         */
        @Nullable public Executor executor();
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void onMarkCheckpointBegin(Context ctx) throws IgniteCheckedException;

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void onCheckpointBegin(Context ctx) throws IgniteCheckedException;

    /**
     * Do some actions before checkpoint write lock.
     *
     * @throws IgniteCheckedException If failed.
     */
    public void beforeCheckpointBegin(Context ctx) throws IgniteCheckedException;

    /**
     * Do some actions after checkpoint end.
     *
     * @throws IgniteCheckedException If failed.
     */
    default void afterCheckpointEnd(Context ctx) throws IgniteCheckedException {

    }
}
