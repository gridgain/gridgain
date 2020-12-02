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

package org.apache.ignite.internal.processors.cache.persistence.defragmentation;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointTimeoutLock;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.util.collection.IntMap;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;

/**
 *
 */
public interface GridQueryIndexingDefragmentation {
    /**
     * Defragment index partition.
     *
     * @param grpCtx Group context.
     * @param newCtx Group context for defragmented group.
     * @param partPageMem Page memory.
     * @param mappingByPartition Page id mapping.
     * @param cpLock Checkpoint lock.
     * @param cancellationChecker Cancellation checker.
     * @param log Logger.
     * @param defragmentationThreadPool Thread pool for defragmentation.
     * @throws IgniteCheckedException If failed.
     */
    void defragment(
        CacheGroupContext grpCtx,
        CacheGroupContext newCtx,
        PageMemoryEx partPageMem,
        IntMap<LinkMap> mappingByPartition,
        CheckpointTimeoutLock cpLock,
        Runnable cancellationChecker,
        IgniteLogger log,
        IgniteThreadPoolExecutor defragmentationThreadPool
    ) throws IgniteCheckedException;
}
