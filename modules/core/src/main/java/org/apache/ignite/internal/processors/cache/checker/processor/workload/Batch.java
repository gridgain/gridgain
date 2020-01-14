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

package org.apache.ignite.internal.processors.cache.checker.processor.workload;

import java.util.UUID;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.checker.processor.PipelineWorkload;
import org.apache.ignite.internal.processors.cache.checker.tasks.CollectPartitionKeysByBatchTask;

/**
 * Describes batch workload for {@link CollectPartitionKeysByBatchTask} include the pagination.
 */
public class Batch implements PipelineWorkload {
    /** Cache name. */
    private final String cacheName;

    /** Partition id. */
    private final int partitionId;

    /** Lower key, uses for pagination. The first request should set this value to null. */
    private final KeyCacheObject lowerKey;

    /**
     *
     */
    private final UUID sessionId;

    /**
     *
     */
    public Batch(UUID sessionId, String cacheName, int partId, KeyCacheObject lowerKey) {
        this.sessionId = sessionId;
        this.cacheName = cacheName;
        this.partitionId = partId;
        this.lowerKey = lowerKey;
    }

    /**
     *
     */
    public String cacheName() {
        return cacheName;
    }

    /**
     *
     */
    public int partitionId() {
        return partitionId;
    }

    /**
     *
     */
    public KeyCacheObject lowerKey() {
        return lowerKey;
    }

    @Override public UUID getSessionId() {
        return sessionId;
    }
}