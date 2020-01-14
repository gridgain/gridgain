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

import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.checker.processor.PipelineWorkload;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;

/**
 *
 */
public class Recheck implements PipelineWorkload {
    /** Recheck keys. */
    private final Map<KeyCacheObject, Map<UUID, GridCacheVersion>> recheckKeys;

    /** Cache name. */
    private final String cacheName;

    /** Partition id. */
    private final int partitionId;

    /** Attempt number. */
    private final int attempt;

    /** Repair attempt. */
    private final int repairAttempt;

    /**
     *
     */
    private final UUID sessionId;

    /**
     *
     */
    public Recheck(UUID sessionId, Map<KeyCacheObject, Map<UUID, GridCacheVersion>> recheckKeys, String cacheName,
        int partitionId, int attempt, int repairAttempt) {
        this.sessionId = sessionId;
        this.recheckKeys = recheckKeys;
        this.cacheName = cacheName;
        this.partitionId = partitionId;
        this.attempt = attempt;
        this.repairAttempt = repairAttempt;
    }

    /**
     *
     */
    public Map<KeyCacheObject, Map<UUID, GridCacheVersion>> recheckKeys() {
        return recheckKeys;
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
    public int attempt() {
        return attempt;
    }

    /**
     * @return Repair attempt.
     */
    public int repairAttempt() {
        return repairAttempt;
    }

    /** {@inheritDoc} */
    @Override public UUID getSessionId() {
        return sessionId;
    }
}
