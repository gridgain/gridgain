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
 * Work container for recheck stage.
 */
public class Recheck implements PipelineWorkload {
    /** Recheck keys. */
    private final Map<KeyCacheObject, Map<UUID, GridCacheVersion>> recheckKeys;

    /** Cache name. */
    private final String cacheName;

    /** Partition id. */
    private final int partId;

    /** Attempt number. */
    private final int recheckAttempt;

    /** Repair attempt. */
    private final int repairAttempt;

    /** Session id. */
    private final long sessionId;

    /** Workload chain id. */
    private final UUID workloadChainId;

    /**
     *
     */
    public Recheck(long sessionId, UUID workloadChainId,
        Map<KeyCacheObject, Map<UUID, GridCacheVersion>> recheckKeys, String cacheName,
        int partId, int recheckAttempt, int repairAttempt) {
        this.sessionId = sessionId;
        this.workloadChainId = workloadChainId;
        this.recheckKeys = recheckKeys;
        this.cacheName = cacheName;
        this.partId = partId;
        this.recheckAttempt = recheckAttempt;
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
        return partId;
    }

    /**
     *
     */
    public int recheckAttempt() {
        return recheckAttempt;
    }

    /**
     * @return Repair attempt.
     */
    public int repairAttempt() {
        return repairAttempt;
    }

    /** {@inheritDoc} */
    @Override public long sessionId() {
        return sessionId;
    }

    /** {@inheritDoc} */
    @Override public UUID workloadChainId() {
        return workloadChainId;
    }
}
