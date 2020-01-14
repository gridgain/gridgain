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
import org.apache.ignite.internal.processors.cache.checker.objects.VersionedValue;
import org.apache.ignite.internal.processors.cache.checker.processor.PipelineWorkload;

/**
 *
 */
public class Repair implements PipelineWorkload {
    /**
     *
     */
    private String cacheName;

    /**
     *
     */
    private int partId;

    /** Attempt number. */
    private int attempt;

    private UUID sessionId;

    /**
     *
     */
    private Map<KeyCacheObject, Map<UUID, VersionedValue>> data;

    /** */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType") public Repair(
        UUID sessionId,
        String cacheName,
        int partId,
        Map<KeyCacheObject, Map<UUID, VersionedValue>> data,
        int attempt
    ) {
        this.sessionId = sessionId;
        this.cacheName = cacheName;
        this.partId = partId;
        this.data = data;
        this.attempt = attempt;
    }

    /**
     * @return Cache name.
     */
    public String cacheName() {
        return cacheName;
    }

    /**
     * @return Partition id.
     */
    public int partitionId() {
        return partId;
    }

    /**
     * @return Data.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public Map<KeyCacheObject, Map<UUID, VersionedValue>> data() {
        return data;
    }

    /**
     * @return Attempt number.
     */
    public int attempt() {
        return attempt;
    }

    /** {@inheritDoc} */
    @Override public UUID getSessionId() {
        return sessionId;
    }
}
