/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.checker.objects;

import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;

/**
 * Request object contains a set key for repair.
 */
public class RepairRequest extends CachePartitionRequest {
    /** */
    private static final long serialVersionUID = 0L;

    private Map<KeyCacheObject, Map<UUID, VersionedValue>> data;

    /** Cache name. */
    private String cacheName;

    /** Partition id. */
    private int partId;

    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public RepairRequest(Map<KeyCacheObject, Map<UUID, VersionedValue>> data, String cacheName, int partId,
        AffinityTopologyVersion startTopVer) {
        this.data = data;
        this.cacheName = cacheName;
        this.partId = partId;
    }

    /**
     * @return Data.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public Map<KeyCacheObject, Map<UUID, VersionedValue>> data() {
        return data;
    }

    /** {@inheritDoc} */
    @Override public int partitionId() {
        return partId;
    }

    /** {@inheritDoc} */
    @Override public String cacheName() {
        return cacheName;
    }
}
