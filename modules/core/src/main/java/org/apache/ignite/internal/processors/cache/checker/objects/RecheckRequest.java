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

package org.apache.ignite.internal.processors.cache.checker.objects;

import java.util.UUID;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;

import java.util.Collection;

/**
 * Request object contains a set key for recheck.
 */
public class RecheckRequest extends CachePartitionRequest {
    /**
     *
     */
    private static final long serialVersionUID = 0L;

    /** Recheck keys. */
    private Collection<KeyCacheObject> recheckKeys;

    /** Cache name. */
    private String cacheName;

    /** Partition id. */
    private int partId;

    /**
     * @param sesId Session id.
     * @param workloadChainId Workload chain id.
     * @param recheckKeys Recheck keys.
     * @param cacheName Cache name.
     * @param partId Partition id.
     * @param startTopVer Start topology version.
     */
    public RecheckRequest(long sesId, UUID workloadChainId, Collection<KeyCacheObject> recheckKeys,
        String cacheName, int partId,
        AffinityTopologyVersion startTopVer) {
        super(sesId, workloadChainId);
        this.recheckKeys = recheckKeys;
        this.cacheName = cacheName;
        this.partId = partId;
    }

    /**
     * @return Keys to recheck.
     */
    public Collection<KeyCacheObject> recheckKeys() {
        return recheckKeys;
    }

    /** {@inheritDoc} */
    @Override public String cacheName() {
        return cacheName;
    }

    /** {@inheritDoc} */
    @Override public int partitionId() {
        return partId;
    }
}
