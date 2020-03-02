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
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Pageable partition batch request.
 */
public class PartitionBatchRequest extends CachePartitionRequest {
    /**
     *
     */
    private static final long serialVersionUID = 0L;

    /** Cache name. */
    private final String cacheName;

    /** Partition id. */
    private final int partId;

    /** Batch size. */
    private final int batchSize;

    /**
     * Lower key, uses for pagination. The first request should set this value to null. This key excluding.
     */
    private final KeyCacheObject lowerKey;

    /**
     * Reconciliation start topology version.
     */
    private final AffinityTopologyVersion startTopVer;

    /**
     * @param sesId Session id.
     * @param workloadChainId Workload chain id.
     * @param cacheName Cache name.
     * @param partId Partition id.
     * @param batchSize Batch size.
     * @param lowerKey Lower key.
     * @param startTopVer Start topology version.
     */
    public PartitionBatchRequest(
        long sesId,
        UUID workloadChainId,
        String cacheName,
        int partId,
        int batchSize,
        KeyCacheObject lowerKey,
        AffinityTopologyVersion startTopVer
    ) {
        super(sesId, workloadChainId);
        this.cacheName = cacheName;
        this.partId = partId;
        this.batchSize = batchSize;
        this.lowerKey = lowerKey;
        this.startTopVer = startTopVer;
    }

    /** {@inheritDoc} */
    @Override public int partitionId() {
        return partId;
    }

    /** {@inheritDoc} */
    @Override public String cacheName() {
        return cacheName;
    }

    /**
     * @return Batch size.
     */
    public int batchSize() {
        return batchSize;
    }

    /**
     * @return Lowest key of current batch.
     */
    public KeyCacheObject lowerKey() {
        return lowerKey;
    }

    /**
     * @return Reconciliation start topology version.
     */
    public AffinityTopologyVersion startTopVer() {
        return startTopVer;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(PartitionBatchRequest.class, this);
    }
}
