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
import org.apache.ignite.internal.processors.cache.checker.objects.NodePartitionSize;
import org.apache.ignite.internal.processors.cache.checker.processor.PipelineWorkload;
import org.apache.ignite.internal.processors.cache.checker.tasks.CollectPartitionKeysByBatchTask;

/**
 * Describes batch workload for {@link CollectPartitionKeysByBatchTask} include the pagination.
 */
public class Batch extends PipelineWorkload {
    /** If reconciliation of consistency is needed. */
    private boolean dataReconciliation;

    /** If reconciliation of cache sizes is needed. */
    private boolean cacheSizeReconciliation;

    /** Cache name. */
    private final String cacheName;

    /** Partition id. */
    private final int partId;

    /** Lower key, uses for pagination. The first request should set this value to null. */
    private final KeyCacheObject lowerKey;

    /** Map of partition sizes for reconciliation of cache sizes. */
    private Map<UUID, NodePartitionSize> partSizesMap;

    /** Indicates that inconsistent partition sizes should be repaired. */
    private final boolean repair;

    /**
     * @param sesId Session id.
     * @param workloadChainId Workload chain id.
     * @param cacheName Cache name.
     * @param partId Partition id.
     * @param lowerKey Lower key.
     * @param repair Repair partition sizes flag.
     */
    public Batch(long sesId, UUID workloadChainId, String cacheName, int partId, KeyCacheObject lowerKey, boolean repair) {
        super(sesId, workloadChainId);

        this.cacheName = cacheName;
        this.partId = partId;
        this.lowerKey = lowerKey;
        this.repair = repair;
    }

    /**
     * @param sesId Session id.
     * @param workloadChainId Workload chain id.
     * @param dataReconciliation Flag indicates that data consistency reconciliation is requested.
     * @param cacheSizeReconciliation Flag indicates that cache size consistency reconciliation is requested.
     * @param cacheName Cache name.
     * @param partId Partition id.
     * @param lowerKey Lower key.
     * @param partSizesMap Map of partition sizes.
     * @param repair Repair partition sizes flag.
     */
    public Batch(
        boolean dataReconciliation,
        boolean cacheSizeReconciliation,
        long sesId,
        UUID workloadChainId,
        String cacheName,
        int partId,
        KeyCacheObject lowerKey,
        Map<UUID, NodePartitionSize> partSizesMap,
        boolean repair
    ) {
        super(sesId, workloadChainId);
        this.dataReconciliation = dataReconciliation;
        this.cacheSizeReconciliation = cacheSizeReconciliation;

        this.cacheName = cacheName;
        this.partId = partId;
        this.lowerKey = lowerKey;
        this.partSizesMap = partSizesMap;
        this.repair = repair;
    }

    /**
     * @return Data reconciliation.
     */
    public boolean dataReconciliation() {
        return dataReconciliation;
    }

    /**
     * @return Cache size reconciliation.
     */
    public boolean cacheSizeReconciliation() {
        return cacheSizeReconciliation;
    }

    /**
     * @return Cache name.
     */
    public String cacheName() {
        return cacheName;
    }

    /**
     * @return Partition ID.
     */
    public int partitionId() {
        return partId;
    }

    /**
     * @return Lower key.
     */
    public KeyCacheObject lowerKey() {
        return lowerKey;
    }

    /**
     * @return Repair partition sizes flag.
     */
    public boolean repair() {
        return repair;
    }

    /**
     * @return Map of partition sizes for reconciliation of cache sizes.
     */
    public Map<UUID, NodePartitionSize> partSizesMap() {
        return partSizesMap;
    }
}
