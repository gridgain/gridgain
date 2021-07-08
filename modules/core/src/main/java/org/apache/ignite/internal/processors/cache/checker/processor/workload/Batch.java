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
import org.apache.ignite.internal.processors.cache.verify.RepairAlgorithm;

/**
 * Describes batch workload for {@link CollectPartitionKeysByBatchTask} include the pagination.
 */
public class Batch extends PipelineWorkload {
    /** If reconciliation of consistency is needed. */
    private boolean dataReconciliation;

    /** If reconciliation of cache sizes is needed. */
    private boolean cacheSizeReconciliation;

    /** If {@code true} - Partition Reconciliation&Fix. */
    private boolean repair;

    /**
     * Specifies which fix algorithm to use: options {@code PartitionReconciliationRepairMeta.RepairAlg} while
     * repairing doubtful keys.
     */
    private RepairAlgorithm repairAlg;

    /** Cache name. */
    private final String cacheName;

    /** Cache id. */
    private int cacheId;

    /** Partition id. */
    private final int partId;

    /** Lower key, uses for pagination. The first request should set this value to null. */
    private final KeyCacheObject lowerKey;

    /** */
    private Map<UUID, NodePartitionSize> partSizesMap;

    /**
     * @param sesId Session id.
     * @param workloadChainId Workload chain id.
     * @param cacheName Cache name.
     * @param partId Partition id.
     * @param lowerKey Lower key.
     */
    public Batch(long sesId, UUID workloadChainId, String cacheName, int partId, KeyCacheObject lowerKey) {
        super(sesId, workloadChainId);

        this.cacheName = cacheName;
        this.partId = partId;
        this.lowerKey = lowerKey;
    }

    /**
     * @param sesId Session id.
     * @param workloadChainId Workload chain id.
     * @param dataReconciliation Flag indicates that data consistency reconciliation is requested.
     * @param cacheSizeReconciliation Flag indicates that cache size consistency reconciliation is requested.
     * @param repair Flag indicates that an inconsistency should be fixed in accordance with {@code repairAlg} parameter.
     * @param repairAlg Partition reconciliation repair algorithm to be used.
     * @param cacheName Cache name.
     * @param partId Partition id.
     * @param cacheId cache id.
     * @param lowerKey Lower key.
     * @param partSizesMap Map of partition sizes.
     */
    public Batch(boolean dataReconciliation, boolean cacheSizeReconciliation, boolean repair, RepairAlgorithm repairAlg, long sesId, UUID workloadChainId, String cacheName, int cacheId, int partId, KeyCacheObject lowerKey, Map<UUID, NodePartitionSize> partSizesMap) {
        super(sesId, workloadChainId);
        this.dataReconciliation = dataReconciliation;
        this.cacheSizeReconciliation = cacheSizeReconciliation;
        this.repair = repair;
        this.repairAlg = repairAlg;

        this.cacheName = cacheName;
        this.partId = partId;
        this.cacheId = cacheId;
        this.lowerKey = lowerKey;
        this.partSizesMap = partSizesMap;
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
     * @return Repair.
     */
    public boolean repair() {
        return repair;
    }

    /**
     * @return Repair algorithm.
     */
    public RepairAlgorithm repairAlg() {
        return repairAlg;
    }

    /**
     * @return Cache name.
     */
    public String cacheName() {
        return cacheName;
    }

    /**
     * @return Cache ID.
     */
    public int cacheId() {
        return cacheId;
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
     * @return Map of partition sizes for reconciliation of cache sizes.
     */
    public Map<UUID, NodePartitionSize> partSizesMap() {
        return partSizesMap;
    }
}
