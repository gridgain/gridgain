/*
 * Copyright 2021 GridGain Systems, Inc. and Contributors.
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
import org.apache.ignite.internal.processors.cache.checker.objects.NodePartitionSize;
import org.apache.ignite.internal.processors.cache.checker.processor.PipelineWorkload;
import org.apache.ignite.internal.processors.cache.checker.tasks.PartitionSizeRepairRequestTask;

/**
 * Describes batch workload for {@link PartitionSizeRepairRequestTask}.
 */
public class PartitionSizeRepair extends PipelineWorkload {
    /** Cache name. */
    private final String cacheName;

    /** Partition id. */
    private final int partId;

    /**
     * If {@code true} need to check and repair.
     * If {@code false} need to check only.
     */
    private boolean repair;

    /** Map of partition sizes for reconciliation of cache sizes. */
    private Map<UUID, NodePartitionSize> partSizesMap;

    /**
     * @param sesId Session id.
     * @param workloadChainId Workload chain id.
     * @param cacheName Cache name.
     * @param partId Partition id.
     * @param partSizesMap Map of partition sizes.
     */
    public PartitionSizeRepair(
        long sesId,
        UUID workloadChainId,
        String cacheName,
        int partId,
        boolean repair,
        Map<UUID, NodePartitionSize> partSizesMap
    ) {
        super(sesId, workloadChainId);

        this.cacheName = cacheName;
        this.partId = partId;
        this.repair = repair;
        this.partSizesMap = partSizesMap;
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
     * @return Map of partition sizes for reconciliation of cache sizes.
     */
    public Map<UUID, NodePartitionSize> partSizesMap() {
        return partSizesMap;
    }

    /**
     * @return Repair.
     */
    public boolean repair() {
        return repair;
    }

    /**
     * @param repair Repair.
     */
    public void repair(boolean repair) {
        this.repair = repair;
    }
}
