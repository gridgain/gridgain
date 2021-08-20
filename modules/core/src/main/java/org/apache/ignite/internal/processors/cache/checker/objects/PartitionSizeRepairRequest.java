/*
 * Copyright 2021 GridGain Systems, Inc. and Contributors.
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

/**
 * Request object contains a set key for repair.
 */
public class PartitionSizeRepairRequest extends CachePartitionRequest {
    /** */
    private static final long serialVersionUID = 0L;

    /** Cache name. */
    private String cacheName;

    /** Partition id. */
    private int partId;

    private boolean repair;

    /** Start topology version. */
    private AffinityTopologyVersion startTopVer;

    /** */
    private Map<UUID, NodePartitionSize> partSizesMap;

    /**
     * @param sesId Session id.
     * @param workloadChainId Workload chain id.
     * @param cacheName Cache name.
     * @param partId Partition id.
     * @param startTopVer Start topology version.
     * @param partSizesMap Map of partition sizes.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public PartitionSizeRepairRequest(long sesId, UUID workloadChainId, String cacheName, int partId, boolean repair,
        AffinityTopologyVersion startTopVer, Map<UUID, NodePartitionSize> partSizesMap) {
        super(sesId, workloadChainId);
        this.cacheName = cacheName;
        this.partId = partId;
        this.repair = repair;
        this.startTopVer = startTopVer;
        this.partSizesMap = partSizesMap;
    }

    /** {@inheritDoc} */
    @Override public int partitionId() {
        return partId;
    }

    public boolean isRepair() {
        return repair;
    }

    public void setRepair(boolean repair) {
        this.repair = repair;
    }

    /** {@inheritDoc} */
    @Override public String cacheName() {
        return cacheName;
    }

    /**
     * @return Start topology version.
     */
    public AffinityTopologyVersion startTopologyVersion() {
        return startTopVer;
    }

    /**
     * @return Map of partition sizes for reconciliation of cache sizes.
     */
    public Map<UUID, NodePartitionSize> partSizesMap() {
        return partSizesMap;
    }
}
