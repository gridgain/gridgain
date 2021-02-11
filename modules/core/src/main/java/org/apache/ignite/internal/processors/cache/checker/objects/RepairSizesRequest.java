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
import org.apache.ignite.internal.processors.cache.verify.RepairAlgorithm;

/**
 * Request object contains a set key for repair.
 */
public class RepairSizesRequest extends CachePartitionRequest {
    /** */
    private static final long serialVersionUID = 0L;

    /** Cache name. */
    private String cacheName;

    /** Partition id. */
    private int partId;

    /** Start topology version. */
    private AffinityTopologyVersion startTopVer;

    /** */
    private Map<Integer, Map<Integer, Map<UUID, Long>>> partSizesMap;

    /**
     * @param sesId Session id.
     * @param workloadChainId Workload chain id.
     * @param cacheName Cache name.
     * @param partId Partition id.
     * @param startTopVer Start topology version.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public RepairSizesRequest(long sesId, UUID workloadChainId,
        String cacheName, int partId,
        AffinityTopologyVersion startTopVer,
        Map<Integer, Map<Integer, Map<UUID, Long>>> partSizesMap) {
        super(sesId, workloadChainId);
        this.cacheName = cacheName;
        this.partId = partId;
        this.startTopVer = startTopVer;
        this.partSizesMap = partSizesMap;
    }

    /** {@inheritDoc} */
    @Override public int partitionId() {
        return partId;
    }

    /** {@inheritDoc} */
    @Override public String cacheName() {
        return cacheName;
    }

    public Map<Integer, Map<Integer, Map<UUID, Long>>> partSizesMap() {
        return partSizesMap;
    }

    /**
     * @return Start topology version.
     */
    public AffinityTopologyVersion startTopologyVersion() {
        return startTopVer;
    }
}
