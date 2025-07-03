/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.cache.distributed.dht.preloader;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Information about supplier for specific partition.
 */
public class SupplyGroupInfo {
    /** Cache group ID. */
    private final int groupId;

    /** Map of suppliers categorized by rebalance reason, node ID, and partition details. */
    private final Map<FullRebalanceReason, Map<UUID, List<T3<Integer, Long, Long>>>> suppliers = new HashMap<>();

    /**
     * Constructs a new instance of SupplyGroupInfo for the given cache group ID.
     *
     * @param groupId Cache group ID.
     */
    public SupplyGroupInfo(int groupId) {
        this.groupId = groupId;
    }

    /**
     * Adds information about a partition that has no history.
     *
     * @param part Partition ID.
     * @param nodeId Node ID of the supplier.
     */
    public void addPartitionNoHistory(int part, UUID nodeId) {
        suppliers.computeIfAbsent(FullRebalanceReason.NO_HISTORY_PARTITIONS, r -> new HashMap<>())
            .computeIfAbsent(nodeId, n -> new ArrayList<>())
            .add(new T3<>(part, null, null));
    }

    /**
     * Adds information about a partition with insufficient size for historical rebalance.
     *
     * @param part Partition ID.
     * @param nodeId Node ID of the supplier.
     * @param size Current size of the partition.
     * @param threshold Required size threshold for rebalance.
     */
    public void addPartitionInsufficientSize(int part, UUID nodeId, long size, long threshold) {
        suppliers.computeIfAbsent(FullRebalanceReason.INSUFFICIENT_PARTITION_SIZE, r -> new HashMap<>())
            .computeIfAbsent(nodeId, n -> new ArrayList<>())
            .add(new T3<>(part, size, threshold));
    }

    /**
     * Adds information about a partition where the available counter exceeds the demanded counter.
     *
     * @param part Partition ID.
     * @param nodeId Node ID of the supplier.
     * @param availableCntr Available counter value.
     * @param demandedCntr Demanded counter value.
     */
    public void addPartitionCounterExceedsDemand(int part, UUID nodeId, long availableCntr, long demandedCntr) {
        suppliers.computeIfAbsent(FullRebalanceReason.COUNTER_EXCEEDS_DEMAND, r -> new HashMap<>())
            .computeIfAbsent(nodeId, n -> new ArrayList<>())
            .add(new T3<>(part, availableCntr, demandedCntr));
    }

    /**
     * Adds information about a partition with excessive updates.
     *
     * @param part Partition ID.
     * @param nodeId Node ID of the supplier.
     * @param size Current size of the partition.
     * @param updates Number of updates in the partition.
     */
    public void addPartitionExcessiveUpdates(int part, UUID nodeId, long size, long updates) {
        suppliers.computeIfAbsent(FullRebalanceReason.EXCESSIVE_PARTITION_UPDATES, r -> new HashMap<>())
            .computeIfAbsent(nodeId, n -> new ArrayList<>())
            .add(new T3<>(part, size, updates));
    }

    /**
     * Retrieves the map of suppliers categorized by rebalance reason, node ID, and partition details.
     *
     * @return Map of suppliers.
     */
    public Map<FullRebalanceReason, Map<UUID, List<T3<Integer, Long, Long>>>> getSuppliersInfo() {
        return suppliers;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SupplyGroupInfo.class, this);
    }
}
