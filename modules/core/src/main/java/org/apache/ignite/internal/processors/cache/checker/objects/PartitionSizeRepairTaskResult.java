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

import java.io.Serializable;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/** Result of {@code RepairResultTask}. */
public class PartitionSizeRepairTaskResult implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private Map<UUID, NodePartitionSize> sizeMap = new ConcurrentHashMap<>();

    /**
     * Default constructor.
     */
    public PartitionSizeRepairTaskResult() {
        // No-op
    }

//    /**
//     * Constructor.
//     *
//     * @param keysToRepair Keys to repair within next recheck-repair iteration.
//     * @param repairedKeys Repaired keys.
//     */
//    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
//    public PartitionSizeRepairJobResult(
//        Map<VersionedKey, Map<UUID, VersionedValue>> keysToRepair,
//        Map<VersionedKey, RepairMeta> repairedKeys) {
//        this.keysToRepair = keysToRepair;
//        this.repairedKeys = repairedKeys;
//    }

//    /**
//     * @return Keys to repair.
//     */
//    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
//    public Map<VersionedKey, Map<UUID, VersionedValue>> keysToRepair() {
//        return keysToRepair;
//    }
//
//    /**
//     * @return Repaired keys.
//     */
//    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
//    public Map<VersionedKey, RepairMeta> repairedKeys() {
//        return repairedKeys;
//    }

    public Map<UUID, NodePartitionSize> getSizeMap() {
        return sizeMap;
    }

    public void setSizeMap(
        Map<UUID, NodePartitionSize> sizeMap) {
        this.sizeMap = sizeMap;
    }
}
