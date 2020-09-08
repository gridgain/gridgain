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

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.processors.cache.verify.RepairMeta;

/** Result of {@code RepairResultTask}. */
public class RepairResult {
    /** Keys to repair with corresponding values and versions per nodes. */
    private Map<VersionedKey, Map<UUID, VersionedValue>> keysToRepair = new HashMap<>();

    /** Repaired keys. */
    private Map<VersionedKey, RepairMeta> repairedKeys = new HashMap<>();

    /**
     * Default constructor.
     */
    public RepairResult() {
        // No-op
    }

    /**
     * Constructor.
     *
     * @param keysToRepair Keys to repair within next recheck-repair iteration.
     * @param repairedKeys Repaired keys.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public RepairResult(
        Map<VersionedKey, Map<UUID, VersionedValue>> keysToRepair,
        Map<VersionedKey, RepairMeta> repairedKeys) {
        this.keysToRepair = keysToRepair;
        this.repairedKeys = repairedKeys;
    }

    /**
     * @return Keys to repair.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public Map<VersionedKey, Map<UUID, VersionedValue>> keysToRepair() {
        return keysToRepair;
    }

    /**
     * @return Repaired keys.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public Map<VersionedKey, RepairMeta> repairedKeys() {
        return repairedKeys;
    }
}
