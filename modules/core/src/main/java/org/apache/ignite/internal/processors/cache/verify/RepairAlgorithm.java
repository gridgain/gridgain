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

package org.apache.ignite.internal.processors.cache.verify;

import org.jetbrains.annotations.Nullable;

/**
 * Partition reconciliation repair algorithm options for fixing doubtful keys
 * (in practice doubtful usually means fully removed keys that cannot be found in deferred delete queue).
 */
public enum RepairAlgorithm {
    /** Value from entry with max grid cache version will be used.*/
    LATEST,

    /** Value from entry from primary node will be used. */
    PRIMARY,

    /** Most common value will be used. */
    MAJORITY,

    /** Does remove a key form all partitions. */
    REMOVE,

    /** Nicht repair, only printing. */
    PRINT_ONLY,

    /**
     * This algorithm behaves in the same way as {@link #LATEST} except the case when a key is missing
     * on the primary node. In that case, the conflict will not be resolved.
     */
    LATEST_SKIP_MISSING_PRIMARY,

    /**
     * This algorithm behaves in the same way as {@link #LATEST} except the case when a key is missing
     * on the primary node. In that case, the conflict will be resolved by removing the key from all partitions.
     */
    LATEST_TRUST_MISSING_PRIMARY;

    /** Enumerated values. */
    private static final RepairAlgorithm[] VALS = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value or {@code null} if ordinal out of range.
     */
    @Nullable public static RepairAlgorithm fromOrdinal(int ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }

    /**
     * Efficiently gets enumerated value from its string representation ignoring case.
     *
     * @param str String representation.
     * @return Enumerated value.
     * @throws IllegalArgumentException If string representation is not valid.
     */
    public static RepairAlgorithm fromString(String str) {
        for (RepairAlgorithm val : VALS) {
            if (val.name().equalsIgnoreCase(str))
                return val;
        }

        throw new IllegalArgumentException("Invalid repair algorithm [name=" + str + ']');
    }

    /**
     * @return Default repair algorithm.
     */
    public static RepairAlgorithm defaultValue() {
        return PRINT_ONLY;
    }
}
