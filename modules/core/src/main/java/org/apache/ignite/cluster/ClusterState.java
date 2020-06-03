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

package org.apache.ignite.cluster;

import org.jetbrains.annotations.Nullable;

/**
 * Cluster states.
 */
public enum ClusterState {
    /** Cluster deactivated. Cache operations aren't allowed. */
    INACTIVE,

    /** Cluster activated. All cache operations are allowed. */
    ACTIVE,

    /** Cluster activated. Cache read operation allowed, Cache data change operation aren't allowed. */
    ACTIVE_READ_ONLY;

    /** Enumerated values. */
    private static final ClusterState[] VALS = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value or {@code null} if ordinal out of range.
     */
    @Nullable public static ClusterState fromOrdinal(int ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }

    /**
     * @param state Cluster state
     * @return {@code True} if cluster in given cluster {@code state} is activated and {@code False} otherwise.
     */
    public static boolean active(ClusterState state) {
        return state != INACTIVE;
    }
}
