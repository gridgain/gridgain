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
 * Partition reconciliation caches type.
 * Specify which types of caches are allowed for processing by reconciliation tool.
 */
public enum ReconciliationCachesType {
    /** Allows ony user defined caches to be processed. */
    USER,

    /** Allows only internal (system) caches to be processed. */
    INTERNAL,

    /** Allows all cache types for processing. */
    ALL;

    /** Enumerated values. */
    private static final ReconciliationCachesType[] VALS = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value or {@code null} if ordinal out of range.
     */
    @Nullable
    public static ReconciliationCachesType fromOrdinal(int ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }

    /**
     * @return Default reconciliation cache type.
     */
    public static ReconciliationCachesType defaultValue() {
        return USER;
    }

}
