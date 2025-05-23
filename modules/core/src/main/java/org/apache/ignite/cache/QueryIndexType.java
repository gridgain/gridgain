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

package org.apache.ignite.cache;

import org.jetbrains.annotations.Nullable;

/**
 * Index type.
 */
public enum QueryIndexType {
    /**
     * Sorted index.
     */
    SORTED,

    /**
     * Full-text index.
     */
    FULLTEXT,

    /**
     * Geo-spatial index.
     */
    GEOSPATIAL,

    /**
     * Vector index.
     */
    VECTOR;

    /** Enum values. */
    private static final QueryIndexType[] VALS = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value.
     */
    @Nullable public static QueryIndexType fromOrdinal(int ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}
