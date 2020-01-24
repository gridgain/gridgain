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

package org.apache.ignite.internal.visor.cache;

import org.apache.ignite.IgniteException;
import org.jetbrains.annotations.Nullable;

/**
 * Enumeration of all supported cache key types in {@link VisorCacheGetValueTask}.
 */
public enum VisorObjectType {
    /**  */
    STRING,

    /**  */
    CHARACTER,

    /**  */
    INT,

    /**  */
    LONG,

    /**  */
    SHORT,

    /**  */
    BYTE,

    /**  */
    FLOAT,

    /**  */
    DOUBLE,

    /**  */
    BOOLEAN,

    /**  */
    DATE,

    /**  */
    TIMESTAMP,

    /**  */
    UUID,

    /**  */
    BINARY;

    /** Enumerated values. */
    private static final VisorObjectType[] VALS = values();

    /**
     * Get enum value in accondance to specified string or generate exception when enum value is not found.
     *
     * @param value String value.
     * @return Enum value.
     */
    public static VisorObjectType parse(String value) {
        try {
            return valueOf(value);
        }
        catch (IllegalArgumentException iae) {
            throw new IgniteException("Unsupported object type: " + value);
        }
    }

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value or {@code null} if ordinal out of range.
     */
    @Nullable public static VisorObjectType fromOrdinal(int ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}
