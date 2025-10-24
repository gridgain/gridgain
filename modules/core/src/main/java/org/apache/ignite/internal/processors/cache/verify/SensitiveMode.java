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

import org.apache.ignite.IgniteSystemProperties;
import org.jetbrains.annotations.Nullable;

/**
 * This enum defines possible modes of printing sensitive information provided by the partition reconciliation tool.
 */
public enum SensitiveMode {
    /**
     * This mode provides the same level as defined in the cluster.
     * @see IgniteSystemProperties#IGNITE_SENSITIVE_DATA_LOGGING
     **/
    DEFAULT,

    /**
     * This mode provides a hashed representation of objects.
     * @see IgniteSystemProperties#IGNITE_SENSITIVE_DATA_LOGGING
     **/
    HASH,

    /**
     * This mode provides a plain representation of objects.
     * @see IgniteSystemProperties#IGNITE_SENSITIVE_DATA_LOGGING
     **/
    PLAIN;

    /** Enumerated values. */
    private static final SensitiveMode[] VALS = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value or {@code null} if ordinal out of range.
     */
    @Nullable public static SensitiveMode fromOrdinal(int ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }

    /**
     * Efficiently gets enumerated value from its string representation ignoring case.
     *
     * @param str String representation.
     * @return Enumerated value.
     * @throws IllegalArgumentException If string representation is not valid.
     */
    public static SensitiveMode fromString(String str) {
        for (SensitiveMode val : VALS) {
            if (val.name().equalsIgnoreCase(str))
                return val;
        }

        throw new IllegalArgumentException("Invalid sensitive mode [name=" + str + ']');
    }

    /**
     * @return Default repair algorithm.
     */
    public static SensitiveMode defaultValue() {
        return DEFAULT;
    }
}
