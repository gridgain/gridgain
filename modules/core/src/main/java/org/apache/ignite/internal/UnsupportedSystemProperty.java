/*
 * Copyright 2026 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.util.typedef.internal.LT;

/**
 * Registry of Apache Ignite {@code IGNITE_*} system properties that are known to be unsupported in GridGain.
 * <p>
 * Each constant's name is the system property name, and the associated reason explains why the property has no
 * effect in GridGain. On node start {@link #warnForConfigured(IgniteLogger)} emits a throttled warning for every
 * listed property that is currently set as a system property or environment variable.
 */
public enum UnsupportedSystemProperty {
    // Calcite-based SQL engine (GridGain uses an H2-based engine).
    IGNITE_CALCITE_EXEC_IN_BUFFER_SIZE(Reason.CALCITE),
    IGNITE_CALCITE_EXEC_IO_BATCH_CNT(Reason.CALCITE),
    IGNITE_CALCITE_EXEC_IO_BATCH_SIZE(Reason.CALCITE),
    IGNITE_CALCITE_EXEC_MODIFY_BATCH_SIZE(Reason.CALCITE),
    IGNITE_CALCITE_REL_JSON_PRETTY_PRINT(Reason.CALCITE),

    // Snapshots.
    IGNITE_SNAPSHOT_SEQUENTIAL_WRITE(Reason.SNAPSHOT),

    // Performance statistics.
    IGNITE_PERF_STAT_BUFFER_SIZE(Reason.PERF_STAT),
    IGNITE_PERF_STAT_CACHED_STRINGS_THRESHOLD(Reason.PERF_STAT),
    IGNITE_PERF_STAT_FILE_MAX_SIZE(Reason.PERF_STAT),
    IGNITE_PERF_STAT_FLUSH_SIZE(Reason.PERF_STAT),

    // Miscellaneous.
    IGNITE_ALLOW_MIXED_CACHE_GROUPS(Reason.MIXED_CACHE_GROUPS),
    IGNITE_BPLUS_TREE_DISABLE_METRICS(Reason.BPLUS_TREE_METRICS),
    IGNITE_ENABLE_OBJECT_INPUT_FILTER_AUTOCONFIGURATION(Reason.OBJECT_INPUT_FILTER),
    IGNITE_USE_BINARY_ARRAYS(Reason.BINARY_ARRAYS),
    IGNITE_SQL_FORCE_LAZY_RESULT_SET(Reason.SQL_LAZY_RESULT_SET),

    // Thin client.
    IGNITE_THIN_CLIENT_ASYNC_REQUESTS_WAIT_TIMEOUT(Reason.THIN_CLIENT_ASYNC);

    /** Migration guide referenced by every warning. */
    private static final String MIGRATION_GUIDE_URL =
        "https://www.gridgain.com/docs/gridgain8/latest/installation-guide/migrating-from-ignite";

    /** Reason why this property is not supported in GridGain (no trailing period; the message adds one). */
    private final String reason;

    /**
     * @param reason Reason why this property is not supported in GridGain.
     */
    UnsupportedSystemProperty(String reason) {
        this.reason = reason;
    }

    /**
     * Emits a throttled warning for every unsupported property that is currently set as a system property or
     * environment variable.
     *
     * @param log Logger.
     */
    public static void warnForConfigured(IgniteLogger log) {
        for (UnsupportedSystemProperty prop : values()) {
            if (IgniteSystemProperties.getString(prop.name()) != null)
                LT.warn(log, prop.warning());
        }
    }

    /**
     * @return Warning message to log when this property is set.
     */
    String warning() {
        return "Property " + name() + " is not supported in GridGain (reason: " + reason + "). See " +
            MIGRATION_GUIDE_URL + '.';
    }

    /**
     * Reason texts explaining why each property is unsupported in GridGain.
     */
    private static final class Reason {
        /** Calcite-based SQL engine. */
        static final String CALCITE = "GridGain SQL engine is H2-based; Calcite is not available";

        /** Snapshot subsystem. */
        static final String SNAPSHOT = "GridGain snapshot subsystem differs from Apache Ignite";

        /** Performance statistics subsystem. */
        static final String PERF_STAT =
            "Performance statistics subsystem is not implemented in GridGain; use GridGain metrics";

        /** Mixed cache groups. */
        static final String MIXED_CACHE_GROUPS = "Mixed cache groups are not available in GridGain";

        /** B+ tree metrics toggling. */
        static final String BPLUS_TREE_METRICS = "B+ tree metrics toggling is not available in GridGain";

        /** Object input filter auto-configuration. */
        static final String OBJECT_INPUT_FILTER =
            "Object input filter auto-configuration is not available in GridGain";

        /** Typed binary arrays. */
        static final String BINARY_ARRAYS = "Typed binary arrays are not available in GridGain";

        /** Lazy SQL result set. */
        static final String SQL_LAZY_RESULT_SET = "Not available in GridGain's H2-based SQL engine";

        /** Thin-client async requests. */
        static final String THIN_CLIENT_ASYNC =
            "This thin-client async request setting is not available in GridGain";
    }
}
