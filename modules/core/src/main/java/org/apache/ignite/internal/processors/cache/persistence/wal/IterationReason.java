/*
 * Copyright 2021 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.cache.persistence.wal;

import java.util.function.Supplier;
import org.apache.ignite.IgniteLogger;

/**
 * Reason for WAL iteration.
 */
public enum IterationReason {
    UNSPECIFIED(false, "unspecified"),

    /**
     * Applying logical metastore / cache updates since last checkpoint record.
     */
    LOGICAL_UPDATES(true, "apply logical updates"),

    /**
     * Restore memory state if node stopped in the middle of checkpoint.
     */
    RESTORE_BINARY_STATE(true, "restore binary state"),

    PITR(false, "PITR"),

    APPLY_CONSISTENT_CUT(false, "apply consistent cut"),

    HANDLE_CONSISTENT_CUT(false, "handle consistent cut"),

    PRINT(false, "print WAL records"),

    READ_VALUE(false, "read value from WAL"),

    UPDATE_COUNTERS(false, "collect update counters"),

    HISTORICAL(false, "historical WAL iterator"),

    GROUP_STATE_STORE_INIT(false, "group state store init"),

    RESTORE_PAGE(false, "restore page");

    private final boolean logToInfo;

    private final String name;

    /**
     *
     */
    IterationReason(boolean logToInfo, String name) {
        this.logToInfo = logToInfo;
        this.name = name;
    }

    @Override public String toString() {
        return name;
    }

    /** Logs message provided by supplier at appropriate level, if needed. */
    public void logIfNeeded(IgniteLogger log, Supplier<String> message) {
        if (logToInfo && log.isInfoEnabled()) {
            log.info(message.get());
        } else if (log.isDebugEnabled()) {
            log.debug(message.get());
        }
    }
}
