/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.internal.processors.cache.persistence.wal;

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

    /**
     * If true, info-level logging should be used when reporting iteration progress.
     */
    public boolean shouldLogToInfo() {
        return logToInfo;
    }

    @Override
    public String toString() {
        return name;
    }
}
