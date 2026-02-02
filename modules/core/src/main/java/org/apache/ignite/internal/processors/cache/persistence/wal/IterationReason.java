/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.internal.processors.cache.persistence.wal;

/** Reason for WAL iteration. */
public enum IterationReason {
    /** Applying lost metastore / cache updates since last checkpoint record. */
    LOST_UPDATES(true, "apply lost updates"),

    /** Restore memory state if node stopped in the middle of checkpoint. */
    RESTORE_BINARY_STATE(true, "restore binary state");

    private final boolean logToInfo;

    private final String name;

    /** */
    IterationReason(boolean logToInfo, String name) {
        this.logToInfo = logToInfo;
        this.name = name;
    }

    /** If true, info-level logging should be used when reporting iteration progress. */
    public boolean shouldLogToInfo() {
        return logToInfo;
    }

    @Override public String toString() {
        return name;
    }
}
