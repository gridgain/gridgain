package org.apache.ignite.internal.processors.query.stat;

import org.jetbrains.annotations.Nullable;

/**
 * Types of statistics width.
 */
public enum StatsType {
    /** Statistics by some particular partition. */
    PARTITION,

    /** Statistics by some data node. */
    LOCAL,

    /** Statistics by whole object (table or index). */
    GLOBAL;

    /** Enumerated values. */
    private static final StatsType[] VALUES = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value or {@code null} if ordinal out of range.
     */
    @Nullable
    public static StatsType fromOrdinal(int ord) {
        return ord >= 0 && ord < VALUES.length ? VALUES[ord] : null;
    }
}
