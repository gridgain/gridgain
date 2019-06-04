package org.apache.ignite.internal.visor.ru;

import org.jetbrains.annotations.Nullable;

/**
 * Rolling upgrade operations.
 */
public enum VisorRollingUpgradeOperation {
    /**
     * Enable rolling upgrade.
     */
    ENABLE,

    /**
     * Disable rolling upgrade.
     */
    DISABLE,

    /**
     * Remove nodes from baseline.
     */
    STATUS;

    /** Enumerated values. */
    private static final VisorRollingUpgradeOperation[] VALS = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value or {@code null} if ordinal out of range.
     */
    @Nullable public static VisorRollingUpgradeOperation fromOrdinal(int ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}
