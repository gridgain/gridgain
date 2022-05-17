/*
 * Copyright 2022 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.visor.dr;

import java.util.Set;

/**
 * Visor repair partition counters job result with metrics.
 */
public class VisorDrRepairPartitionCountersJobResult extends VisorDrCheckPartitionCountersJobResult {
    /** */
    private static final long serialVersionUID = 0L;

    /** Tombstones cleared. */
    private final long tombstonesCleared;

    /** Tombstones failed to clear. */
    private final long tombstonesFailedToClear;

    /** Entries fixed. */
    private final long entriesFixed;

    /** Entries failed to fix. */
    private final long entriesFailedToFix;

    /**
     * Constructor.
     *
     * @param cacheOrGroupName Cache or group name.
     * @param size Cache or group name size.
     * @param affectedCaches Affected cache ids.
     * @param affectedPartitions Affected partitions.
     * @param entriesProcessed Count of entries processed.
     * @param brokenEntriesFound Count of broken entries.
     * @param tombstonesCleared Count of tombstones cleared.
     * @param tombstonesFailedToClear Count of tombstones failed to clear.
     * @param entriesFixed Count of entries fixed.
     * @param entriesFailedToFix Count of entries failed to fix.
     */
    public VisorDrRepairPartitionCountersJobResult(String cacheOrGroupName, long size,
            Set<Integer> affectedCaches, Set<Integer> affectedPartitions, long entriesProcessed,
            long brokenEntriesFound, long tombstonesCleared, long tombstonesFailedToClear,
            long entriesFixed, long entriesFailedToFix) {
        super(cacheOrGroupName, size, affectedCaches, affectedPartitions, entriesProcessed,
                brokenEntriesFound);
        this.tombstonesCleared = tombstonesCleared;
        this.tombstonesFailedToClear = tombstonesFailedToClear;
        this.entriesFixed = entriesFixed;
        this.entriesFailedToFix = entriesFailedToFix;
    }

    /**
     * @return Tombstones cleared.
     */
    public long getTombstonesCleared() {
        return tombstonesCleared;
    }

    /**
     * @return Tombstones failed to clear.
     */
    public long getTombstonesFailedToClear() {
        return tombstonesFailedToClear;
    }

    /**
     * @return Entries fixed.
     */
    public long getEntriesFixed() {
        return entriesFixed;
    }

    /**
     * @return Entries failed to fix.
     */
    public long getEntriesFailedToFix() {
        return entriesFailedToFix;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "VisorDrRepairPartitionCountersJobResult{" +
                "cacheOrGroupName='" + cacheOrGroupName + '\'' +
                ", size=" + size +
                ", affectedCaches=" + affectedCaches +
                ", affectedPartitions=" + affectedPartitions +
                ", entriesProcessed=" + entriesProcessed +
                ", brokenEntriesFound=" + brokenEntriesFound +
                ", tombstonesCleared=" + tombstonesCleared +
                ", tombstonesFailedToClear=" + tombstonesFailedToClear +
                ", entriesFixed=" + entriesFixed +
                ", entriesFailedToFix=" + entriesFailedToFix +
                '}';
    }
}