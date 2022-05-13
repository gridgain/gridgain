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

import java.io.Serializable;
import java.util.Set;

/**
 * Partition dr partition counters metrics.
 */
class VisorDrCachePartitionMetrics implements Serializable {
    /** Serial number. */
    private static final long serialVersionUID = 0L;

    /** Size. */
    private final long size;

    /** Affected cache ids. */
    private final Set<Integer> affectedCaches;

    /** Entries processed. */
    private final long entriesProcessed;

    /** Broken entries found. */
    private final long brokenEntriesFound;

    /** Constructor. */
    public VisorDrCachePartitionMetrics(long size, Set<Integer> affectedCaches, long entriesProcessed, long brokenEntriesFound) {
        this.size = size;
        this.affectedCaches = affectedCaches;
        this.entriesProcessed = entriesProcessed;
        this.brokenEntriesFound = brokenEntriesFound;
    }

    public long getSize() {
        return size;
    }

    public Set<Integer> getAffectedCaches() {
        return affectedCaches;
    }

    public long getEntriesProcessed() {
        return entriesProcessed;
    }

    public long getBrokenEntriesFound() {
        return brokenEntriesFound;
    }
}
