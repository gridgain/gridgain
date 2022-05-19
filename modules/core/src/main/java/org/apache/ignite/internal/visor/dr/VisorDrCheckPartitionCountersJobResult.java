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
 * Validate cache entry job result.
 */
public class VisorDrCheckPartitionCountersJobResult implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Cache or group name. */
    protected final String cacheOrGroupName;

    /** Cache size. */
    protected final long size;

    /** Affected cache ids. */
    protected final Set<Integer> affectedCaches;

    /** Affected partitions. */
    protected final Set<Integer> affectedPartitions;

    /** Number of entries processed. */
    protected final long entriesProcessed;

    /** Number of broken entries. */
    protected final long brokenEntriesFound;

    /**
     * Constructor.
     *
     * @param cacheOrGroupName cache.
     * @param size Entries size.
     * @param affectedCaches Affected cache number.
     * @param affectedPartitions Affected partitions.
     * @param entriesProcessed Entries processed size.
     * @param brokenEntriesFound Broken entries size.
     */
    public VisorDrCheckPartitionCountersJobResult(String cacheOrGroupName, long size,
            Set<Integer> affectedCaches, Set<Integer> affectedPartitions, long entriesProcessed,
            long brokenEntriesFound) {
        assert affectedCaches != null;
        assert affectedPartitions != null;

        this.cacheOrGroupName = cacheOrGroupName;
        this.size = size;
        this.affectedCaches = affectedCaches;
        this.affectedPartitions = affectedPartitions;
        this.entriesProcessed = entriesProcessed;
        this.brokenEntriesFound = brokenEntriesFound;
    }

    /**
     * @return HasIssues flag.
     */
    public boolean hasIssues() {
        return brokenEntriesFound != 0;
    }

    /**
     * @return Cache or group name.
     */
    public String getCacheOrGroupName() {
        return cacheOrGroupName;
    }

    /**
     * @return Cache or group size.
     */
    public long getSize() {
        return size;
    }

    /**
     * @return Affected cache ids.
     */
    public Set<Integer> getAffectedCaches() {
        return affectedCaches;
    }

    /**
     * @return Affected partitions.
     */
    public Set<Integer> getAffectedPartitions() {
        return affectedPartitions;
    }

    /**
     * @return Entries processed.
     */
    public long getEntriesProcessed() {
        return entriesProcessed;
    }

    /**
     * @return Broken entries found.
     */
    public long getBrokenEntriesFound() {
        return brokenEntriesFound;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "AggregatedCacheMetrics{" +
                "cacheOrGroupName='" + cacheOrGroupName + '\'' +
                ", size=" + size +
                ", affectedCaches=" + affectedCaches +
                ", affectedPartitions=" + affectedPartitions +
                ", entriesProcessed=" + entriesProcessed +
                ", brokenEntriesFound=" + brokenEntriesFound +
                '}';
    }
}
