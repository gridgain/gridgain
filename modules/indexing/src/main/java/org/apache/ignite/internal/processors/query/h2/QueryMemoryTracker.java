/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.query.h2;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import org.apache.ignite.internal.mem.IgniteOutOfMemoryException;

/**
 * Query memory tracker.
 *
 * Track query memory usage and throws an exception if query tries to allocate memory over limit.
 */
public class QueryMemoryTracker {
    //TODO: GG-18629: Move defaults to memory quotas configuration.
    /** Default query memory limit. */
    public static final long DFLT_QRY_MEMORY_LIMIT = 100L * 1024 * 1024;

    /** Atomic field updater. */
    private static final AtomicLongFieldUpdater<QueryMemoryTracker> ALLOC_UPD = AtomicLongFieldUpdater.newUpdater(QueryMemoryTracker.class, "allocated");

    /** Memory limit. */
    private final long maxMem;

    /** Memory allocated. */
    private volatile long allocated;

    /**
     * Constructor.
     *
     * @param maxMem Query memory limit in bytes. If zero value, then {@link QueryMemoryTracker#DFLT_QRY_MEMORY_LIMIT} will be used.
     */
    public QueryMemoryTracker(long maxMem) {
        assert maxMem >= 0 && maxMem != Long.MAX_VALUE;

        if (maxMem > 0)
            this.maxMem = maxMem;
        else
            this.maxMem = DFLT_QRY_MEMORY_LIMIT;
    }

    /**
     * Check allocated size is less than query memory pool threshold.
     *
     * @param size Allocated size in bytes.
     * @throws IgniteOutOfMemoryException if memory limit has been exceeded.
     */
    public void allocate(long size) {
        if (ALLOC_UPD.addAndGet(this, size) >= maxMem)
            throw new IgniteOutOfMemoryException("SQL query out of memory");
    }

    /**
     * Free allocated memory.
     *
     * @param size Free size in bytes.
     */
    public void free(long size) {
        long allocated = ALLOC_UPD.getAndAdd(this, -size);

        assert allocated >= size : "Invalid free memory size [allocated=" + allocated + ", free=" + size + ']';
    }

    /**
     * @return Memory allocated by tracker.
     */
    public long getAllocated() {
        return allocated;
    }
}