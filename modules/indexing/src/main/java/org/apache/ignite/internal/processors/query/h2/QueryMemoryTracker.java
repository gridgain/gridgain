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

import org.apache.ignite.internal.mem.IgniteOutOfMemoryException;

/**
 * Query memory tracker.
 */
public class QueryMemoryTracker {
    /** Mem query pool size. */
    private long workMem;

    /** Memory used. */
    private long allocated;

    /**
     * Constructor.
     * @param workMem Query work memory.
     */
    public QueryMemoryTracker(long workMem) {
       this.workMem = workMem;
    }

    /**
     * Check allocated size is less than query memory pool threshold.
     * @param size Allocated size.
     */
    public void allocate(long size) {
        allocated += size;

        if (allocated >= workMem)
            throw new IgniteOutOfMemoryException("SQL query out of memory");
    }

    /**
     * Free allocated memory.
     * @param size Free size.
     */
    public void free(long size) {
        assert allocated >= size: "Invalid free memory size [allocated=" + allocated + ", free=" + size + ']';

        allocated -= size;
    }
}