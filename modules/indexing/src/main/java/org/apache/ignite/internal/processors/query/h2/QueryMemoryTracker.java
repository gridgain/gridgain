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
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Query memory tracker.
 *
 * Track query memory usage and throws an exception if query tries to allocate memory over limit.
 */
public class QueryMemoryTracker extends H2MemoryTracker implements AutoCloseable {

    /** Allocated field updater. */
    private static final AtomicLongFieldUpdater<QueryMemoryTracker> ALLOC_UPD =
        AtomicLongFieldUpdater.newUpdater(QueryMemoryTracker.class, "allocated");

    /** Closed flag updater. */
    private static final AtomicReferenceFieldUpdater<QueryMemoryTracker, Boolean> CLOSED_UPD =
        AtomicReferenceFieldUpdater.newUpdater(QueryMemoryTracker.class, Boolean.class, "closed");

    /** Memory limit. */
    private final long maxMem;

    /** Parent tracker. */
    private final H2MemoryTracker parent;

    /** Memory allocated. */
    private volatile long allocated;

    /** Close flag to prevent tracker reuse. */
    private volatile Boolean closed = Boolean.FALSE;

    /**
     * Constructor.
     *
     * @param parent Parent memory tracker.
     * @param maxMem Query memory limit in bytes.
     */
    QueryMemoryTracker(H2MemoryTracker parent, long maxMem) {
        assert maxMem > 0;

        this.parent = parent;
        this.maxMem = maxMem;
    }

    /**
     * Check allocated size is less than query memory pool threshold.
     *
     * @param size Allocated size in bytes.
     * @throws IgniteSQLException if memory limit has been exceeded.
     */
    @Override public void allocate(long size) {
        assert !closed && size >= 0;

        if (size == 0)
            return;

        //TODO: GG-18628: tries to allocate memory from parent first. Let's make this allocation coarse-grained.
        parent.allocate(size);

        if (ALLOC_UPD.addAndGet(this, size) >= maxMem)
            throw new IgniteSQLException("SQL query run out of memory.", IgniteQueryErrorCode.QUERY_OUT_OF_MEMORY);
    }

    /** {@inheritDoc} */
    @Override public void release(long size) {
        assert size >= 0;

        if (size == 0)
            return;

        parent.release(size);

        long allocated = ALLOC_UPD.addAndGet(this, -size);

        assert !closed && allocated >= 0 || allocated == 0 : "Invalid allocated memory size:" + allocated;
    }

    /**
     * @return Memory allocated by tracker.
     */
    public long getAllocated() {
        return allocated;
    }

    /**
     * @return {@code True} if closed, {@code False} otherwise.
     */
    public boolean closed() {
        return closed;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        // It is not expected to be called concurrently with allocate\free.
        if (CLOSED_UPD.compareAndSet(this, Boolean.FALSE, Boolean.TRUE))
            release(allocated);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(QueryMemoryTracker.class, this);
    }
}