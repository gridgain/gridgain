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
    private static final AtomicLongFieldUpdater<QueryMemoryTracker> RESERVED_UPD =
        AtomicLongFieldUpdater.newUpdater(QueryMemoryTracker.class, "reserved");

    /** Closed flag updater. */
    private static final AtomicReferenceFieldUpdater<QueryMemoryTracker, Boolean> CLOSED_UPD =
        AtomicReferenceFieldUpdater.newUpdater(QueryMemoryTracker.class, Boolean.class, "closed");

    /** Memory limit. */
    private final long maxMem;

    /** Parent tracker. */
    private final H2MemoryTracker parent;

    /** Memory reserved. */
    private volatile long reserved;

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

    /** {@inheritDoc} */
    @Override public void reserve(long size) {
        assert !closed && size >= 0;

        if (size == 0)
            return;

        RESERVED_UPD.accumulateAndGet(this, size, (prev, x) -> {
            if (prev + x > maxMem) {
                throw new IgniteSQLException("SQL query run out of memory: Query quota exceeded. " + x,
                    IgniteQueryErrorCode.QUERY_OUT_OF_MEMORY);
            }

            return prev + x;
        });

        //TODO: GG-18840: Let's make this reservation coarse-grained.
        //TODO: GG-18840: Let's make this reservation coarse-grained.
        if (parent != null) {
            try {
                parent.reserve(size);
            }
            catch (Throwable e) {
                RESERVED_UPD.addAndGet(this, -size);

                throw e;
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void release(long size) {
        assert size >= 0;

        if (size == 0)
            return;

        long reserved = RESERVED_UPD.accumulateAndGet(this, -size, (prev, x) -> {
            if (prev + x < 0)
                throw new IllegalStateException("Try to release more memory that were reserved: [" +
                    "reserved=" + prev + ", toRelease=" + x + ']');

            return prev + x;
        });

        assert !closed && reserved >= 0 || reserved == 0 : "Invalid reserved memory size:" + reserved;

        if (parent != null)
            parent.release(size);
    }

    /**
     * @return Memory reserved by tracker.
     */
    public long memoryReserved() {
        return RESERVED_UPD.get(this);
    }

    /**
     * @return {@code True} if closed, {@code False} otherwise.
     */
    public boolean closed() {
        return closed;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        // It is not expected to be called concurrently with reserve\release.
        // But query can be cancelled concurrently on query finish.
        if (CLOSED_UPD.compareAndSet(this, Boolean.FALSE, Boolean.TRUE))
            release(RESERVED_UPD.get(this));
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(QueryMemoryTracker.class, this);
    }
}