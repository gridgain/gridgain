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

import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Query memory tracker.
 *
 * Track query memory usage and throws an exception if query tries to allocate memory over limit.
 */
public class QueryMemoryTracker implements H2MemoryTracker {
    /** Parent tracker. */
    private final H2MemoryTracker parent;

    /** Query memory limit. */
    private final long quota;

    /**
     * Defines an action that occurs when the memory limit is exceeded. Possible variants:
     * <ul>
     * <li>{@code false} - exception will be thrown.</li>
     * <li>{@code true} - intermediate query results will be spilled to the disk.</li>
     * </ul>
     */
    private final boolean offloadingEnabled;

    /** Reservation block size. */
    private final long blockSize;

    /** Memory reserved on parent. */
    private long reservedFromParent;

    /** Memory reserved by query. */
    private long reserved;

    /** Close flag to prevent tracker reuse. */
    private Boolean closed = Boolean.FALSE;

    /**
     * Constructor.
     *
     * @param parent Parent memory tracker.
     * @param quota Query memory limit in bytes.
     * @param blockSize Reservation block size.
     * @param offloadingEnabled Flag whether to fail when memory limit is exceeded.
     */
    QueryMemoryTracker(H2MemoryTracker parent, long quota, long blockSize, boolean offloadingEnabled) {
        assert quota >= 0;

        this.offloadingEnabled = offloadingEnabled;
        this.parent = parent;
        this.quota = quota;
        this.blockSize = quota != 0 ? Math.min(quota, blockSize) : blockSize;
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean reserved(long toReserve) {
        assert toReserve >= 0;

        checkClosed();

        reserved += toReserve;

        if (parent != null && reserved > reservedFromParent) {
            if (!reserveFromParent())
                return false; // Offloading.
        }

        if (quota > 0 && reserved >= quota)
            return onQuotaExceeded();

        return true;
    }

    /**
     * Checks whether tracker was closed.
     */
    private void checkClosed() {
        if (closed)
            throw new IllegalStateException("Memory tracker has been closed concurrently.");
    }

    /**
     * Reserves memory from parent tracker.
     * @return {@code false} if offloading is needed.
     */
    private boolean reserveFromParent() {
        // If single block size is too small.
        long blockSize = Math.max(reserved - reservedFromParent, this.blockSize);

        // If we are too close to limit.
        if (quota > 0)
            blockSize = Math.min(blockSize, quota - reservedFromParent);

        if (parent.reserved(blockSize))
            reservedFromParent += blockSize;
        else
            return false;

        return true;
    }

    /**
     * Action on quota exceeded.
     * @return {@code false} if offloading is needed.
     */
    private boolean onQuotaExceeded() {
        if (offloadingEnabled)
            return false;
        else
            throw new IgniteSQLException("SQL query run out of memory: Query quota exceeded.",
                IgniteQueryErrorCode.QUERY_OUT_OF_MEMORY);
    }

    /** {@inheritDoc} */
    @Override public synchronized void released(long toRelease) {
        assert toRelease >= 0;

        if (toRelease == 0)
            return;

        checkClosed();

        reserved -= toRelease;

        assert reserved >= 0 : "Try to free more memory that ever be reserved: [reserved=" + (reserved + toRelease) +
            ", toFree=" + toRelease + ']';

        if (parent != null && reservedFromParent - reserved > blockSize)
            releaseFromParent();
    }

    /**
     * Releases memory from parent.
     */
    private void releaseFromParent() {
        long toReleaseFromParent = reservedFromParent - reserved;

        parent.released(toReleaseFromParent);

        reservedFromParent -= toReleaseFromParent;

        assert reservedFromParent >= 0 : reservedFromParent;
    }

    /** {@inheritDoc} */
    @Override public synchronized long memoryReserved() {
        return reserved;
    }

    /** {@inheritDoc} */
    @Override public long memoryLimit() {
        return quota;
    }

    /**
     * @return {@code True} if closed, {@code False} otherwise.
     */
    public synchronized boolean closed() {
        return closed;
    }

    /** {@inheritDoc} */
    @Override public synchronized void close() {
        // It is not expected to be called concurrently with reserve\release.
        // But query can be cancelled concurrently on query finish.
        if (closed)
            return;

        closed = true;

        reserved = 0;

        if (parent != null)
            parent.released(reservedFromParent);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(QueryMemoryTracker.class, this);
    }
}