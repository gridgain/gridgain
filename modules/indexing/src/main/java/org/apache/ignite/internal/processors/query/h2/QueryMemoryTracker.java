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

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.GridQueryMemoryTracker;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Query memory tracker.
 *
 * Track query memory usage and throws an exception if query tries to allocate memory over limit.
 */
public class QueryMemoryTracker implements H2MemoryTracker, GridQueryMemoryTracker {
    /** Logger. */
    private final IgniteLogger log;

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
    private volatile long reserved;

    /** Maximum number of bytes reserved by query. */
    private volatile long maxReserved;

    /** Number of bytes written on disk at the current moment. */
    private volatile long writtenOnDisk;

    /** Maximum number of bytes written on disk at the same time. */
    private volatile long maxWrittenOnDisk;

    /** Total number of bytes written on disk tracked by current tracker. */
    private volatile long totalWrittenOnDisk;

    /** Close flag to prevent tracker reuse. */
    private Boolean closed = Boolean.FALSE;

    /** The number of files created by the query. */
    private volatile int filesCreated;

    /** Query descriptor (for logging). */
    private final String qryDesc;

    /**
     * Constructor.
     *
     * @param log Logger.
     * @param parent Parent memory tracker.
     * @param quota Query memory limit in bytes.
     * @param blockSize Reservation block size.
     * @param offloadingEnabled Flag whether to fail when memory limit is exceeded.
     * @param qryDesc Descriptor of the racked query.
     */
    public QueryMemoryTracker(
        IgniteLogger log,
        H2MemoryTracker parent,
        long quota,
        long blockSize,
        boolean offloadingEnabled,
        String qryDesc
    ) {
        assert quota >= 0;

        this.log = log;
        this.offloadingEnabled = offloadingEnabled;
        this.parent = parent;
        this.quota = quota;
        this.blockSize = quota != 0 ? Math.min(quota, blockSize) : blockSize;
        this.qryDesc = qryDesc;
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean reserve(long size) {
        assert size >= 0;

        checkClosed();

        reserved += size;
        maxReserved = Math.max(maxReserved, reserved);

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

        if (parent.reserve(blockSize))
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
    @Override public synchronized void release(long size) {
        assert size >= 0;

        if (size == 0)
            return;

        checkClosed();

        reserved -= size;

        assert reserved >= 0 : "Try to free more memory that ever be reserved: [reserved=" + (reserved + size) +
            ", toFree=" + size + ']';

        if (parent != null && reservedFromParent - reserved > blockSize)
            releaseFromParent();
    }

    /**
     * Releases memory from parent.
     */
    private void releaseFromParent() {
        long toReleaseFromParent = reservedFromParent - reserved;

        parent.release(toReleaseFromParent);

        reservedFromParent -= toReleaseFromParent;

        assert reservedFromParent >= 0 : reservedFromParent;
    }

    /** {@inheritDoc} */
    @Override public synchronized long reserved() {
        return reserved;
    }

    /** {@inheritDoc} */
    @Override public synchronized long maxReserved() {
        return maxReserved;
    }

    /** {@inheritDoc} */
    @Override public synchronized long writtenOnDisk() {
        return writtenOnDisk;
    }

    /** {@inheritDoc} */
    @Override public synchronized long maxWrittenOnDisk() {
        return maxWrittenOnDisk;
    }

    /** {@inheritDoc} */
    @Override public synchronized long totalWrittenOnDisk() {
        return totalWrittenOnDisk;
    }

    /**
     * @return Offloading enabled flag.
     */
    public boolean isOffloadingEnabled() {
        return offloadingEnabled;
    }

    /** {@inheritDoc} */
    @Override public synchronized void swap(long size) {
        writtenOnDisk += size;
        totalWrittenOnDisk += size;
        maxWrittenOnDisk = Math.max(maxWrittenOnDisk, writtenOnDisk);
    }

    /** {@inheritDoc} */
    @Override public synchronized void unswap(long size) {
        writtenOnDisk -= size;
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
            parent.release(reservedFromParent);

        if (log.isDebugEnabled()) {
            log.debug("Query has been completed with memory metrics: [bytesAllocatedMax="  + maxReserved +
                ", bytesOffloaded=" + totalWrittenOnDisk + ", filesCreated=" + filesCreated +
                ", query=" + qryDesc + ']');
        }
    }

    /** {@inheritDoc} */
    @Override public synchronized void incrementFilesCreated() {
        filesCreated++;
    }

    /**
     * @return Query descriptor.
     */
    public String queryDescriptor() {
        return qryDesc;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(QueryMemoryTracker.class, this);
    }
}