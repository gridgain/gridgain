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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import org.apache.ignite.cache.query.exceptions.SqlMemoryQuotaExceededException;
import org.apache.ignite.internal.processors.query.GridQueryMemoryMetricProvider;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Query memory tracker.
 *
 * Track query memory usage and throws an exception if query tries to allocate memory over limit.
 */
public class QueryMemoryTracker implements H2MemoryTracker, GridQueryMemoryMetricProvider {
    /** State updater. */
    private static final AtomicIntegerFieldUpdater<QueryMemoryTracker> STATE_UPDATER
        = AtomicIntegerFieldUpdater.newUpdater(QueryMemoryTracker.class, "state");

    /** Tracker is not closed and not in the middle of the closing process. */
    private static final int STATE_INITIAL = 0;

    /** Tracker is closed or in the middle of the closing process. */
    private static final int STATE_CLOSED = 1;

    /** Parent tracker. */
    @GridToStringExclude
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
    private volatile boolean closed;

    /** State of the tracker. Can be equal {@link #STATE_INITIAL} or {@link #STATE_CLOSED}*/
    private volatile int state;

    /** Children. */
    private final List<H2MemoryTracker> children = new ArrayList<>();

    /** The number of files created by the query. */
    private volatile int filesCreated;

    /**
     * Constructor.
     *
     * @param parent Parent memory tracker.
     * @param quota Query memory limit in bytes.
     * @param blockSize Reservation block size.
     * @param offloadingEnabled Flag whether to fail when memory limit is exceeded.
     */
    public QueryMemoryTracker(
        H2MemoryTracker parent,
        long quota,
        long blockSize,
        boolean offloadingEnabled
    ) {
        assert quota >= 0;

        this.offloadingEnabled = offloadingEnabled;
        this.parent = parent;
        this.quota = quota;
        this.blockSize = quota != 0 ? Math.min(quota, blockSize) : blockSize;
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
            throw new TrackerWasClosedException("Memory tracker has been closed concurrently.");
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
            throw new SqlMemoryQuotaExceededException("SQL query ran out of memory: Query quota was exceeded.");
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
    @Override public long reserved() {
        return reserved;
    }

    /** {@inheritDoc} */
    @Override public long maxReserved() {
        return maxReserved;
    }

    /** {@inheritDoc} */
    @Override public long writtenOnDisk() {
        return writtenOnDisk;
    }

    /** {@inheritDoc} */
    @Override public long maxWrittenOnDisk() {
        return maxWrittenOnDisk;
    }

    /** {@inheritDoc} */
    @Override public long totalWrittenOnDisk() {
        return totalWrittenOnDisk;
    }

    /**
     * @return Offloading enabled flag.
     */
    public boolean isOffloadingEnabled() {
        return offloadingEnabled;
    }

    /** {@inheritDoc} */
    @Override public synchronized void spill(long size) {
        assert size >= 0;

        if (size == 0)
            return;

        checkClosed();

        if (parent != null)
            parent.spill(size);

        writtenOnDisk += size;
        totalWrittenOnDisk += size;
        maxWrittenOnDisk = Math.max(maxWrittenOnDisk, writtenOnDisk);
    }

    /** {@inheritDoc} */
    @Override public synchronized void unspill(long size) {
        assert size >= 0;

        if (size == 0)
            return;

        checkClosed();

        if (parent != null)
            parent.unspill(size);

        writtenOnDisk -= size;
    }

    /**
     * @return {@code true} if closed, {@code false} otherwise.
     */
    @Override public boolean closed() {
        return closed;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        // It is not expected to be called concurrently with reserve\release.
        // But query can be cancelled concurrently on query finish.
        if (!STATE_UPDATER.compareAndSet(this, STATE_INITIAL, STATE_CLOSED))
            return;

        synchronized (this) {
            for (H2MemoryTracker child : children)
                child.close();

            children.clear();
        }

        closed = true;

        reserved = 0;

        if (parent != null)
            parent.release(reservedFromParent);
    }

    /** {@inheritDoc} */
    @Override public synchronized void incrementFilesCreated() {
        if (parent != null)
            parent.incrementFilesCreated();

        filesCreated++;
    }

    /** {@inheritDoc} */
    @Override public synchronized H2MemoryTracker createChildTracker() {
        checkClosed();

        H2MemoryTracker child = new ChildMemoryTracker(this);

        children.add(child);

        return child;
    }

    /** {@inheritDoc} */
    @Override public synchronized void onChildClosed(H2MemoryTracker child) {
        if (state != STATE_CLOSED)
            children.remove(child);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(QueryMemoryTracker.class, this);
    }

    /** */
    private static class ChildMemoryTracker implements H2MemoryTracker {
        /** State updater. */
        private static final AtomicIntegerFieldUpdater<ChildMemoryTracker> STATE_UPDATER
            = AtomicIntegerFieldUpdater.newUpdater(ChildMemoryTracker.class, "state");

        /** */
        private final H2MemoryTracker parent;

        /** */
        private long reserved;

        /** */
        private long writtenOnDisk;

        /** */
        private long totalWrittenOnDisk;

        /** */
        private volatile int state;

        /**
         * @param parent Parent.
         */
        public ChildMemoryTracker(H2MemoryTracker parent) {
            this.parent = parent;
        }

        /** {@inheritDoc} */
        @Override public boolean reserve(long size) {
            checkClosed();

            boolean res;
            try {
                res = parent.reserve(size);
            }
            finally {
                reserved += size;
            }

            return res;
        }

        /** {@inheritDoc} */
        @Override public void release(long size) {
            checkClosed();

            reserved -= size;

            parent.release(size);
        }

        /** {@inheritDoc} */
        @Override public long writtenOnDisk() {
            return writtenOnDisk;
        }

        /** {@inheritDoc} */
        @Override public long totalWrittenOnDisk() {
            return totalWrittenOnDisk;
        }

        /** {@inheritDoc} */
        @Override public long reserved() {
            return reserved;
        }

        /** {@inheritDoc} */
        @Override public void spill(long size) {
            checkClosed();

            parent.spill(size);

            writtenOnDisk += size;
            totalWrittenOnDisk += size;
        }

        /** {@inheritDoc} */
        @Override public void unspill(long size) {
            checkClosed();

            parent.unspill(size);

            writtenOnDisk -= size;
        }

        /** {@inheritDoc} */
        @Override public void incrementFilesCreated() {
            checkClosed();

            parent.incrementFilesCreated();
        }

        /** {@inheritDoc} */
        @Override public H2MemoryTracker createChildTracker() {
            checkClosed();

            return parent.createChildTracker();
        }

        /** {@inheritDoc} */
        @Override public void onChildClosed(H2MemoryTracker child) {
            parent.onChildClosed(child);
        }

        /** {@inheritDoc} */
        @Override public boolean closed() {
            return state == STATE_CLOSED;
        }

        /** {@inheritDoc} */
        @Override public void close() {
            if (!STATE_UPDATER.compareAndSet(this, STATE_INITIAL, STATE_CLOSED))
                return;

            parent.release(reserved);
            parent.unspill(writtenOnDisk);

            reserved = 0;
            writtenOnDisk = 0;

            parent.onChildClosed(this);
        }

        /** */
        private void checkClosed() {
            if (state == STATE_CLOSED)
                throw new TrackerWasClosedException("Memory tracker has been closed concurrently.");
        }
    }

    /** Exception thrown when try to track memory with closed tracker. */
    public static class TrackerWasClosedException extends RuntimeException {
        /**
         * @param msg Message.
         */
        public TrackerWasClosedException(String msg) {
            super(msg);
        }
    }
}
