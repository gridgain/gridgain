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

import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.GridQueryMemoryMetricProvider;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Query memory tracker.
 *
 * Track query memory usage and throws an exception if query tries to allocate memory over limit.
 */
public class QueryMemoryTracker implements H2MemoryTracker, GridQueryMemoryMetricProvider {
    /** Tracker was closed exception. */
    private static final String TRACKER_WAS_CLOSED_MESSAGE = "Memory tracker has been closed concurrently.";

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
    private final AtomicLong reservedFromParent = new AtomicLong();

    /** Memory reserved by query. */
    private final AtomicLong reserved = new AtomicLong();

    /** Maximum number of bytes reserved by query. */
    private final AtomicLong maxReserved = new AtomicLong();

    /** Number of bytes written on disk at the current moment. */
    private final AtomicLong writtenOnDisk = new AtomicLong();

    /** Maximum number of bytes written on disk at the same time. */
    private final AtomicLong maxWrittenOnDisk = new AtomicLong();

    /** Total number of bytes written on disk tracked by current tracker. */
    private final AtomicLong totalWrittenOnDisk = new AtomicLong();

    /** Close flag to prevent tracker reuse. */
    private volatile boolean closed;

    /** State of the tracker. Can be equal {@link #STATE_INITIAL} or {@link #STATE_CLOSED}*/
    private volatile int state;

    /** Children. */
    private final Queue<H2MemoryTracker> children = new LinkedBlockingQueue<>();

    /** The number of files created by the query. */
    private final AtomicInteger filesCreated = new AtomicInteger();

    /** Lock object. */
    private final Object lock = new Object();

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
    @Override public boolean reserve(long size) {
        assert size >= 0;

        if (closed)
            throw new TrackerWasClosedException(TRACKER_WAS_CLOSED_MESSAGE);

        final long reserved0 = reserved.addAndGet(size);

        if (size == 0)
            return quota == 0 || reserved0 < quota;

        if (quota > 0 && reserved0 >= quota) {
            onQuotaExceeded();

            if (reserved0 > reservedFromParent.get() && parent != null)
                reserveFromParent(reserved0);

            return false;
        }

        if (reserved0 > reservedFromParent.get() && parent != null)
            return reserveFromParent(reserved0);

        return true;
    }

    /**
     * Reserves memory from parent tracker.
     *
     * @param currentlyReserved Amount of memory currently reserved.
     * @return {@code false} if offloading is needed.
     */
    private boolean reserveFromParent(long currentlyReserved) {
        synchronized (lock) {
            if (closed)
                throw new TrackerWasClosedException(TRACKER_WAS_CLOSED_MESSAGE);

            long reservedFromParent0 = reservedFromParent.get();

            long diff = currentlyReserved - reservedFromParent0;

            if (diff <= 0) // someone already reserved memory from parent
                return true;

            // If single block size is too small.
            long reservationSize = Math.max(diff, blockSize);

            // If we are too close to limit.
            if (quota > 0)
                reservationSize = Math.min(reservationSize, quota - reservedFromParent0);

            if (parent.reserve(reservationSize))
                reservedFromParent.addAndGet(reservationSize);
            else
                return false;

            return true;
        }
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
    @Override public void release(long size) {
        assert size >= 0;

        if (closed)
            throw new TrackerWasClosedException(TRACKER_WAS_CLOSED_MESSAGE);

        if (size == 0 || state == STATE_CLOSED)
            return;

        final long reserved0 = reserved.addAndGet(-size);

        maxReserved.set(Math.max(maxReserved.get(), reserved0 + size));

        assert reserved0 >= 0 : "Try to free more memory that ever be reserved: [reserved=" + (reserved0 + size) +
            ", toFree=" + size + ']';

        if (reservedFromParent.get() - reserved0 > blockSize && parent != null)
            releaseFromParent(reserved0);
    }

    /**
     * Releases memory from parent.
     */
    private void releaseFromParent(long currentlyReserved) {
        synchronized (lock) {
            if (closed)
                throw new TrackerWasClosedException(TRACKER_WAS_CLOSED_MESSAGE);

            long toReleaseFromParent = reservedFromParent.get() - currentlyReserved;

            if (toReleaseFromParent < blockSize)
                return;

            parent.release(toReleaseFromParent);

            final long reservedFromParent0 = reservedFromParent.addAndGet(-toReleaseFromParent);

            assert reservedFromParent0 >= 0 : reservedFromParent;
        }
    }

    /** {@inheritDoc} */
    @Override public long reserved() {
        return reserved.get();
    }

    /** {@inheritDoc} */
    @Override public long maxReserved() {
        return Math.max(maxReserved.get(), reserved.get());
    }

    /** {@inheritDoc} */
    @Override public long writtenOnDisk() {
        return writtenOnDisk.get();
    }

    /** {@inheritDoc} */
    @Override public long maxWrittenOnDisk() {
        return Math.max(maxWrittenOnDisk.get(), writtenOnDisk.get());
    }

    /** {@inheritDoc} */
    @Override public long totalWrittenOnDisk() {
        return totalWrittenOnDisk.get();
    }

    /**
     * @return Offloading enabled flag.
     */
    public boolean isOffloadingEnabled() {
        return offloadingEnabled;
    }

    /** {@inheritDoc} */
    @Override public void spill(long size) {
        assert size >= 0;

        if (closed)
            throw new TrackerWasClosedException(TRACKER_WAS_CLOSED_MESSAGE);

        if (size == 0)
            return;

        if (parent != null)
            parent.spill(size);

        writtenOnDisk.addAndGet(size);
        totalWrittenOnDisk.addAndGet(size);
    }

    /** {@inheritDoc} */
    @Override public synchronized void unspill(long size) {
        assert size >= 0;

        if (closed)
            throw new TrackerWasClosedException(TRACKER_WAS_CLOSED_MESSAGE);

        if (size == 0)
            return;

        if (parent != null)
            parent.unspill(size);

        long writtenOnDisk0 = writtenOnDisk.getAndAdd(-size);

        maxWrittenOnDisk.set(Math.max(maxWrittenOnDisk.get(), writtenOnDisk0));
    }

    /**
     * @return {@code true} if closed, {@code false} otherwise.
     */
    @Override public boolean closed() {
        return closed;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        if (state == STATE_CLOSED)
            return;

        synchronized (lock) {
            // It is not expected to be called concurrently with reserve\release.
            // But query can be cancelled concurrently on query finish.
            if (state == STATE_CLOSED)
                return;

            state = STATE_CLOSED;

            children.forEach(H2MemoryTracker::close);

            closed = true;
            reserved.set(0);
            writtenOnDisk.set(0);

            if (parent != null)
                parent.release(reservedFromParent.get());

            reservedFromParent.set(0);
        }
    }

    /** {@inheritDoc} */
    @Override public void incrementFilesCreated() {
        if (closed)
            throw new TrackerWasClosedException(TRACKER_WAS_CLOSED_MESSAGE);

        if (parent != null)
            parent.incrementFilesCreated();

        filesCreated.incrementAndGet();
    }

    /** {@inheritDoc} */
    @Override public H2MemoryTracker createChildTracker() {
        if (state == STATE_CLOSED)
            throw new TrackerWasClosedException(TRACKER_WAS_CLOSED_MESSAGE);

        H2MemoryTracker child = new ChildMemoryTracker(this);

        children.add(child);

        if (state == STATE_CLOSED)
            throw new TrackerWasClosedException(TRACKER_WAS_CLOSED_MESSAGE);

        return child;
    }

    /**
     * @return Count of created files.
     */
    public int filesCreated() {
        return filesCreated.get();
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
            if (state == STATE_CLOSED)
                throw new TrackerWasClosedException(TRACKER_WAS_CLOSED_MESSAGE);

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
            if (state == STATE_CLOSED)
                throw new TrackerWasClosedException(TRACKER_WAS_CLOSED_MESSAGE);

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
            if (state == STATE_CLOSED)
                throw new TrackerWasClosedException(TRACKER_WAS_CLOSED_MESSAGE);

            parent.spill(size);

            writtenOnDisk += size;
            totalWrittenOnDisk += size;
        }

        /** {@inheritDoc} */
        @Override public void unspill(long size) {
            if (state == STATE_CLOSED)
                throw new TrackerWasClosedException(TRACKER_WAS_CLOSED_MESSAGE);

            parent.unspill(size);

            writtenOnDisk -= size;
        }

        /** {@inheritDoc} */
        @Override public void incrementFilesCreated() {
            if (state == STATE_CLOSED)
                throw new TrackerWasClosedException(TRACKER_WAS_CLOSED_MESSAGE);

            parent.incrementFilesCreated();
        }

        /** {@inheritDoc} */
        @Override public H2MemoryTracker createChildTracker() {
            if (state == STATE_CLOSED)
                throw new TrackerWasClosedException(TRACKER_WAS_CLOSED_MESSAGE);

            return parent.createChildTracker();
        }

        /** {@inheritDoc} */
        @Override public boolean closed() {
            return state == STATE_CLOSED;
        }

        /** {@inheritDoc} */
        @Override public void close() {
            if (!STATE_UPDATER.compareAndSet(this, STATE_INITIAL, STATE_CLOSED))
                return;

            try {
                parent.release(reserved);
                parent.unspill(writtenOnDisk);
            }
            catch (TrackerWasClosedException ignored) {
                // NO-OP
            }

            reserved = 0;
            writtenOnDisk = 0;
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
