/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.cache.persistence.wal;

import java.util.Collections;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteAbsClosureX;
import org.apache.ignite.lang.IgniteBiClosureX;
import org.jetbrains.annotations.Nullable;

/**
 * Class for keeping track of WAL archive size.
 * <p/>
 * Allows to organize parallel work with WAL archive
 * using methods {@link #reserve} and {@link #release} similar to locks.
 * <p/>
 * If during reservation there is no space and it is impossible to clear WAL archive,
 * then there will be a wait for state update through the methods:
 * {@link #updateCurrentSize}, {@link #updateLastCheckpointSegmentIndex}, {@link #updateMinReservedSegmentIndex}.
 * <p/>
 * Segments can be deleted only if they are not needed for recovery and are not reserved by other processes.
 */
public class WalArchiveSize {
    /** Logger. */
    private final IgniteLogger log;

    /** Max WAL archive size in bytes. */
    private final long max;

    /** Current WAL archive size in bytes. */
    private volatile long curr;

    /** Reserved WAL archive size in bytes. */
    private volatile long reserved;

    /**
     * Current segments in WAL archive.
     * Mapping: absolute index of segment -> total segment size.
     */
    private final NavigableMap<Long, Long> segments;

    /** Segment index of last checkpoint. */
    private long lastCheckpointSegmentIdx;

    /** Segment index of minimum reserved from deletion, {@code -1} if absent. */
    private long minReservedSegmentIdx = -1;

    /** Number of segments that can be removed from WAL archive now. */
    private volatile int availableDel;

    /**
     * Constructor.
     *
     * @param log Logger.
     * @param max Max WAL archive size in bytes.
     */
    public WalArchiveSize(IgniteLogger log, long max) {
        this.log = log;
        this.max = max;

        segments = unlimited() ? Collections.emptyNavigableMap() : new TreeMap<>();
    }

    /**
     * Reservation of space in WAL archive, with ability to clean up if there is insufficient space.
     * After reservation and end of work with WAL archive, space must be {@link #release released}.
     *
     * A range of segments that can be safely deleted are passed to input of cleanup function.
     * If cleaning is not possible now, it will wait for the update of WAL archive state.
     *
     * @param size Byte count.
     * @param cleanupC Cleanup closure.
     * @param beforeWaitC Closing before waiting.
     *
     * @throws IgniteInterruptedCheckedException If waiting for free space is interrupted.
     * @throws IgniteCheckedException If failed.
     */
    public synchronized void reserve(
        long size,
        @Nullable IgniteBiClosureX<Long, Long, Integer> cleanupC,
        @Nullable IgniteAbsClosureX beforeWaitC
    ) throws IgniteCheckedException {
        while (!unlimited() && max - (curr + reserved) < size) {
            if (availableDel == 0 || (cleanupC != null && cleanupC.applyx(segments.firstKey(), safeCleanIdx()) == 0)) {
                if (beforeWaitC != null)
                    beforeWaitC.applyx();

                U.wait(this);
            }
        }

        release(-size);
    }

    /**
     * Release previously {@link #reserve reserved} space in WAL archive.
     *
     * @param size Byte count.
     */
    public synchronized void release(long size) {
        reserved -= size;

        notifyAll();
    }

    /**
     * Add or subtract current segment size.
     *
     * @param idx Absolut segment index.
     * @param size Segment size in bytes.
     */
    public synchronized void updateCurrentSize(long idx, long size) {
        curr += size;

        if (!unlimited()) {
            long res = segments.merge(idx, size, Long::sum);

            if (res == 0)
                segments.remove(idx);
            else if (res < 0) {
                // Avoid parallel deletion.
                segments.remove(idx);
                curr -= size;
            }

            updateAvailableDelete();

            logState("Update current size of WAL archive");
        }
    }

    /**
     * Update {@link #minReservedSegmentIdx}.
     * Segments that are less than this are not reserved by other processes, i.e. they can be deleted.
     *
     * @param idx Absolut segment index.
     */
    public synchronized void updateMinReservedSegmentIndex(@Nullable Long idx) {
        minReservedSegmentIdx = idx == null ? -1 : idx;

        updateAvailableDelete();

        logState("Update minimum reserved segment");
    }

    /**
     * Update {@link #lastCheckpointSegmentIdx}.
     * Segments that are less than this are no longer needed for recovery, i.e. they can be deleted.
     *
     * @param idx Absolut segment index.
     */
    public synchronized void updateLastCheckpointSegmentIndex(long idx) {
        lastCheckpointSegmentIdx = idx;

        updateAvailableDelete();

        logState("Update last checkpoint segment");
    }

    /**
     * Checking if maximum WAL archive size is exceeded.
     *
     * @return {@code True} if exceeded.
     */
    public boolean exceedMax() {
        return max - (curr + reserved) < 0;
    }

    /**
     * Checking whether WAL archive is unlimited.
     *
     * @return {@code True} if unlimited.
     */
    public boolean unlimited() {
        return max == DataStorageConfiguration.UNLIMITED_WAL_ARCHIVE;
    }

    /**
     * Getting number of segments that can be removed from WAL archive now.
     *
     * @return Number of segments.
     */
    public int availableDelete() {
        return availableDel;
    }

    /**
     * Getting current size of WAL archive in bytes.
     *
     * @return Size in bytes.
     */
    public long currentSize() {
        return curr;
    }

    /**
     * Getting reserved size of WAL archive in bytes.
     *
     * @return Size in bytes.
     */
    public long reservedSize() {
        return reserved;
    }

    /**
     * Getting max WAL archive size in bytes.
     *
     * @return Size in bytes.
     */
    public long maxSize() {
        return max;
    }

    /**
     * Getting copy of current segments in WAL archive.
     *
     * @return Mapping: absolute segment index -> total segment size.
     */
    public synchronized NavigableMap<Long, Long> currentSegments() {
        return new TreeMap<>(segments);
    }

    /**
     * Update {@link #availableDel}.
     */
    private synchronized void updateAvailableDelete() {
        if (!unlimited()) {
            availableDel = segments.headMap(safeCleanIdx()).size();

            notifyAll();
        }
    }

    /**
     * Getting index segment to which it is safe to clean WAL archive.
     *
     * @return Segment index.
     */
    private synchronized long safeCleanIdx() {
        return minReservedSegmentIdx == -1 ? lastCheckpointSegmentIdx :
            Math.min(lastCheckpointSegmentIdx, minReservedSegmentIdx);
    }

    /**
     * Log current state.
     *
     * @param prefix Log message prefix.
     */
    private synchronized void logState(String prefix) {
        if (log.isInfoEnabled()) {
            String msg = "max=" + (unlimited() ? "unlimited" : U.humanReadableByteCount(max)) +
                ", currentSize=" + U.humanReadableByteCount(curr);

            if (!unlimited()) {
                msg += ", reservedSize=" + U.humanReadableByteCount(reserved) + ", segmentCnt=" + segments.size() +
                    ", availableDelete=" + availableDel;
            }

            log.info(prefix + " [" + msg + ']');
        }
    }
}
