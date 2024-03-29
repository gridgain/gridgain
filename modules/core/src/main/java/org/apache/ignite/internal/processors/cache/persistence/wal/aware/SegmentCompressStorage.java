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

package org.apache.ignite.internal.processors.cache.persistence.wal.aware;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;

/**
 * Storage of actual information about current index of compressed segments.
 */
class SegmentCompressStorage {
    /** Logger. */
    private final IgniteLogger log;

    /** Flag of interrupt waiting on this object. */
    private volatile boolean interrupted;

    /** If WAL compaction enabled. */
    private final boolean compactionEnabled;

    /** Last successfully compressed segment. */
    private volatile long lastCompressedIdx = -1L;

    /** Last enqueued to compress segment. */
    private long lastEnqueuedToCompressIdx = -1L;

    /** Segment of last completed checkpoint. */
    private long lastCpIdx = -1L;

    /** Segments to compress queue. */
    private final Queue<Long> segmentsToCompress = new ArrayDeque<>();

    /** List of currently compressing segments. */
    private final List<Long> compressingSegments = new ArrayList<>();

    /** Compressed segment with maximal index. */
    private long lastMaxCompressedIdx = -1L;

    /**
     * Constructor.
     *
     * @param log Logger.
     * @param compactionEnabled If WAL compaction enabled.
     */
    SegmentCompressStorage(IgniteLogger log, boolean compactionEnabled) {
        this.log = log;
        this.compactionEnabled = compactionEnabled;
    }

    /**
     * Sets the largest index of previously compressed segment.
     *
     * @param idx Absolute segment index.
     */
    synchronized void lastSegmentCompressed(long idx) {
        onSegmentCompressed(lastEnqueuedToCompressIdx = idx);
    }

    /**
     * Update segment of last completed checkpoint.
     *
     * @param lastCpIdx Absolute segment index.
     */
    synchronized void lastCheckpointIdx(long lastCpIdx) {
        this.lastCpIdx = lastCpIdx;
    }

    /**
     * Callback after segment compression finish.
     *
     * @param compressedIdx Index of compressed segment.
     */
    synchronized void onSegmentCompressed(long compressedIdx) {
        if (log.isInfoEnabled())
            log.info("Segment compressed notification [idx=" + compressedIdx + ']');

        if (compressedIdx > lastMaxCompressedIdx)
            lastMaxCompressedIdx = compressedIdx;

        compressingSegments.remove(compressedIdx);

        if (!compressingSegments.isEmpty())
            this.lastCompressedIdx = Math.min(lastMaxCompressedIdx, compressingSegments.get(0) - 1);
        else
            this.lastCompressedIdx = lastMaxCompressedIdx;

        if (compressedIdx > lastEnqueuedToCompressIdx)
            lastEnqueuedToCompressIdx = compressedIdx;
    }

    /**
     * @return Last compressed segment.
     */
    long lastCompressedIdx() {
        return lastCompressedIdx;
    }

    /**
     * Pessimistically tries to reserve segment for compression in order to avoid concurrent truncation. Waits if
     * there's no segment to archive right now.
     */
    synchronized long nextSegmentToCompressOrWait() throws IgniteInterruptedCheckedException {
        try {
            while (segmentsToCompress.peek() == null && !interrupted)
                wait();
        }
        catch (InterruptedException e) {
            throw new IgniteInterruptedCheckedException(e);
        }

        checkInterrupted();

        Long idx = segmentsToCompress.poll();

        assert idx != null;

        compressingSegments.add(idx);

        return idx;
    }

    /**
     * Interrupt waiting on this object.
     */
    synchronized void interrupt() {
        interrupted = true;

        notifyAll();
    }

    /**
     * Check for interrupt flag was set.
     */
    private void checkInterrupted() throws IgniteInterruptedCheckedException {
        if (interrupted)
            throw new IgniteInterruptedCheckedException("Interrupt waiting of change compressed idx");
    }

    /**
     * Callback for waking up compressor when new segment is archived.
     */
    synchronized void onSegmentArchived(long lastAbsArchivedIdx) {
        while (lastEnqueuedToCompressIdx < Math.min(lastAbsArchivedIdx, lastCpIdx - 1) && compactionEnabled) {
            if (log.isInfoEnabled())
                log.info("Enqueuing segment for compression [idx=" + (lastEnqueuedToCompressIdx + 1) + ']');

            segmentsToCompress.add(++lastEnqueuedToCompressIdx);
        }

        notifyAll();
    }

    /**
     * Reset interrupted flag.
     */
    public void reset() {
        interrupted = false;
    }
}
