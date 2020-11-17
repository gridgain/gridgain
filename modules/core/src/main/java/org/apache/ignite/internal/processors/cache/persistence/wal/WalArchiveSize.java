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

package org.apache.ignite.internal.processors.cache.persistence.wal;

import java.util.Collection;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointManager;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointProgress;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.Checkpointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.aware.SegmentAware;
import org.apache.ignite.internal.processors.failure.FailureProcessor;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

/**
 * Class for working with WAL archive size.
 * If archive is not unlimited, then it makes sure that maximum archive size is not exceeded.
 * If there is insufficient space, archive is cleaned.
 * Reserved segments or those required for recovery will not be affected by cleanup.
 */
public class WalArchiveSize {
    /** Logger. */
    private final IgniteLogger log;

    /** Maximum WAL archive size in bytes. */
    private final long maxSize;

    /** Current size of WAL archive in bytes. */
    private final AtomicLong currSize = new AtomicLong();

    /** Mapping: absolute segment index -> segment size in bytes. */
    @Nullable private final NavigableMap<Long, Long> sizes;

    /** WAL manger instance. */
    private final IgniteWriteAheadLogManager walMgr;

    /** Checkpoint manager instance. */
    @Nullable private volatile CheckpointManager cpMgr;

    /** Holder of actual information of latest manipulation on WAL segments. */
    @Nullable private volatile SegmentAware segmentAware;

    /** Failure processor. */
    private final FailureProcessor failureProcessor;

    /** Predicate for checking whether node should fail because WAL archive is full and cannot be cleared. */
    @Nullable private volatile BooleanSupplier shouldFailure;

    /** Number of segments that can be deleted now. */
    private volatile int availableToClear;

    /** Segment index of last checkpoint, up to which we can safely delete segments in archive. */
    private long cpIdx;

    /** Index of smallest reserved segment, up to which we can safely delete segments in archive. */
    private long reservedIdx = -1;

    /** Node stopping flag. */
    private boolean nodeStop;

    /** Node failed by {@link #shouldFailure} flag. */
    private volatile boolean nodeFailure;

    /**
     * Constructor.
     *
     * @param logFun Function for getting a logger.
     * @param dsCfg Data storage configuration.
     * @param walMgr WAL manger instance.
     * @param failureProcessor Failure processor.
     */
    public WalArchiveSize(
        Function<Class<?>, IgniteLogger> logFun,
        DataStorageConfiguration dsCfg,
        IgniteWriteAheadLogManager walMgr,
        FailureProcessor failureProcessor
    ) {
        log = logFun.apply(getClass());

        maxSize = dsCfg.getMaxWalArchiveSize();

        this.walMgr = walMgr;
        this.failureProcessor = failureProcessor;

        sizes = !unlimited() ? new TreeMap<>() : null;
    }

    /**
     * Callback on start of WAL manager.
     *
     * @param segAware Holder of actual information of latest manipulation on WAL segments.
     * @param dbMgr Database manager.
     * @param archiveEnabled Archive enabled.
     * @param compressionEnabled Compression enabled.
     */
    public void onStartWalManager(
        SegmentAware segAware,
        GridCacheDatabaseSharedManager dbMgr,
        boolean archiveEnabled,
        boolean compressionEnabled
    ) {
        if (!unlimited()) {
            this.segmentAware = segAware;

            shouldFailure = () -> !checkpointInProgress(dbMgr) && dbMgr.txAcquireCheckpointReadLockCount() > 0 &&
                ((!archiveEnabled && !segAware.rolloverInProgress())
                    || (archiveEnabled && segAware.isWaitSegmentArchiving() && !segAware.archivingInProgress())) &&
                (!compressionEnabled || !segAware.compressionInProgress());

            if (archiveEnabled) {
                segAware.addMinReservedSegmentObserver(absSegIdx -> {
                    synchronized (this) {
                        reservedIdx = absSegIdx == null ? -1 : absSegIdx;

                        updateAvailableToClear();

                        logShortInfo("Update after changing minimum reserved segment");
                    }
                });
            }
        }
    }

    /**
     * Callback on start of checkpoint manager.
     *
     * @param cpMgr Checkpoint manager.
     */
    public void onStartCheckpointManager(CheckpointManager cpMgr) {
        if (!unlimited()) {
            this.cpMgr = cpMgr;

            cpMgr.checkpointHistory().addObserver(cpEntry -> {
                synchronized (this) {
                    cpIdx = ((FileWALPointer)cpEntry.checkpointMark()).index();

                    updateAvailableToClear();

                    logShortInfo("Update after last finished checkpoint");
                }
            });
        }
    }

    /**
     * Adding a segment size.
     *
     * @param idx Absolut segment index.
     * @param size Segment size in bytes.
     */
    public void add(long idx, long size) {
        currSize.addAndGet(size);

        if (!unlimited()) {
            synchronized (this) {
                assert sizes != null;

                long res = sizes.merge(idx, size, Long::sum);

                if (res == 0)
                    sizes.remove(idx);
                else if (res <= 0) {
                    // To avoid double deletion of one file from different threads.
                    sizes.remove(idx);
                    currSize.addAndGet(-size);
                }

                updateAvailableToClear();
            }
        }
    }

    /**
     * Reserving space in WAL archive. If it is not unlimited, then if there is not enough space,
     * it will try to clear it until there's enough space. If cleanup is not possible now,
     * it will wait for checkpoint to finish or segments to be released.
     *
     * Concurrent reservation of space in archive if it is not unlimited.
     * Will continue until space is freed either by ending a checkpoint or releasing a segment.
     *
     * @param idx Absolut segment index.
     * @param size Required space in bytes.
     * @param afterReserveCb Callback after reserving.
     * @throws IgniteCheckedException If failed.
     */
    public void reserveSpaceWithClear(
        long idx,
        long size,
        @Nullable Runnable afterReserveCb
    ) throws IgniteCheckedException {
        if (!unlimited()) {
            synchronized (this) {
                assert sizes != null;

                try {
                    while (maxSize - currentSize() < size) {
                        if (nodeStop)
                            throw new IgniteCheckedException("Node stopping");
                        else if (nodeFailure)
                            throw new IgniteCheckedException("WAL archive is full and cannot be cleared");
                        else if (availableToClear == 0) {
                            BooleanSupplier shouldFall = this.shouldFailure;

                            if (shouldFall != null && shouldFall.getAsBoolean()) {
                                nodeFailure = true;

                                SegmentAware segmentAware = this.segmentAware;

                                if (segmentAware != null)
                                    segmentAware.forceInterrupt();

                                failureProcessor.process(
                                    new FailureContext(
                                        FailureType.CRITICAL_ERROR,
                                        new IgniteCheckedException("WAL archive is full and cannot be cleared")
                                    ),
                                    new StopNodeFailureHandler()
                                );

                                notifyAll();
                            }
                            else
                                wait(1_000);
                        }
                        else {
                            FileWALPointer low = new FileWALPointer(sizes.firstKey(), 0, 0);
                            FileWALPointer high = new FileWALPointer(safeIdx(), 0, 0);

                            CheckpointManager cpMgr = this.cpMgr;

                            if (cpMgr != null)
                                cpMgr.removeCheckpointsUntil(high);

                            int rmvSegments = walMgr.truncate(low, high);

                            if (log.isInfoEnabled()) {
                                log.info("Cleaning WAL archive [low=" + low.index() + ", high=" + high.index() +
                                    ", removedSegments=" + rmvSegments + ", availableToClear=" + availableToClear +
                                    ']');
                            }

                            if (rmvSegments == 0)
                                wait();
                        }
                    }

                    add(idx, size);

                    if (afterReserveCb != null)
                        afterReserveCb.run();

                    notifyAll();
                }
                catch (InterruptedException e) {
                    throw new IgniteInterruptedCheckedException(e);
                }
            }
        }
        else
            add(idx, size);
    }

    /**
     * Getting number of segments that can be deleted in archive.
     *
     * @return Number of segments that can be deleted in archive,
     *      {@code -1} if there is space in it and it is not necessary to clean it.
     */
    public int availableDeleteArchiveSegments() {
        return unlimited() || (maxSize - currentSize() > 0) ? -1 : availableToClear;
    }

    /**
     * Check that the segments in the archive do not exceed the maximum.
     * If there is not enough space, then an attempt is made to clean.
     * Segment data will be reloaded.
     *
     * @param segments Segments data: id and size.
     * @return {@code True} if archive is valid.
     * @throws IgniteCheckedException If failed.
     */
    public boolean prepareAndCheck(Collection<IgniteBiTuple<Long, Long>> segments) throws IgniteCheckedException {
        currSize.set(segments.stream().mapToLong(IgniteBiTuple::get2).sum());

        if (!unlimited()) {
            synchronized (this) {
                assert sizes != null;

                sizes.clear();

                segments.forEach(t -> sizes.merge(t.get1(), t.get2(), Long::sum));

                updateAvailableToClear();

                if (maxSize - currentSize() < 0) {
                    if (availableToClear == 0)
                        return false;

                    FileWALPointer low = new FileWALPointer(sizes.firstKey(), 0, 0);
                    FileWALPointer high = new FileWALPointer(safeIdx(), 0, 0);

                    CheckpointManager cpMgr = this.cpMgr;

                    if (cpMgr != null)
                        cpMgr.removeCheckpointsUntil(high);

                    int rmvSegments = walMgr.truncate(low, high);

                    if (log.isInfoEnabled()) {
                        log.info("Cleaning WAL archive on prepare stage [low=" + low + ", high=" + high +
                            ", removedSegments=" + rmvSegments + ", availableToClear=" + availableToClear + ']');
                    }

                    if (rmvSegments == 0 || maxSize - currentSize() < 0)
                        return false;
                }
            }
        }

        return true;
    }

    /**
     * Getting current size of WAL archive in bytes.
     *
     * @return Current size of WAL archive in bytes.
     */
    public long currentSize() {
        return currSize.get();
    }

    /**
     * Getting maximum WAL archive size in bytes.
     *
     * @return Maximum WAL archive size in bytes.
     */
    public long maxSize() {
        return maxSize;
    }

    /**
     * Shutdown when the node stopping.
     */
    public synchronized void shutdown() {
        nodeStop = true;

        notifyAll();
    }

    /**
     * Check whether node failed because WAL archive is full and cannot be cleared.
     *
     * @return {@code True} if node failed.
     */
    public boolean nodeFailure() {
        return nodeFailure;
    }

    /**
     * Recalculation of number of segments that can be deleted now.
     */
    private synchronized void updateAvailableToClear() {
        assert !unlimited();
        assert sizes != null;

        availableToClear = sizes.headMap(safeIdx(), false).size();

        notifyAll();
    }

    /**
     * Calculation of index up to which we can delete segments in archive.
     *
     * @return Index to which it is safe to clear archive.
     */
    private long safeIdx() {
        return reservedIdx == -1 ? cpIdx : Math.min(cpIdx, reservedIdx);
    }

    /**
     * Checking that archive is unlimited.
     *
     * @return {@code True} if archive unlimited.
     */
    private boolean unlimited() {
        return maxSize == DataStorageConfiguration.UNLIMITED_WAL_ARCHIVE;
    }

    /**
     * Output of short information to log.
     *
     * @param prefix Message prefix.
     */
    private synchronized void logShortInfo(String prefix) {
        if (log.isInfoEnabled()) {
            NavigableMap<Long, Long> sizes = this.sizes;

            log.info(prefix + " [cpIdx=" + cpIdx + ", reservedIdx=" + reservedIdx + ", safeIdx=" + safeIdx()
                + ", segments=" + (sizes == null ? 0 : sizes.size()) + ", availableToClear=" + availableToClear + ']');
        }
    }

    /**
     * Checking that checkpoint is in progress.
     *
     * @param dbMgr Database manager.
     * @return {@code True} if checkpoint in progress.
     */
    private boolean checkpointInProgress(@Nullable GridCacheDatabaseSharedManager dbMgr) {
        if (dbMgr != null) {
            Checkpointer checkpointer = dbMgr.getCheckpointer();

            if (checkpointer != null) {
                CheckpointProgress checkpointProgress = checkpointer.currentProgress();

                if (checkpointProgress != null)
                    return checkpointProgress.inProgress();
            }
        }

        return false;
    }
}
