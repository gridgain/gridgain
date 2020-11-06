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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointManager;
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

    /** Сurrent size of WAL archive in bytes. */
    private final AtomicLong currSize = new AtomicLong();

    /** WAL manger instance. */
    @Nullable private final IgniteWriteAheadLogManager walMgr;

    /** Checkpoint manager instance. */
    @Nullable private final CheckpointManager cpMgr;

    /** Mapping: absolute segment index -> segment size in bytes. */
    @Nullable private final NavigableMap<Long, Long> sizes;

    /** Number of segments that can be deleted now. */
    private volatile int availableToClear;

    /** Absolute segment index last checkpoint. */
    private long cpIdx;

    /** Smallest absolute index of reserved segment, {@code -1} if not present. */
    private long reservedIdx = -1;

    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     */
    public WalArchiveSize(GridKernalContext ctx) {
        assert !ctx.clientNode();

        log = ctx.log(getClass());

        maxSize = ctx.config().getDataStorageConfiguration().getMaxWalArchiveSize();

        if (!unlimited()) {
            walMgr = ctx.cache().context().wal();
            cpMgr = ((GridCacheDatabaseSharedManager)ctx.cache().context().database()).checkpointManager();

            sizes = new TreeMap<>();

            // Adding a callback on change border for recovery.
            cpMgr.checkpointHistory().addObserver(cpEntry -> {
                synchronized (this) {
                    cpIdx = ((FileWALPointer)cpEntry.checkpointMark()).index();

                    updateAvailableToClear();
                }
            });

            // Adding callback on changes minimum reserved segment.
            FileWriteAheadLogManager walMgr0 = (FileWriteAheadLogManager)walMgr;

            if (walMgr0.isArchiverEnabled()) {
                walMgr0.addMinReservedSegmentObserver(absSegIdx -> {
                    synchronized (this) {
                        reservedIdx = absSegIdx == null ? -1 : absSegIdx;

                        updateAvailableToClear();
                    }
                });
            }
        }
        else {
            sizes = null;
            walMgr = null;
            cpMgr = null;
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
                int segmentCnt = sizes.size();

                if (sizes.merge(idx, size, Long::sum) == 0)
                    sizes.remove(idx);

                if (segmentCnt != sizes.size())
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
     * @throws IgniteCheckedException If failed.
     */
    public void reserveSpaceWithClear(long idx, long size) throws IgniteCheckedException {
        if (!unlimited()) {
            synchronized (this) {
                try {
                    while (maxSize - currentSize() < size) {
                        if (availableToClear == 0)
                            wait();
                        else {
                            FileWALPointer low = new FileWALPointer(sizes.firstKey(), 0, 0);
                            FileWALPointer high = new FileWALPointer(minIdx(), 0, 0);

                            cpMgr.removeCheckpointsUntil(high);
                            int rmvSegments = walMgr.truncate(low, high);

                            if (log.isInfoEnabled()) {
                                log.info("Clearning WAL archive [low=" + low + ", high=" + high +
                                    ", removedSegments=" + rmvSegments + ", availableToClear=" + availableToClear +
                                    ']');
                            }

                            if (rmvSegments == 0)
                                wait();
                        }
                    }

                    add(idx, size);
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
                sizes.clear();

                segments.forEach(t -> sizes.merge(t.get1(), t.get2(), Long::sum));

                updateAvailableToClear();

                if (maxSize - currentSize() < 0) {
                    if (availableToClear == 0)
                        return false;

                    FileWALPointer low = new FileWALPointer(sizes.firstKey(), 0, 0);
                    FileWALPointer high = new FileWALPointer(minIdx(), 0, 0);

                    cpMgr.removeCheckpointsUntil(high);
                    int rmvSegments = walMgr.truncate(low, high);

                    if (log.isInfoEnabled()) {
                        log.info("Clearning WAL archive on prepare stage [low=" + low + ", high=" + high +
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
     * Return current size of WAL archive in bytes.
     *
     * @return Сurrent size of WAL archive in bytes.
     */
    public long currentSize() {
        return currSize.get();
    }

    /**
     * Return maximum WAL archive size in bytes.
     *
     * @return Maximum WAL archive size in bytes.
     */
    public long maxSize() {
        return maxSize;
    }

    /**
     * Recalculation of number of segments that can be deleted now.
     */
    private synchronized void updateAvailableToClear() {
        assert !unlimited();

        availableToClear = sizes.headMap(minIdx(), false).size();

        notifyAll();
    }

    /**
     * Return index to which it is safe to clear archive.
     *
     * @return Index to which it is safe to clear archive.
     */
    private long minIdx() {
        return reservedIdx == -1 ? cpIdx : Math.min(cpIdx, reservedIdx);
    }

    /**
     * Checking that archive is unlimited.
     *
     * @return {@code True} if archive is unlimited.
     */
    private boolean unlimited() {
        return maxSize == DataStorageConfiguration.UNLIMITED_WAL_ARCHIVE;
    }
}
