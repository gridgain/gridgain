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

package org.apache.ignite.internal.processors.cache.persistence.pagemem;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointProgress;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.lang.IgniteOutClosure;

class CleanPagesProtectionThrottle {
    /**
     * Maximum dirty pages in region.
     */
    private static final double MAX_DIRTY_PAGES = 0.75;

    /**
     * Percent of dirty pages which will not cause throttling.
     */
    private static final double MIN_RATIO_NO_THROTTLE = 0.03;

    /**
     * Page memory.
     */
    private final PageMemoryImpl pageMemory;

    /**
     * Checkpoint progress provider.
     */
    private final IgniteOutClosure<CheckpointProgress> cpProgress;

    /**
     * Total pages which is possible to store in page memory.
     */
    private final long totalPages;

    /**
     * Last estimated speed for marking all clear pages as dirty till the end of checkpoint.
     */
    private volatile long speedForMarkAll;

    /**
     * Target (maximum) dirty pages ratio, after which throttling will start using
     * {@link #getParkTime(double, long, int, int, long, long)}.
     */
    private volatile double targetDirtyRatio;

    /**
     * Current dirty pages ratio (percent of dirty pages in most used segment), negative value means no cp is running.
     */
    private volatile double currDirtyRatio;

    /**
     * Threads set. Contains identifiers of all threads which were marking pages for current checkpoint.
     */
    private final GridConcurrentHashSet<Long> threadIds = new GridConcurrentHashSet<>();

    /**
     * Counter of written pages from checkpoint. Value is saved here for detecting checkpoint start.
     */
    private final AtomicInteger lastObservedWritten = new AtomicInteger(0);

    /**
     * Dirty pages ratio was observed at checkpoint start (here start is moment when first page was actually saved to
     * store). This ratio is excluded from throttling.
     */
    private volatile double initDirtyRatioAtCpBegin = MIN_RATIO_NO_THROTTLE;

    /**
     * Speed average checkpoint write speed. Current and 3 past checkpoints used. Pages/second.
     */
    private final IntervalBasedMeasurement speedCpWrite = new IntervalBasedMeasurement();

    /**
     * Used for calculating speed of marking pages dirty.
     * Value from past 750-1000 millis only.
     * {@link IntervalBasedMeasurement#getSpeedOpsPerSec(long)} returns pages marked/second.
     * {@link IntervalBasedMeasurement#getAverage()} returns average throttle time.
     */
    private final IntervalBasedMeasurement speedMarkAndAvgParkTime;

    CleanPagesProtectionThrottle(PageMemoryImpl pageMemory,
                                 IgniteOutClosure<CheckpointProgress> cpProgress,
                                 IntervalBasedMeasurement speedMarkAndAvgParkTime) {
        this.pageMemory = pageMemory;
        this.cpProgress = cpProgress;
        this.speedMarkAndAvgParkTime = speedMarkAndAvgParkTime;

        totalPages = pageMemory.totalPages();
    }

    /**
     * @return Target (maximum) dirty pages ratio, after which throttling will start.
     */
    public double getTargetDirtyRatio() {
        return targetDirtyRatio;
    }

    /**
     * @return Current dirty pages ratio.
     */
    public double getCurrDirtyRatio() {
        double ratio = currDirtyRatio;

        if (ratio >= 0)
            return ratio;

        return pageMemory.getDirtyPagesRatio();
    }

    /**
     * @return Returns {@link #speedForMarkAll}.
     */
    public long getLastEstimatedSpeedForMarkAll() {
        return speedForMarkAll;
    }

    /**
     * @return Speed average checkpoint write speed. Current and 3 past checkpoints used. Pages/second.
     */
    public long getCpWriteSpeed() {
        return speedCpWrite.getSpeedOpsPerSecReadOnly();
    }

    /***/
    long computeCleanPagesProtectionParkTime(long curNanoTime) {
        CheckpointProgress progress = cpProgress.apply();
        AtomicInteger writtenPagesCntr = progress == null ? null : progress.writtenPagesCounter();

        if (writtenPagesCntr == null) {
            resetStatistics();

            return PagesWriteSpeedBasedThrottle.NO_THROTTLING_MARKER;
        }

        return computeCleanPagesProtectionParkTime(writtenPagesCntr, curNanoTime);
    }

    /***/
    private void resetStatistics() {
        speedForMarkAll = 0;
        targetDirtyRatio = -1;
        currDirtyRatio = -1;
    }

    /***/
    private long computeCleanPagesProtectionParkTime(AtomicInteger writtenPagesCntr, long curNanoTime) {
        threadIds.add(Thread.currentThread().getId());

        ThrottleMode level = ThrottleMode.NO;
        long throttleParkTimeNs = 0;

        int cpWrittenPages = writtenPagesCntr == null ? 0 : writtenPagesCntr.get();
        long fullyCompletedPages = (cpWrittenPages + cpSyncedPages()) / 2; // written & sync'ed

        long markDirtySpeed = speedMarkAndAvgParkTime.getSpeedOpsPerSec(curNanoTime);
        speedCpWrite.setCounter(fullyCompletedPages, curNanoTime);
        long curCpWriteSpeed = speedCpWrite.getSpeedOpsPerSec(curNanoTime);

        int nThreads = threadIdsCount();

        int cpTotalPages = cpTotalPages();

        if (cpTotalPages == 0) {
            boolean throttleByCpSpeed = curCpWriteSpeed > 0 && markDirtySpeed > curCpWriteSpeed;

            if (throttleByCpSpeed) {
                throttleParkTimeNs = calcDelayTime(curCpWriteSpeed, nThreads, 1);
                level = ThrottleMode.LIMITED;
            }
        }
        else {
            double dirtyPagesRatio = pageMemory.getDirtyPagesRatio();

            currDirtyRatio = dirtyPagesRatio;

            detectCpPagesWriteStart(cpWrittenPages, dirtyPagesRatio);

            if (dirtyPagesRatio >= MAX_DIRTY_PAGES)
                level = ThrottleMode.NO; // too late to throttle, will wait on safe to update instead.
            else {
                int notEvictedPagesTotal = cpTotalPages - cpEvictedPages();

                throttleParkTimeNs = getParkTime(dirtyPagesRatio,
                        fullyCompletedPages,
                        Math.max(notEvictedPagesTotal, 0),
                        nThreads,
                        markDirtySpeed,
                        curCpWriteSpeed);
                level = throttleParkTimeNs == 0 ? ThrottleMode.NO : ThrottleMode.LIMITED;
            }
        }

        if (level == ThrottleMode.NO)
            throttleParkTimeNs = 0;

        return throttleParkTimeNs;
    }

    /**
     * @param dirtyPagesRatio     actual percent of dirty pages.
     * @param fullyCompletedPages written & fsynced pages count.
     * @param cpTotalPages        total checkpoint scope.
     * @param nThreads            number of threads providing data during current checkpoint.
     * @param markDirtySpeed      registered mark dirty speed, pages/sec.
     * @param curCpWriteSpeed     average checkpoint write speed, pages/sec.
     * @return time in nanoseconds to part or 0 if throttling is not required.
     */
    long getParkTime(
            double dirtyPagesRatio,
            long fullyCompletedPages,
            int cpTotalPages,
            int nThreads,
            long markDirtySpeed,
            long curCpWriteSpeed) {

        long speedForMarkAll = calcSpeedToMarkAllSpaceTillEndOfCp(dirtyPagesRatio,
                fullyCompletedPages,
                curCpWriteSpeed,
                cpTotalPages);

        double targetDirtyRatio = calcTargetDirtyRatio(fullyCompletedPages, cpTotalPages);

        this.speedForMarkAll = speedForMarkAll; //publish for metrics
        this.targetDirtyRatio = targetDirtyRatio; //publish for metrics

        boolean lowSpaceLeft = dirtyPagesRatio > targetDirtyRatio && (dirtyPagesRatio + 0.05 > MAX_DIRTY_PAGES);
        final int slowdown = lowSpaceLeft ? 3 : 1;

        double multiplierForSpeedForMarkAll = lowSpaceLeft
                ? 0.8
                : 1.0;

        boolean markingTooFast = speedForMarkAll > 0 && markDirtySpeed > multiplierForSpeedForMarkAll * speedForMarkAll;
        boolean throttleBySizeAndMarkSpeed = dirtyPagesRatio > targetDirtyRatio && markingTooFast;

        //for case of speedForMarkAll >> markDirtySpeed, allow write little bit faster than CP average
        double allowWriteFasterThanCp = (markDirtySpeed > 0 && speedForMarkAll > markDirtySpeed)
                ? (0.1 * speedForMarkAll / markDirtySpeed)
                : (dirtyPagesRatio > targetDirtyRatio ? 0.0 : 0.1);

        double fasterThanCpWriteSpeed = lowSpaceLeft
                ? 1.0
                : 1.0 + allowWriteFasterThanCp;
        boolean throttleByCpSpeed = curCpWriteSpeed > 0 && markDirtySpeed > (fasterThanCpWriteSpeed * curCpWriteSpeed);

        long delayByCpWrite;
        if (throttleByCpSpeed) {
            long nanosecPerDirtyPage = TimeUnit.SECONDS.toNanos(1) * nThreads / (markDirtySpeed);

            delayByCpWrite = calcDelayTime(curCpWriteSpeed, nThreads, slowdown) - nanosecPerDirtyPage;
        }
        else
            delayByCpWrite = 0;

        long delayByMarkAllWrite = throttleBySizeAndMarkSpeed ? calcDelayTime(speedForMarkAll, nThreads, slowdown) : 0;
        return Math.max(delayByCpWrite, delayByMarkAllWrite);
    }

    /**
     * @param dirtyPagesRatio     current percent of dirty pages.
     * @param fullyCompletedPages count of written and sync'ed pages
     * @param curCpWriteSpeed     pages/second checkpoint write speed. 0 speed means 'no data'.
     * @param cpTotalPages        total pages in checkpoint.
     * @return pages/second to mark to mark all clean pages as dirty till the end of checkpoint. 0 speed means 'no
     * data'.
     */
    private long calcSpeedToMarkAllSpaceTillEndOfCp(double dirtyPagesRatio,
                                                    long fullyCompletedPages,
                                                    long curCpWriteSpeed,
                                                    int cpTotalPages) {

        if (curCpWriteSpeed == 0)
            return 0;

        if (cpTotalPages <= 0)
            return 0;

        if (dirtyPagesRatio >= MAX_DIRTY_PAGES)
            return 0;

        double remainedClear = (MAX_DIRTY_PAGES - dirtyPagesRatio) * totalPages;

        double timeRemainedSeconds = 1.0 * (cpTotalPages - fullyCompletedPages) / curCpWriteSpeed;

        return (long) (remainedClear / timeRemainedSeconds);
    }

    /**
     * @param fullyCompletedPages number of completed.
     * @param cpTotalPages        Total amount of pages under checkpoint.
     * @return size-based calculation of target ratio.
     */
    private double calcTargetDirtyRatio(long fullyCompletedPages, int cpTotalPages) {
        double cpProgress = ((double) fullyCompletedPages) / cpTotalPages;

        // Starting with initialDirtyRatioAtCpBegin to avoid throttle right after checkpoint start
        double constStart = initDirtyRatioAtCpBegin;

        double throttleTotalWeight = 1.0 - constStart;

        // .75 is maximum ratio of dirty pages
        return (cpProgress * throttleTotalWeight + constStart) * MAX_DIRTY_PAGES;
    }

    /***/
    int threadIdsCount() {
        return threadIds.size();
    }

    /**
     * @return Counter for fsynced checkpoint pages.
     */
    int cpSyncedPages() {
        AtomicInteger syncedPagesCntr = cpProgress.apply().syncedPagesCounter();

        return syncedPagesCntr == null ? 0 : syncedPagesCntr.get();
    }

    /**
     * @return Number of pages in current checkpoint.
     */
    int cpTotalPages() {
        return cpProgress.apply().currentCheckpointPagesCount();
    }

    /**
     * @return number of evicted pages.
     */
    int cpEvictedPages() {
        AtomicInteger evictedPagesCntr = cpProgress.apply().evictedPagesCounter();

        return evictedPagesCntr == null ? 0 : evictedPagesCntr.get();
    }

    /**
     * @param baseSpeed   speed to slow down.
     * @param nThreads    operating threads.
     * @param coefficient how much it is needed to slowdown base speed. 1.0 means delay to get exact base speed.
     * @return sleep time in nanoseconds.
     */
    long calcDelayTime(long baseSpeed, int nThreads, int coefficient) {
        if (coefficient <= 0)
            throw new IllegalStateException("Coefficient should be positive");

        if (baseSpeed <= 0)
            return 0;

        long updTimeNsForOnePage = TimeUnit.SECONDS.toNanos(1) * nThreads / (baseSpeed);

        return coefficient * updTimeNsForOnePage;
    }

    /**
     * @param cpWrittenPages  current counter of written pages.
     * @param dirtyPagesRatio current percent of dirty pages.
     */
    private void detectCpPagesWriteStart(int cpWrittenPages, double dirtyPagesRatio) {
        if (cpWrittenPages > 0 && lastObservedWritten.compareAndSet(0, cpWrittenPages)) {
            double newMinRatio = dirtyPagesRatio;

            if (newMinRatio < MIN_RATIO_NO_THROTTLE)
                newMinRatio = MIN_RATIO_NO_THROTTLE;

            if (newMinRatio > 1)
                newMinRatio = 1;

            //for slow cp is completed now, drop previous dirty page percent
            initDirtyRatioAtCpBegin = newMinRatio;
        }
    }

    /***/
    void reset() {
        speedCpWrite.setCounter(0L, System.nanoTime());
        initDirtyRatioAtCpBegin = MIN_RATIO_NO_THROTTLE;
        lastObservedWritten.set(0);
    }

    void close() {
        speedCpWrite.finishInterval();
        threadIds.clear();
    }

    /**
     * Throttling mode for page.
     */
    private enum ThrottleMode {
        /**
         * No delay is applied.
         */
        NO,

        /**
         * Limited, time is based on target speed.
         */
        LIMITED
    }
}
