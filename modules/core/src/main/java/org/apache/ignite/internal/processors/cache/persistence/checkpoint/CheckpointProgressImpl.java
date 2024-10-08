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

package org.apache.ignite.internal.processors.cache.persistence.checkpoint;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.processors.cache.persistence.CheckpointState;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.CheckpointPageReplacement;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotOperation;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.persistence.CheckpointState.FINISHED;
import static org.apache.ignite.internal.processors.cache.persistence.CheckpointState.LOCK_RELEASED;

/**
 * Data class representing the state of running/scheduled checkpoint.
 */
public class CheckpointProgressImpl implements CheckpointProgress {
    /** Scheduled time of checkpoint. */
    private volatile long nextCpNanos;

    /** Current checkpoint state. */
    private volatile AtomicReference<CheckpointState> state = new AtomicReference(CheckpointState.SCHEDULED);

    /** Future which would be finished when corresponds state is set. */
    private final Map<CheckpointState, GridFutureAdapter> stateFutures = new ConcurrentHashMap<>();

    /** Cause of fail, which has happened during the checkpoint or null if checkpoint was successful. */
    private volatile Throwable failCause;

    /** Flag indicates that snapshot operation will be performed after checkpoint. */
    private volatile boolean nextSnapshot;

    /** Snapshot operation that should be performed if {@link #nextSnapshot} set to true. */
    private volatile SnapshotOperation snapshotOperation;

    /** Partitions destroy queue. */
    private final PartitionDestroyQueue destroyQueue = new PartitionDestroyQueue();

    /** Wakeup reason. */
    private String reason;

    /** Counter for written checkpoint pages. Not null only if checkpoint is running. */
    private volatile AtomicInteger writtenPagesCntr;

    /** Counter for fsynced checkpoint pages. Not null only if checkpoint is running. */
    private volatile AtomicInteger syncedPagesCntr;

    /** Counter for evicted checkpoint pages. Not null only if checkpoint is running. */
    private volatile AtomicInteger evictedPagesCntr;

    /** Number of pages in current checkpoint at the beginning of checkpoint. */
    private volatile int currCheckpointPagesCnt;

    /** Assistant for synchronizing page replacement and fsync phase. */
    private final CheckpointPageReplacement checkpointPageReplacement = new CheckpointPageReplacement();

    /**
     * @param cpFreq Timeout until next checkpoint.
     */
    public CheckpointProgressImpl(long cpFreq) {
        // Avoid overflow on nextCpNanos.
        cpFreq = Math.min(TimeUnit.DAYS.toMillis(365), cpFreq);

        nextCpNanos = System.nanoTime() + U.millisToNanos(cpFreq);
    }

    /**
     * @return {@code true} If checkpoint already started but have not finished yet.
     */
    @Override public boolean inProgress() {
        return greaterOrEqualTo(LOCK_RELEASED) && !greaterOrEqualTo(FINISHED);
    }

    /**
     * @param expectedState Expected state.
     * @return {@code true} if current state equal to given state.
     */
    public boolean greaterOrEqualTo(CheckpointState expectedState) {
        return state.get().ordinal() >= expectedState.ordinal();
    }

    /**
     * @param state State for which future should be returned.
     * @return Existed or new future which corresponds to the given state.
     */
    @Override public GridFutureAdapter futureFor(CheckpointState state) {
        GridFutureAdapter stateFut = stateFutures.computeIfAbsent(state, (k) -> new GridFutureAdapter());

        if (greaterOrEqualTo(state) && !stateFut.isDone())
            stateFut.onDone(failCause);

        return stateFut;
    }

    /**
     * Mark this checkpoint execution as failed.
     *
     * @param error Causal error of fail.
     */
    @Override public void fail(Throwable error) {
        failCause = error;

        transitTo(FINISHED);
    }

    /**
     * Changing checkpoint state if order of state is correct.
     *
     * @param newState New checkpoint state.
     */
    @Override public void transitTo(@NotNull CheckpointState newState) {
        CheckpointState state = this.state.get();

        if (state.ordinal() < newState.ordinal()) {
            this.state.compareAndSet(state, newState);

            doFinishFuturesWhichLessOrEqualTo(newState);
        }
    }

    /**
     * Finishing futures with correct result in direct state order until lastState(included).
     *
     * @param lastState State until which futures should be done.
     */
    private void doFinishFuturesWhichLessOrEqualTo(@NotNull CheckpointState lastState) {
        for (CheckpointState old : CheckpointState.values()) {
            GridFutureAdapter fut = stateFutures.get(old);

            if (fut != null && !fut.isDone())
                fut.onDone(failCause);

            if (old == lastState)
                return;
        }
    }

    /**
     * @return Destroy queue.
     */
    @Override public PartitionDestroyQueue getDestroyQueue() {
        return destroyQueue;
    }

    /**
     * @return Flag indicates that snapshot operation will be performed after checkpoint.
     */
    public boolean nextSnapshot() {
        return nextSnapshot;
    }

    /**
     * @return Scheduled time of checkpoint.
     */
    public long nextCpNanos() {
        return nextCpNanos;
    }

    /**
     * @param nextCpNanos New scheduled time of checkpoint.
     */
    public void nextCpNanos(long nextCpNanos) {
        this.nextCpNanos = nextCpNanos;
    }

    /** {@inheritDoc} */
    @Override public String reason() {
        return reason;
    }

    /**
     * @param reason New wakeup reason.
     */
    public void reason(String reason) {
        this.reason = reason;
    }

    /**
     * @return Snapshot operation that should be performed if  set to true.
     */
    public SnapshotOperation snapshotOperation() {
        return snapshotOperation;
    }

    /**
     * @param snapshotOperation New snapshot operation that should be performed if  set to true.
     */
    public void snapshotOperation(SnapshotOperation snapshotOperation) {
        this.snapshotOperation = snapshotOperation;
    }

    /**
     * @param nextSnapshot New flag indicates that snapshot operation will be performed after checkpoint.
     */
    public void nextSnapshot(boolean nextSnapshot) {
        this.nextSnapshot = nextSnapshot;
    }

    /** {@inheritDoc} */
    @Override public AtomicInteger writtenPagesCounter() {
        return writtenPagesCntr;
    }

    /** {@inheritDoc} */
    @Override public void updateWrittenPages(int deltha) {
        A.ensure(deltha > 0, "param must be positive");

        writtenPagesCntr.addAndGet(deltha);
    }

    /** {@inheritDoc} */
    @Override public AtomicInteger syncedPagesCounter() {
        return syncedPagesCntr;
    }

    /** {@inheritDoc} */
    @Override public void updateSyncedPages(int deltha) {
        A.ensure(deltha > 0, "param must be positive");

        syncedPagesCntr.addAndGet(deltha);
    }

    /** {@inheritDoc} */
    @Override public AtomicInteger evictedPagesCounter() {
        return evictedPagesCntr;
    }

    /** {@inheritDoc} */
    @Override public void updateEvictedPages(int delta) {
        A.ensure(delta > 0, "param must be positive");

        if (evictedPagesCounter() != null)
            evictedPagesCounter().addAndGet(delta);
    }

    /** {@inheritDoc} */
    @Override public int currentCheckpointPagesCount() {
        return currCheckpointPagesCnt;
    }

    /** {@inheritDoc} */
    @Override public void currentCheckpointPagesCount(int num) {
        currCheckpointPagesCnt = num;
    }

    /** {@inheritDoc} */
    @Override public void initCounters(int pagesSize) {
        currCheckpointPagesCnt = pagesSize;

        writtenPagesCntr = new AtomicInteger();
        syncedPagesCntr = new AtomicInteger();
        evictedPagesCntr = new AtomicInteger();
    }

    /** {@inheritDoc} */
    @Override public void clearCounters() {
        currCheckpointPagesCnt = 0;

        writtenPagesCntr = null;
        syncedPagesCntr = null;
        evictedPagesCntr = null;
    }

    /** {@inheritDoc} */
    @Override public void onStateChanged(CheckpointState state, Runnable clo) {
        GridFutureAdapter<?> fut0 = futureFor(state);

        fut0.listen((IgniteInClosure<IgniteInternalFuture>)fut -> {
            if (fut.error() == null)
                clo.run();
        });
    }

    /**
     * Block the start of the fsync phase at a checkpoint before replacing the page.
     *
     * <p>It is expected that the method will be invoked once and after that the {@link #unblockFsyncOnPageReplacement}
     * will be invoked on the same page.</p>
     *
     * <p>It is expected that the method will not be invoked after {@link #getUnblockFsyncOnPageReplacementFuture},
     * since by the start of the fsync phase, write dirty pages at the checkpoint should be complete and no new page
     * replacements should be started.</p>
     *
     * @param pageId Page ID for which page replacement is expected to begin.
     * @see #unblockFsyncOnPageReplacement(FullPageId, Throwable)
     * @see #getUnblockFsyncOnPageReplacementFuture()
     */
    public void blockFsyncOnPageReplacement(FullPageId pageId) {
        checkpointPageReplacement.block(pageId);
    }

    /**
     * Unblocks the start of the fsync phase at a checkpoint after the page replacement is completed.
     *
     * <p>It is expected that the method will be invoked once and after the {@link #blockFsyncOnPageReplacement} for
     * same page ID.</p>
     *
     * <p>The fsync phase will only be started after page replacement has been completed for all pages for which
     * {@link #blockFsyncOnPageReplacement} was invoked before {@link #getUnblockFsyncOnPageReplacementFuture} was
     * invoked, or no page replacement occurred at all.</p>
     *
     * <p>If an error occurs on any page replacement during one checkpoint, the future from
     * {@link #getUnblockFsyncOnPageReplacementFuture} will complete with the first error.</p>
     *
     * <p>The method must be invoked even if any error occurred, so as not to hang a checkpoint.</p>
     *
     * @param pageId Page ID for which the page replacement has ended.
     * @param error Error on page replacement, {@code null} if missing.
     * @see #blockFsyncOnPageReplacement(FullPageId)
     * @see #getUnblockFsyncOnPageReplacementFuture()
     */
    public void unblockFsyncOnPageReplacement(FullPageId pageId, @Nullable Throwable error) {
        checkpointPageReplacement.unblock(pageId, error);
    }

    /**
     * Return future that will be completed successfully if all {@link #blockFsyncOnPageReplacement} are completed, either if there were
     * none, or with an error from the first {@link #unblockFsyncOnPageReplacement}.
     *
     * <p>Must be invoked before the start of the fsync phase at the checkpoint and wait for the future to complete in order to safely
     * perform the phase.</p>
     *
     * @see #blockFsyncOnPageReplacement(FullPageId)
     * @see #unblockFsyncOnPageReplacement(FullPageId, Throwable)
     */
    public CompletableFuture<Void> getUnblockFsyncOnPageReplacementFuture() {
        return checkpointPageReplacement.stopBlocking();
    }
}
