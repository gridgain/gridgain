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

package org.apache.ignite.internal.processors.cache.distributed.dht.topology;

import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.CacheMetricsImpl;
import org.apache.ignite.internal.processors.cache.CacheStoppedException;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;
import org.jetbrains.annotations.Nullable;

import static java.util.Objects.nonNull;
import static org.apache.ignite.IgniteSystemProperties.getLong;
import static org.apache.ignite.failure.FailureType.SYSTEM_WORKER_TERMINATION;

/**
 * Class that serves asynchronous partition clearing process.
 * Partitions clearing can be scheduled for following reasons:
 *
 * <ul>
 *     <li>The local node is no longer an owner for a partition (partition is evicted) </li>
 *     <li>The partition should be cleared before rebalancing to avoid desync, because supplying node
 *     not guaranties having history for all required keys.</li>
 *     <li>The partition tombstones must be cleaned.</li>
 * </ul>
 */
public class PartitionsEvictManager extends GridCacheSharedManagerAdapter {
    /** Default eviction progress show frequency. */
    private static final int DEFAULT_SHOW_EVICTION_PROGRESS_FREQ_MS = 2 * 60 * 1000;

    /** Eviction progress frequency property name. */
    private static final String SHOW_EVICTION_PROGRESS_FREQ = "SHOW_EVICTION_PROGRESS_FREQ";

    /** Eviction progress frequency in ms. */
    private final long evictionProgressFreqMs =
        getLong(SHOW_EVICTION_PROGRESS_FREQ, DEFAULT_SHOW_EVICTION_PROGRESS_FREQ_MS);

    /** Last time of show eviction progress. */
    private long lastShowProgressTimeNanos = System.nanoTime() - U.millisToNanos(evictionProgressFreqMs);

    /** */
    private final Map<Integer, GroupEvictionContext> evictionGroupsMap = new ConcurrentHashMap<>();

    /**
     * Evicted partitions for printing to log. Should be updated holding a lock on {@link #mux}.
     */
    private final Map<Integer, Map<Integer, EvictReason>> logEvictPartByGrps = new HashMap<>();

    /** Lock object. */
    private final Object mux = new Object();

    /** The executor for clearing jobs. */
    private volatile IgniteThreadPoolExecutor executor;

    /** */
    private final ConcurrentMap<PartitionKey, PartitionEvictionTask> futs = new ConcurrentHashMap<>();

    /**
     * Callback on cache group start.
     *
     * @param grp Group.
     */
    public void onCacheGroupStarted(CacheGroupContext grp) {
        if (!grp.isLocal())
            evictionGroupsMap.put(grp.groupId(), new GroupEvictionContext(grp));
    }

    /**
     * Stops eviction process for group.
     *
     * Method awaits last offered partition eviction.
     *
     * @param grp Group context.
     */
    public void onCacheGroupStopped(CacheGroupContext grp) {
        // Must keep context in the map to avoid race with subsequent clearing request after the call to this method.
        GroupEvictionContext grpEvictionCtx =
            evictionGroupsMap.computeIfAbsent(grp.groupId(), p -> new GroupEvictionContext(grp));

        grpEvictionCtx.stop(new CacheStoppedException(grp.cacheOrGroupName()));
    }

    /**
     * @param grp Group context.
     * @param part Partition.
     */
    public PartitionEvictionTask clearTombstonesAsync(CacheGroupContext grp, GridDhtLocalPartition part) {
        assert grp.supportsTombstone() : grp;

        PartitionEvictionTask task = scheduleEviction(grp, part, EvictReason.TOMBSTONE);

        task.start();

        return task;
    }

    /**
     * Schedules partition for clearing.
     * <p>
     * If the partition is currently clearing, synchronously cancels this process.
     * <p>
     * To start actual clearing call start on returned task object.
     *
     * @param grp Group context.
     * @param part Partition to evict.
     * @param reason Evict reason.
     *
     * @return A scheduled task.
     */
    public PartitionEvictionTask scheduleEviction(
        CacheGroupContext grp,
        GridDhtLocalPartition part,
        EvictReason reason
    ) {
        assert nonNull(grp);
        assert nonNull(part);

        int grpId = grp.groupId();

        GroupEvictionContext grpEvictionCtx = evictionGroupsMap.computeIfAbsent(
            grpId, k -> new GroupEvictionContext(grp));

        // Register new task, cancelling previous if presents.
        PartitionKey key = new PartitionKey(grp.groupId(), part.id());
        GridFutureAdapter<Void> finishFut = new GridFutureAdapter<>();
        PartitionEvictionTask task = new PartitionEvictionTask(part, grpEvictionCtx, reason, finishFut);

        finishFut.listen(fut -> futs.remove(key));

        while (true) {
            if (grp.cacheObjectContext().kernalContext().isStopping()) {
                finishFut.onDone(new NodeStoppingException("Node is stopping"));

                return task;
            }

            PartitionEvictionTask prev = futs.putIfAbsent(key, task);

            if (prev == null) {
                if (log.isDebugEnabled())
                    log.debug("Enqueued partition clearing [grp=" + grp.cacheOrGroupName()
                        + ", task=" + task + ']');

                break;
            }
            else {
                if (log.isDebugEnabled()) {
                    log.debug("Cancelling the clearing [grp=" + grp.cacheOrGroupName()
                        + ", topVer=" + (grp.topology().initialized() ? grp.topology().readyTopologyVersion() : "NA")
                        + ", task=" + task
                        + ", prev=" + prev
                        + ']');
                }

                prev.cancel();
                prev.awaitCompletion();
            }
        }

        // Try eviction fast-path.
        if (part.state() == GridDhtPartitionState.EVICTED && reason == EvictReason.EVICTION) {
            finishFut.onDone();

            return task;
        }

        if (cctx.cache().cacheGroup(grpId) == null) {
            finishFut.onDone(new CacheStoppedException(grp.cacheOrGroupName()));

            return task;
        }

        if (log.isDebugEnabled())
            log.debug("The partition has been scheduled for clearing [grp=" + grp.cacheOrGroupName()
                + ", topVer=" + (grp.topology().initialized() ? grp.topology().readyTopologyVersion() : "NA")
                + ", task" + task + ']');

        return task;
    }

    /**
     * @param grpId Group id.
     * @param partId Partition id.
     */
    public @Nullable PartitionEvictionTask clearingTask(int grpId, int partId) {
        return futs.get(new PartitionKey(grpId, partId));
    }

    /**
     * Shows progress of eviction.
     */
    private void showProgress() {
        if (U.millisSinceNanos(lastShowProgressTimeNanos) >= evictionProgressFreqMs) {
            int size = executor.getQueue().size();

            if (log.isInfoEnabled()) {
                log.info("Eviction in progress [groups=" + evictionGroupsMap.keySet().size() +
                    ", remainingPartsToEvict=" + size + ']');

                evictionGroupsMap.values().forEach(GroupEvictionContext::showProgress);

                if (!logEvictPartByGrps.isEmpty()) {
                    StringJoiner evictPartJoiner = new StringJoiner(", ");

                    logEvictPartByGrps.forEach((grpId, map) -> {
                        CacheGroupContext grpCtx = cctx.cache().cacheGroup(grpId);

                        String grpName = (nonNull(grpCtx) ? grpCtx.cacheOrGroupName() : null);

                        evictPartJoiner.add("[grpId=" + grpId + ", grpName=" + grpName + ", " + toString(map) + ']');
                    });

                    log.info("Partitions have been scheduled for eviction: " + evictPartJoiner);

                    logEvictPartByGrps.clear();
                }
            }

            lastShowProgressTimeNanos = System.nanoTime();
        }
    }

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        super.start0();

        executor = (IgniteThreadPoolExecutor) cctx.kernalContext().getRebalanceExecutorService();
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStop0(boolean cancel) {
        super.onKernalStop0(cancel);

        Collection<GroupEvictionContext> evictionGrps = evictionGroupsMap.values();

        NodeStoppingException ex = new NodeStoppingException("Node is stopping");

        // Ignore cancel flag for group eviction because it may take a while.
        for (GroupEvictionContext evictionGrp : evictionGrps)
            evictionGrp.stop(ex);

        executor = null;
    }

    /**
     * Creating a group partitions for reasons of eviction as a string.
     *
     * @param evictParts Partitions with a reason for eviction.
     * @return String with group partitions for reasons of eviction.
     */
    private String toString(Map<Integer, EvictReason> evictParts) {
        assert nonNull(evictParts);

        Map<EvictReason, Collection<Integer>> partByReason = new EnumMap<>(EvictReason.class);

        for (Entry<Integer, EvictReason> entry : evictParts.entrySet())
            partByReason.computeIfAbsent(entry.getValue(), b -> new ArrayList<>()).add(entry.getKey());

        StringJoiner joiner = new StringJoiner(", ");

        partByReason.forEach((reason, partIds) -> joiner.add(reason.toString() + '=' + S.compact(partIds)));

        return joiner.toString();
    }

    /**
     * Cleans up group eviction context when it's safe.
     *
     * @param grpId Group id.
     */
    public void cleanupRemovedGroup(int grpId) {
        evictionGroupsMap.remove(grpId);
    }

    /**
     *
     */
    private class GroupEvictionContext {
        /** */
        private final CacheGroupContext grp;

        /** Stop exception. */
        private AtomicReference<Exception> stopExRef = new AtomicReference<>();

        /** Total partition to evict. Can be replaced by the metric counters. */
        private AtomicInteger totalTasks = new AtomicInteger();

        /** Total partition evicts in progress. */
        private int taskInProgress;

        /** */
        private ReadWriteLock busyLock = new ReentrantReadWriteLock();

        /**
         * @param grp Group context.
         */
        private GroupEvictionContext(CacheGroupContext grp) {
            this.grp = grp;
        }

        /**
         *
         * @param task Partition eviction task.
         */
        private synchronized void taskScheduled(PartitionEvictionTask task) {
            taskInProgress++;

            GridFutureAdapter<?> fut = task.finishFut;

            fut.listen(f -> {
                synchronized (this) {
                    taskInProgress--;

                    totalTasks.decrementAndGet();

                    updateMetrics(task.grpEvictionCtx.grp, task.reason, DECREMENT);
                }
            });
        }

        /** */
        public boolean shouldStop() {
            return stopExRef.get() != null;
        }

        /**
         * @param ex Stop exception.
         */
        @SuppressWarnings("LockAcquiredButNotSafelyReleased")
        void stop(Exception ex) {
            // Prevent concurrent stop.
            if (!stopExRef.compareAndSet(null, ex))
                return;

            busyLock.writeLock().lock();
        }

        /**
         * Shows progress group of eviction.
         */
        private void showProgress() {
            if (log.isInfoEnabled())
                log.info("Group eviction in progress [grpName=" + grp.cacheOrGroupName() +
                    ", grpId=" + grp.groupId() +
                    ", remainingPartsToEvict=" + (totalTasks.get() - taskInProgress) +
                    ", partsEvictInProgress=" + taskInProgress +
                    ", totalParts=" + grp.topology().localPartitions().size() + "]");
        }
    }

    /**
     * @return The number of executing + waiting in the queue tasks.
     */
    public int total() {
        return evictionGroupsMap.values().stream().mapToInt(ctx -> ctx.totalTasks.get()).sum();
    }

    /**
     * Cancellable task for partition clearing.
     */
    public class PartitionEvictionTask implements Runnable {
        /** Partition to evict. */
        private final GridDhtLocalPartition part;

        /** Reason for eviction. */
        private final EvictReason reason;

        /** Eviction context. */
        @GridToStringExclude
        private final GroupEvictionContext grpEvictionCtx;

        /** */
        @GridToStringExclude
        private final GridFutureAdapter<Void> finishFut;

        /** */
        @GridToStringExclude
        private final AtomicReference<Boolean> state = new AtomicReference<>(null);

        /**
         * @param part Partition.
         * @param grpEvictionCtx Eviction context.
         * @param reason Reason for eviction.
         * @param finishFut Finish future.
         */
        private PartitionEvictionTask(
            GridDhtLocalPartition part,
            GroupEvictionContext grpEvictionCtx,
            EvictReason reason,
            GridFutureAdapter<Void> finishFut
        ) {
            this.part = part;
            this.grpEvictionCtx = grpEvictionCtx;
            this.reason = reason;
            this.finishFut = finishFut;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            if (grpEvictionCtx.grp.cacheObjectContext().kernalContext().isStopping()) {
                finishFut.onDone(new NodeStoppingException("Node is stopping"));

                return;
            }

            if (!grpEvictionCtx.busyLock.readLock().tryLock()) {
                finishFut.onDone(grpEvictionCtx.stopExRef.get());

                return;
            }

            BooleanSupplier stopClo = () -> grpEvictionCtx.shouldStop() || (state.get() == Boolean.FALSE);

            try {
                long clearedEntities = part.clearAll(stopClo, this);

                if (log.isDebugEnabled()) {
                    log.debug("The partition clearing has been finished [grp=" + part.group().cacheOrGroupName() +
                        ", topVer=" + part.group().topology().readyTopologyVersion() +
                        ", cleared=" + clearedEntities +
                        ", task" + this + ']');
                }

                if (cctx.kernalContext().isStopping())
                    finishFut.onDone(new NodeStoppingException("Node is stopping"));
                else
                    finishFut.onDone();
            }
            catch (Throwable ex) {
                finishFut.onDone(ex);

                if (cctx.kernalContext().isStopping()) {
                    LT.warn(log, ex, "Partition eviction has been cancelled (local node is stopping) " +
                        "[grp=" + grpEvictionCtx.grp.cacheOrGroupName() +
                        ", readyVer=" + grpEvictionCtx.grp.topology().readyTopologyVersion() + ']',
                        false,
                        true);
                }
                else {
                    LT.error(log, ex, "Partition eviction has failed [grp=" +
                        grpEvictionCtx.grp.cacheOrGroupName() + ", part=" + part.id() + ']');

                    cctx.kernalContext().failure().process(new FailureContext(SYSTEM_WORKER_TERMINATION, ex));
                }
            }
            finally {
                grpEvictionCtx.busyLock.readLock().unlock();
            }
        }

        /**
         * @return Eviction reason.
         */
        public EvictReason reason() {
            return reason;
        }

        /**
         * @return Finish future.
         */
        public IgniteInternalFuture<Void> finishFuture() {
            return finishFut;
        }

        /**
         * Submits the task for execution.
         */
        public boolean start() {
            if (!state.compareAndSet(null, Boolean.TRUE))
                return false;

            executor.submit(this);

            synchronized (mux) {
                logEvictPartByGrps.computeIfAbsent(grpEvictionCtx.grp.groupId(),
                    grpId -> new HashMap<>()).put(part.id(), reason);

                grpEvictionCtx.totalTasks.incrementAndGet();

                updateMetrics(grpEvictionCtx.grp, reason, INCREMENT);

                showProgress();

                grpEvictionCtx.taskScheduled(this);
            }

            if (log.isDebugEnabled())
                log.debug("Starting clearing [grp=" + grpEvictionCtx.grp.cacheOrGroupName()
                    + ", topVer=" + grpEvictionCtx.grp.topology().readyTopologyVersion()
                    + ", task" + this + ']');

            return true;
        }

        /**
         * Signals this eviction task to stop.
         */
        public void cancel() {
            if (state.compareAndSet(null, Boolean.FALSE))
                finishFut.onDone(); // Cancelled before start.
            else if (state.get() == Boolean.TRUE)
                state.set(Boolean.FALSE); // Cancelled while running, need to publish stop request.
        }

        /** */
        public void awaitCompletion() {
            while (true) {
                try {
                    finishFut.get(5_000);

                    return;
                }
                catch (IgniteFutureTimeoutCheckedException e) {
                    log.warning("Failed to wait for clearing finish, retrying [task=" + this + ']');
                }
                catch (IgniteCheckedException e) {
                    log.warning("The clearing has finished with error [part=" + part + ']', e);

                    return;
                }
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(PartitionEvictionTask.class, this,
                "grp", grpEvictionCtx.grp.cacheOrGroupName(),
                "reason", reason,
                "state", state.get() == null ? "NotStarted" : state.get() ? "Started" : "Cancelled",
                "done", finishFut.isDone(), "err", finishFut.error() != null);
        }
    }

    /**
     * Reason for eviction of partition.
     */
    public enum EvictReason {
        /**
         * Partition evicted after changing to
         * {@link GridDhtPartitionState#RENTING RENTING} state.
         */
        EVICTION,

        /**
         * Partition evicted after changing to
         * {@link GridDhtPartitionState#MOVING MOVING} state.
         */
        CLEARING,

        /** Partition tombstones must be cleaned. */
        TOMBSTONE;
    }

    /**
     * @param grp Cache group.
     * @param c Update closure.
     */
    private void updateMetrics(CacheGroupContext grp, EvictReason reason, BiConsumer<EvictReason, CacheMetricsImpl> c) {
        for (GridCacheContext cctx : grp.caches()) {
            if (cctx.statisticsEnabled()) {
                final CacheMetricsImpl metrics = cctx.cache().metrics0();

                c.accept(reason, metrics);
            }
        }
    }

    /** Increment closure. */
    private static final BiConsumer<EvictReason, CacheMetricsImpl> INCREMENT = new BiConsumer<EvictReason, CacheMetricsImpl>() {
        @Override public void accept(EvictReason reason, CacheMetricsImpl cacheMetrics) {
            if (reason == EvictReason.CLEARING)
                cacheMetrics.incrementRebalanceClearingPartitions();
            else
                cacheMetrics.incrementEvictingPartitions();
        }
    };

    /** Decrement closure. */
    private static final BiConsumer<EvictReason, CacheMetricsImpl> DECREMENT = new BiConsumer<EvictReason, CacheMetricsImpl>() {
        @Override public void accept(EvictReason reason, CacheMetricsImpl cacheMetrics) {
            if (reason == EvictReason.CLEARING)
                cacheMetrics.decrementRebalanceClearingPartitions();
            else
                cacheMetrics.decrementEvictingPartitions();
        }
    };

    /** */
    private static final class PartitionKey {
        /** */
        final int grpId;

        /** */
        final int partId;

        /**
         * @param grpId Group id.
         * @param partId Partition id.
         */
        public PartitionKey(int grpId, int partId) {
            this.grpId = grpId;
            this.partId = partId;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            PartitionKey that = (PartitionKey) o;

            if (grpId != that.grpId) return false;
            return partId == that.partId;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int result = grpId;
            result = 31 * result + partId;
            return result;
        }
    }
}
