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

package org.apache.ignite.internal.processors.diagnostic;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.stream.Stream;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJobContinuation;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.util.future.GridFutureAdapter;

/**
 * Responsible for storing context of all ongoing reconciliation activities.
 */
public class ReconciliationExecutionContext {
    /** Session ID for test purposes, permits won't be checked if it's passed. */
    public static final long IGNORE_JOB_PERMITS_SESSION_ID = Long.MIN_VALUE / 7;

    /** Maximum number of stored sessions. */
    private static final int MAX_SESSIONS = 10;

    /**Kernal context. We need only closure processor from it, but it's not set at the moment of initialization. */
    private final GridKernalContext kernalCtx;

    /** Node local running jobs limit according to the parallelism level. */
    private final Map<Long/*Session ID*/, Integer> runningJobsLimit = new LinkedHashMap<>();

    /** Number of currently running reconciliation jobs on the local node. */
    private final Map<Long/*Session ID*/, Integer> runningJobsCnt = new LinkedHashMap<>();

    /** Jobs that were enqueued to be executed later due to permits exhaustion. */
    private final Map<Long/*Session ID*/, Queue<ComputeJobContinuation>> pendingJobs = new LinkedHashMap<>();

    /** Id of last or current reconciliation session. */
    private long sesId;

    /** Listener for reconciliation metrics updates. */
    private volatile GridFutureAdapter<ReconciliationStatisticsUpdateListener> statisticsListenerFut = new GridFutureAdapter<>();

    /**
     * @param kernalCtx Kernal context.
     */
    public ReconciliationExecutionContext(GridKernalContext kernalCtx) {
        this.kernalCtx = kernalCtx;
    }

    /**
     * @return Id of last or current reconciliation session.
     */
    public synchronized long sessionId() {
        return sesId;
    }

    /**
     * Registers new partitions reconciliation session.
     *
     * @param sesId Session ID.
     * @param parallelism Parallelism level.
     */
    public synchronized void registerSession(long sesId, int parallelism) {
        this.sesId = sesId;

        runningJobsCnt.put(sesId, 0);

        runningJobsLimit.put(sesId, parallelism);

        pendingJobs.put(sesId, new LinkedList<>());

        statisticsListenerFut = new GridFutureAdapter<>();

        if (runningJobsCnt.size() == MAX_SESSIONS + 1) {
            Stream.of(runningJobsCnt, runningJobsLimit, pendingJobs)
                .map(m -> m.entrySet().iterator())
                .peek(Iterator::next)
                .forEach(Iterator::remove);
        }
    }

    /**
     * Acquires permit for the job execution or holds its execution if permit is not available.
     *
     * @param sesId Session ID.
     * @param jobCont   Job context.
     * @return <code>true</code> if the permit has been granted or
     * <code>false</code> if the job execution should be suspended.
     */
    public synchronized boolean acquireJobPermitOrHold(long sesId, ComputeJobContinuation jobCont) {
        if (sesId == IGNORE_JOB_PERMITS_SESSION_ID)
            return true;

        int limit = runningJobsLimit.get(sesId);

        int running = runningJobsCnt.get(sesId);

        if (running < limit) {
            runningJobsCnt.put(sesId, running + 1);

            return true;
        }
        else {
            jobCont.holdcc();

            Queue<ComputeJobContinuation> jobsQueue = pendingJobs.get(sesId);

            jobsQueue.add(jobCont);

            return false;
        }
    }

    /**
     * Releases execution permit and triggers continuation of a pending job if there are any.
     *
     * @param sesId Session ID.
     */
    public synchronized void releaseJobPermit(long sesId) {
        if (sesId == IGNORE_JOB_PERMITS_SESSION_ID)
            return;

        int running = runningJobsCnt.get(sesId);

        Queue<ComputeJobContinuation> jobsQueue = pendingJobs.get(sesId);

        ComputeJobContinuation pendingJob = jobsQueue.poll();

        runningJobsCnt.put(sesId, running - 1);

        if (pendingJob != null) {
            try {
                kernalCtx.closure().runLocal(pendingJob::callcc, GridIoPolicy.MANAGEMENT_POOL);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }
    }

    /**
     * Updates reconcilation statistics for the gievn cache and parition.
     *
     * @param sesId Session ID.
     * @param cacheName Cache name.
     * @param partId Partition ID.
     * @param primary {@code true} if the partition is primary.
     * @param keysCnt Number of keys scanned.
     */
    public void updatePartitionStatistics(long sesId, String cacheName, int partId, boolean primary, long keysCnt) {
        statisticsListenerFut.listen(f -> {
            try {
                f.get().updateScannedPartition(sesId, cacheName, partId, primary, keysCnt);
            }
            catch (IgniteCheckedException ignore) {
                // No-op.
            }
        });
    }

    /**
     * Registers lsitener for reconciliation metrics updates.
     *
     * @param sesId Session ID.
     * @param listener Listener.
     */
    public synchronized void listenMetricsUpdates(long sesId, ReconciliationStatisticsUpdateListener listener) {
        if (sesId == this.sesId)
            statisticsListenerFut.onDone(new ReconciliationStatisticsUpdateListenerAdapter(sesId, listener));
    }

    public synchronized void removeMetricsUpdateListener(long sesId) {
        if (sesId == this.sesId) {
            GridFutureAdapter<ReconciliationStatisticsUpdateListener> fut = new GridFutureAdapter<>();
            fut.onDone(new NoopReconciliationStatisticsUpdateListener());

            statisticsListenerFut = fut;
        }
    }

    /**
     * Listener for reconciliation metrics updates.
     */
    public static interface ReconciliationStatisticsUpdateListener {
        /**
         * Updates statistics for a scanned partition.
         *
         * @param sesId Session ID.
         * @param cacheName Cache name.
         * @param partId Partition ID.
         * @param primary {@code true} if the partition is primary, {@code false} otherwise.
         * @param keysCnt Number of keys scanned in the partition.
         */
        public void updateScannedPartition(long sesId, String cacheName, int partId, boolean primary, long keysCnt);
    }

    /**
     * Adapter for {@link ReconciliationStatisticsUpdateListener} that filters updates by session ID.
     */
    private static class ReconciliationStatisticsUpdateListenerAdapter implements ReconciliationStatisticsUpdateListener {
        /** Session ID. */
        private final long sesId;

        /** Actual listner. */
        private final ReconciliationStatisticsUpdateListener listener;

        /**
         * @param sesId Session ID.
         * @param listener Actual listener.
         */
        ReconciliationStatisticsUpdateListenerAdapter(long sesId, ReconciliationStatisticsUpdateListener listener) {
            this.sesId = sesId;
            this.listener = listener;
        }

        @Override
        public void updateScannedPartition(long sesId, String cacheName, int partId, boolean primary, long keysCnt) {
            // Just ignore invalid session updates.
            if (sesId == this.sesId)
                listener.updateScannedPartition(sesId, cacheName, partId, primary, keysCnt);
        }
    }

    /**
     * No-op implementation of {@link ReconciliationStatisticsUpdateListener}.
     */
    private static class NoopReconciliationStatisticsUpdateListener implements ReconciliationStatisticsUpdateListener {
        @Override public void updateScannedPartition(long sesId, String cacheName, int partId, boolean primary, long keysCnt) {
            // No-op.
        }
    }
}
