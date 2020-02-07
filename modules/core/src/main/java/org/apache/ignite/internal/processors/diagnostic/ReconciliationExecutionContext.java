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

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJobContinuation;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.processors.closure.GridClosureProcessor;

/**
 *
 */
public class ReconciliationExecutionContext {
    /** Maximum number of stored sessions. */
    private static final int MAX_SESSIONS = 10;

    /**Kernal context. We need only closure processor from it, but it's not set at the moment of initialization. */
    private final GridKernalContext kernalCtx;

    private final Map<Long, Integer> runningJobsLimit = new LinkedHashMap<>();

    private final Map<Long, Integer> runningJobsCnt = new LinkedHashMap<>();;

    private final Map<Long, Queue<ComputeJobContinuation>> pendingJobs = new LinkedHashMap<>();

    /** Id of last or current reconciliation session. */
    private long sessionId;

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
        return sessionId;
    }

    /**
     * Registers new partitions reconciliation session.
     *
     * @param sessionId Session ID.
     * @param parallelism Parallelism level.
     */
    public synchronized void registerSession(long sessionId, int parallelism) {
        this.sessionId = sessionId;

        runningJobsCnt.put(sessionId, 0);

        runningJobsLimit.put(sessionId, parallelism);

        pendingJobs.put(sessionId, new LinkedList<>());

        if (runningJobsCnt.size() == MAX_SESSIONS + 1) {
            runningJobsCnt.entrySet().iterator().remove();

            runningJobsLimit.entrySet().iterator().remove();

            pendingJobs.entrySet().iterator().remove();
        }


    }

    /**
     * Acquires permit for the job execution or holds its execution if permit is not available.
     *
     * @param sessionId Session ID.
     * @param jobCont   Job context.
     * @return <code>true</code> if the permit has been granted or
     * <code>false</code> if the job execution should be suspended.
     */
    public synchronized boolean acquireJobPermitOrHold(long sessionId, ComputeJobContinuation jobCont) {
        int limit = runningJobsLimit.get(sessionId);

        int running = runningJobsCnt.get(sessionId);

        if (running < limit) {
            runningJobsCnt.put(sessionId, running + 1);

            return true;
        }
        else {
            jobCont.holdcc();

            Queue<ComputeJobContinuation> jobsQueue = pendingJobs.get(sessionId);

            jobsQueue.add(jobCont);

            return false;
        }
    }

    /**
     * Releases execution permit and triggers continuation of a pending job if there are any.
     *
     * @param sessionId Session ID.
     */
    public synchronized void releaseJobPermit(long sessionId) {
        int running = runningJobsCnt.get(sessionId);

        Queue<ComputeJobContinuation> jobsQueue = pendingJobs.get(sessionId);

        ComputeJobContinuation pendingJob = jobsQueue.poll();

        runningJobsCnt.put(sessionId, running - 1);

        if (pendingJob != null) {
            try {
                kernalCtx.closure().runLocal(pendingJob::callcc, GridIoPolicy.MANAGEMENT_POOL);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }
    }
}
