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

package org.apache.ignite.internal.processors.cache.checker.processor;

import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCachePartitionExchangeManager;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.checker.objects.CachePartitionRequest;
import org.apache.ignite.internal.processors.cache.checker.objects.ExecutionResult;
import org.apache.ignite.internal.processors.cache.checker.util.DelayedHolder;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.IgniteSystemProperties.getLong;
import static org.apache.ignite.internal.processors.cache.checker.processor.ReconciliationEventListener.WorkLoadStage.BEFORE_PROCESSING;
import static org.apache.ignite.internal.processors.cache.checker.processor.ReconciliationEventListener.WorkLoadStage.FINISHED;
import static org.apache.ignite.internal.processors.cache.checker.processor.ReconciliationEventListener.WorkLoadStage.READY;
import static org.apache.ignite.internal.processors.cache.checker.processor.ReconciliationEventListener.WorkLoadStage.SCHEDULED;
import static org.apache.ignite.internal.processors.cache.checker.processor.ReconciliationEventListener.WorkLoadStage.SKIPPED;

/**
 * Abstraction for the control unit of work.
 */
public class AbstractPipelineProcessor {
    /** Work progress print interval. The default values is 1 min. */
    protected final long workProgressPrintIntervalSec = getLong("RECONCILIATION_WORK_PROGRESS_PRINT_INTERVAL_SEC", 60);

    /** Session identifier that allows identifying particular data flow and workload. */
    protected final long sesId;

    /** Queue. */
    private final BlockingQueue<DelayedHolder<? extends PipelineWorkload>> queue = new DelayQueue<>();

    /** High priority queue. */
    private final BlockingQueue<DelayedHolder<? extends PipelineWorkload>> highPriorityQueue = new LinkedBlockingQueue<>();

    /** Maintains a number of workloads that can be handled simultaneously. */
    private final Semaphore liveListeners;

    /** Maximum number of workloads that can be handled simultaneously. */
    protected final int parallelismLevel;

    /** Latest affinity changed topology version that was available at the processor initialization. */
    protected final AffinityTopologyVersion startTopVer;

    /** Context. */
    protected final GridKernalContext ctx;

    /** Event listener that allows to track the execution of workload. */
    protected volatile ReconciliationEventListener evtLsnr = ReconciliationEventListenerProvider.defaultListenerInstance();

    /** Error. */
    protected final AtomicReference<String> error = new AtomicReference<>();

    /** Ignite instance. */
    protected final IgniteEx ignite;

    /** Exchange manager. */
    private final GridCachePartitionExchangeManager<Object, Object> exchMgr;

    /** Ignite logger. */
    protected final IgniteLogger log;

    /**
     * Creates a new pipeline processor.
     *
     * @param sesId Session identifier that allows to identify different runs of the utility.
     * @param ignite Local Ignite instance to be used as an entry point for the execution of the utility.
     * @param parallelismLevel Number of batches that can be handled simultaneously.
     */
    public AbstractPipelineProcessor(
        long sesId,
        IgniteEx ignite,
        int parallelismLevel
    ) throws IgniteCheckedException {
        this.sesId = sesId;
        this.ctx = ignite.context();
        this.exchMgr = ignite.context().cache().context().exchange();
        this.startTopVer = exchMgr.lastAffinityChangedTopologyVersion(exchMgr.lastTopologyFuture().get());
        this.parallelismLevel = parallelismLevel;
        this.liveListeners = new Semaphore(parallelismLevel);
        this.ignite = ignite;
        this.log = ignite.log().getLogger(getClass());
    }

    /**
     * Register event listener.
     */
    public void registerListener(ReconciliationEventListener evtLsnr) {
        this.evtLsnr = evtLsnr;
    }

    /**
     * @return Returns session identifier.
     */
    public long sessionId() {
        return sesId;
    }

    /**
     * @return true if current topology version isn't equal start topology.
     */
    protected boolean topologyChanged() throws IgniteCheckedException {
        AffinityTopologyVersion currVer = exchMgr.lastAffinityChangedTopologyVersion(exchMgr.lastTopologyFuture().get());

        return !startTopVer.equals(currVer);
    }

    /**
     * @return true if other session exist or interrupted.
     */
    protected boolean isSessionExpired() {
        return ignite.context().diagnostic().reconciliationExecutionContext().sessionId() != sesId;
    }

    /**
     * @return true if some of job register an error.
     */
    protected boolean isInterrupted() {
        return error.get() != null;
    }

    /**
     * @return count of live listener.
     */
    protected boolean hasLiveHandlers() {
        return parallelismLevel != liveListeners.availablePermits();
    }

    /**
     * Wait to finish of mission-critical jobs before stopping.
     */
    protected void waitWorkFinish() {
        while (hasLiveHandlers()) {
            try {
                Thread.sleep(100);
            }
            catch (InterruptedException ignore) {
                // No-op
            }
        }
    }

    /**
     * @return true if tasks for processing doesn't exist.
     */
    protected boolean isEmpty() {
        return highPriorityQueue.isEmpty() && queue.isEmpty();
    }

    /**
     * @return {@link PipelineWorkload} from queue of tasks.
     */
    protected PipelineWorkload takeTask() throws InterruptedException {
        return !highPriorityQueue.isEmpty() ? highPriorityQueue.take().getTask() : queue.take().getTask();
    }

    /**
     * @param timeout how long to wait before giving up, in units of unit
     * @param unit a TimeUnit determining how to interpret the timeout parameter
     * @return {@link PipelineWorkload} from queue of tasks or null if the specified waiting time elapses before an element is available.
     */
    protected PipelineWorkload pollTask(long timeout, TimeUnit unit) throws InterruptedException {
        if (!highPriorityQueue.isEmpty()) {
            DelayedHolder<? extends PipelineWorkload> holder = highPriorityQueue.poll(timeout, unit);
            if (holder != null)
                return holder.getTask();
        }
        else {
            DelayedHolder<? extends PipelineWorkload> holder = queue.poll(timeout, unit);
            if (holder != null)
                return holder.getTask();
        }

        return null;
    }

    /**
     * Executes the given task.
     *
     * @param taskCls Task class.
     * @param workload Argument.
     * @param lsnr Listener.
     * @throws InterruptedException If task was interrupted.
     * @throws IgniteException If failed to execute the task.
     */
    protected <T extends CachePartitionRequest, R> void compute(
        Class<? extends ComputeTask<T, ExecutionResult<R>>> taskCls,
        T workload,
        IgniteInClosure<? super R> lsnr
    ) throws InterruptedException, IgniteException {
        boolean acquired = false;

        while (!acquired) {
            acquired = liveListeners.tryAcquire(workProgressPrintIntervalSec / 5, SECONDS);

            if (!acquired)
                printStatistics();
        }

        ClusterGroup grp;

        try {
            grp = partOwners(workload.cacheName(), workload.partitionId());
        }
        catch (Exception e) {
            liveListeners.release();

            throw new IgniteException("Cannot calculate partition owners [cacheName="
                + workload.cacheName() + ", part=" + workload.partitionId() + ']', e);
        }

        if (grp == null) {
            // There are no owners for this partition.
            evtLsnr.onEvent(SKIPPED, workload);

            evtLsnr.onEvent(FINISHED, workload);

            liveListeners.release();

            return;
        }

        evtLsnr.onEvent(BEFORE_PROCESSING, workload);

        ignite.compute(grp).executeAsync(taskCls, workload).listen(fut -> {
            try {
                ExecutionResult<R> res;

                try {
                    res = fut.get();
                }
                catch (RuntimeException e) {
                    log.error("Failed to execute the task " + taskCls.getName(), e);

                    error.compareAndSet(null, e.getMessage());

                    return;
                }

                if (res.errorMessage() != null) {
                    error.compareAndSet(null, res.errorMessage());

                    return;
                }

                evtLsnr.onEvent(READY, workload);

                lsnr.apply(res.result());

                evtLsnr.onEvent(FINISHED, workload);
            }
            finally {
                liveListeners.release();
            }
        });
    }

    /**
     * Send a task object for immediate processing.
     *
     * @param task Task object.
     */
    protected void schedule(PipelineWorkload task) {
        schedule(task, 0, MILLISECONDS);
    }

    /**
     * Schedules with minimal finish time -1;
     */
    protected void scheduleHighPriority(PipelineWorkload task) {
        evtLsnr.onEvent(SCHEDULED, task);

        highPriorityQueue.offer(new DelayedHolder<>(-1, task));
    }

    /**
     * Send a task object which will available after time.
     *
     * @param task Task object.
     * @param duration Wait time.
     * @param timeUnit Time unit.
     */
    protected void schedule(PipelineWorkload task, long duration, TimeUnit timeUnit) {
        long finishTime = U.currentTimeMillis() + timeUnit.toMillis(duration);

        evtLsnr.onEvent(SCHEDULED, task);

        queue.offer(new DelayedHolder<>(finishTime, task));
    }

    /**
     * Print statistics.
     */
    protected void printStatistics() {
        // No-op.
    }

    /**
     * Returns a cluster group represents a set od nodes that own the given partition.
     * Returned group can be {@code null} in case of there are no owners for the given partition.
     *
     * @throws IgniteException If failed to get partition owners.
     * @return Cluster group of owners.
     */
    private ClusterGroup partOwners(String cacheName, int partId) throws IgniteException {
        IgniteInternalCache<?, ?> cachex = ignite.cachex(cacheName);

        if (cachex == null)
            throw new IgniteException("Cache not found (was stopped) [name=" + cacheName + ']');

        Collection<ClusterNode> nodes = cachex.context().topology().owners(partId, startTopVer);

        return nodes.isEmpty() ? null : ignite.cluster().forNodes(nodes);
    }
}
