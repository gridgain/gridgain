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
import org.apache.ignite.internal.processors.cache.checker.objects.CachePartitionRequest;
import org.apache.ignite.internal.processors.cache.checker.objects.ExecutionResult;
import org.apache.ignite.internal.processors.cache.checker.util.DelayedHolder;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.processors.cache.checker.processor.ReconciliationEventListener.WorkLoadStage.FINISHING;
import static org.apache.ignite.internal.processors.cache.checker.processor.ReconciliationEventListener.WorkLoadStage.PLANNED;
import static org.apache.ignite.internal.processors.cache.checker.processor.ReconciliationEventListener.WorkLoadStage.STARTING;

/**
 * Abstraction for the control unit of work.
 */
public class AbstractPipelineProcessor {
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
    protected volatile ReconciliationEventListener evtLsnr = ReconciliationEventListenerFactory.defaultListenerInstance();

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
     * Executes the given task.
     *
     * @param taskCls Task class.
     * @param arg Argument.
     * @param lsnr Listener.
     */
    protected <T extends CachePartitionRequest, R> void compute(
        Class<? extends ComputeTask<T, ExecutionResult<R>>> taskCls,
        T arg,
        IgniteInClosure<? super R> lsnr
    ) throws InterruptedException {
        liveListeners.acquire();

        ignite.compute(partOwners(arg.cacheName(), arg.partitionId())).executeAsync(taskCls, arg).listen(futRes -> {
            try {
                ExecutionResult<R> res;

                try {
                    res = futRes.get();
                }
                catch (RuntimeException e) {
                    log.error("Failed to execute the task " + taskCls.getName(), e);

                    error.compareAndSet(null, e.getMessage());

                    return;
                }

                if (res.getErrorMessage() != null) {
                    error.compareAndSet(null, res.getErrorMessage());

                    return;
                }

                evtLsnr.registerEvent(STARTING, arg);

                lsnr.apply(res.getResult());

                evtLsnr.registerEvent(FINISHING, arg);
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
        schedule(task, 0, TimeUnit.MILLISECONDS);
    }

    /**
     * Schedule with duration -1;
     */
    protected void scheduleHighPriority(PipelineWorkload task) {
        try {
            evtLsnr.registerEvent(PLANNED, task);

            highPriorityQueue.put(new DelayedHolder<>(-1, task));
        }
        catch (InterruptedException e) { // This queue unbounded as result the exception isn't reachable.
            throw new IgniteException(e);
        }
    }

    /**
     * Send a task object which will available after time.
     *
     * @param task Task object.
     * @param duration Wait time.
     * @param timeUnit Time unit.
     */
    protected void schedule(PipelineWorkload task, long duration, TimeUnit timeUnit) {
        try {
            long finishTime = U.currentTimeMillis() + timeUnit.toMillis(duration);

            evtLsnr.registerEvent(PLANNED, task);

            queue.put(new DelayedHolder<>(finishTime, task));
        }
        catch (InterruptedException e) { // This queue unbounded as result the exception isn't reachable.
            throw new IgniteException(e);
        }
    }

    /**
     * @return Set of partition owners.
     */
    private ClusterGroup partOwners(String cacheName, int partId) {
        Collection<ClusterNode> nodes = ignite.cachex(cacheName).context().topology().owners(partId, startTopVer);

        return ignite.cluster().forNodeIds(nodes.stream().map(ClusterNode::id).collect(toList()));
    }
}
