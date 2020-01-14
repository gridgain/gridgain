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
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeTask;
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
 *
 */
public class AbstractPipelineProcessor {
    /**
     *
     */
    private final BlockingQueue<DelayedHolder<? extends PipelineWorkload>> queue = new DelayQueue<>();

    /** High priority queue. */
    private final BlockingQueue<DelayedHolder<? extends PipelineWorkload>> highPriorityQueue = new LinkedBlockingQueue<>();

    /**
     *
     */
    private final GridCachePartitionExchangeManager<Object, Object> exchMgr;

    /**
     *
     */
    private final Semaphore liveListeners;

    /**
     *
     */
    protected final int parallelismLevel;

    /**
     *
     */
    protected final AffinityTopologyVersion startTopVer;

    /** Error. */
    protected final AtomicReference<String> error = new AtomicReference<>();

    /** Ignite instance. */
    protected final IgniteEx ignite;

    /**
     *
     */
    protected final long sesId;

    /**
     *
     */
    protected volatile ReconciliationEventListener eventListener = ReconciliationEventListenerFactory.create();

    /**
     *
     */
    public AbstractPipelineProcessor(
        long sesId,
        IgniteEx ignite,
        GridCachePartitionExchangeManager<Object, Object> exchMgr,
        int parallelismLevel
    ) throws IgniteCheckedException {
        this.sesId = sesId;
        this.exchMgr = exchMgr;
        this.startTopVer = exchMgr.lastAffinityChangedTopologyVersion(exchMgr.lastTopologyFuture().get());
        this.parallelismLevel = parallelismLevel;
        this.liveListeners = new Semaphore(parallelismLevel);
        this.ignite = ignite;
    }

    /**
     *
     */
    public void registerListener(ReconciliationEventListener evtLsnr) {
        this.eventListener = evtLsnr;
    }

    /**
     *
     */
    protected boolean topologyChanged() throws IgniteCheckedException {
        AffinityTopologyVersion currVer = exchMgr.lastAffinityChangedTopologyVersion(exchMgr.lastTopologyFuture().get());

        return !startTopVer.equals(currVer);
    }

    /**
     *
     */
    protected boolean isSessionExpired() {
        return ignite.context().diagnostic().getReconciliationSessionId() != sesId;
    }

    /**
     *
     */
    protected boolean isInterrupted() {
        return error.get() != null;
    }

    /**
     *
     */
    protected boolean hasLiveHandlers() {
        return parallelismLevel != liveListeners.availablePermits();
    }

    /**
     *
     */
    protected void waitWorkFinish() {
        while (hasLiveHandlers()) {
            try {
                Thread.sleep(300);
            }
            catch (InterruptedException ignore) {
            }
        }
    }

    /**
     *
     */
    protected boolean isEmpty() {
        return highPriorityQueue.isEmpty() && queue.isEmpty();
    }

    /**
     *
     */
    protected PipelineWorkload takeTask() throws InterruptedException {
        if (!highPriorityQueue.isEmpty())
            return highPriorityQueue.take().getTask();
        else
            return queue.take().getTask();
    }

    /**
     * @param taskCls Task class.
     * @param arg Argument.
     * @param lsnr Listener.
     */
    protected <T extends CachePartitionRequest, R> void compute(
        Class<? extends ComputeTask<T, ExecutionResult<R>>> taskCls, T arg,
        IgniteInClosure<? super R> lsnr) throws InterruptedException {
        liveListeners.acquire();

        ignite.compute(partOwners(arg.cacheName(), arg.partitionId())).executeAsync(taskCls, arg).listen(futRes -> {
            try {
                ExecutionResult<R> res = futRes.get();

                if (res.getErrorMessage() != null) {
                    error.compareAndSet(null, res.getErrorMessage());

                    return;
                }

                eventListener.registerEvent(STARTING, arg);

                lsnr.apply(res.getResult());

                eventListener.registerEvent(FINISHING, arg);
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
     *
     */
    protected void scheduleHighPriority(PipelineWorkload task) {
        try {
            eventListener.registerEvent(PLANNED, task);

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

            eventListener.registerEvent(PLANNED, task);

            queue.put(new DelayedHolder<>(finishTime, task));
        }
        catch (InterruptedException e) { // This queue unbounded as result the exception isn't reachable.
            throw new IgniteException(e);
        }
    }

    /**
     *
     */
    private ClusterGroup partOwners(String cacheName, int partId) {
        Collection<ClusterNode> nodes = ignite.cachex(cacheName).context().topology().owners(partId, startTopVer);

        return ignite.cluster().forNodeIds(nodes.stream().map(ClusterNode::id).collect(toList()));
    }
}
