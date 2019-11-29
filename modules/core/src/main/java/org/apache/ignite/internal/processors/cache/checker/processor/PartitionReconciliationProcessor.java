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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.GridCachePartitionExchangeManager;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.checker.objects.CachePartitionRequest;
import org.apache.ignite.internal.processors.cache.checker.objects.PartitionBatchRequest;
import org.apache.ignite.internal.processors.cache.checker.objects.PartitionReconciliationResult;
import org.apache.ignite.internal.processors.cache.checker.objects.RecheckRequest;
import org.apache.ignite.internal.processors.cache.checker.objects.VersionedValue;
import org.apache.ignite.internal.processors.cache.checker.processor.workload.Batch;
import org.apache.ignite.internal.processors.cache.checker.processor.workload.Recheck;
import org.apache.ignite.internal.processors.cache.checker.processor.workload.Repair;
import org.apache.ignite.internal.processors.cache.checker.tasks.CollectPartitionKeysByBatchTask;
import org.apache.ignite.internal.processors.cache.checker.tasks.CollectPartitionKeysByRecheckRequestTask;
import org.apache.ignite.internal.processors.cache.checker.util.DelayedHolder;
import org.apache.ignite.internal.processors.cache.verify.PartitionReconciliationDataRowMeta;
import org.apache.ignite.internal.processors.cache.verify.PartitionReconciliationKeyMeta;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.processors.cache.checker.util.ConsistencyCheckUtils.checkConflicts;

/**
 * The base point of partition reconciliation processing.
 */
public class PartitionReconciliationProcessor {
    /** Recheck delay seconds. */
    private final int RECHECK_DELAY = 10;

    /** Caches. */
    private final Collection<String> caches;

    /** If {@code true} - Partition Reconciliation&Fix: update from Primary partition. */
    private final boolean fixMode;

    /** Interval in milliseconds between running partition reconciliation jobs. */
    private final int throttlingIntervalMillis;

    /** Amount of keys to retrieve within one job. */
    private final int batchSize;

    /** Amount of potentially inconsistent keys recheck attempts. */
    private final int recheckAttempts;

    /**
     *
     */
    private final BlockingQueue<DelayedHolder<? extends PipelineWorkload>> queue = new DelayQueue<>();

    /**
     *
     */
    private final AtomicInteger liveListeners = new AtomicInteger();

    /**
     *
     */
    private final Map<String, Map<Integer, List<Map<UUID, PartitionReconciliationDataRowMeta>>>> inconsistentKeys = new HashMap<>();

    /**
     *
     */
    private final Map<String, Map<Integer, Map<KeyCacheObject, Map<UUID, GridCacheVersion>>>> fixedKeys = new HashMap<>();

    /** Ignite instance. */
    private final IgniteEx ignite;

    /**
     *
     */
    private final IgniteLogger log;

    /**
     *
     */
    private final AffinityTopologyVersion startTopVer;

    /**
     *
     */
    private final GridCachePartitionExchangeManager<Object, Object> exchMgr;

    /**
     *
     */
    public PartitionReconciliationProcessor(
        IgniteEx ignite,
        Collection<String> caches,
        boolean fixMode,
        int throttlingIntervalMillis,
        int batchSize,
        int recheckAttempts
    ) throws IgniteCheckedException {
        this.ignite = ignite;
        this.exchMgr = ignite.context().cache().context().exchange();
        this.startTopVer = exchMgr.lastAffinityChangedTopologyVersion(exchMgr.lastTopologyFuture().get());
        this.log = ignite.log().getLogger(this);
        this.caches = caches;
        this.fixMode = fixMode;
        this.throttlingIntervalMillis = throttlingIntervalMillis;
        this.batchSize = batchSize;
        this.recheckAttempts = recheckAttempts;
    }

    /**
     * @return
     * @throws IgniteException
     */
    public PartitionReconciliationResult execute() throws IgniteException {
        try {
            //TODO skip ttl caches
            for (String cache : caches) {
                int partitions = ignite.affinity(cache).partitions();

                for (int i = 0; i < partitions; i++)
                    schedule(new Batch(cache, i, null));
            }

            while (!queue.isEmpty() || liveListeners.get() != 0) {
                AffinityTopologyVersion currVer = exchMgr.lastAffinityChangedTopologyVersion(exchMgr.lastTopologyFuture().get());

                if (!startTopVer.equals(currVer)) {
                    log.warning("Topology was changed. Partition reconciliation task was stopped.");

                    return new PartitionReconciliationResult(Collections.emptyMap());
                }

                if (queue.isEmpty() && liveListeners.get() != 0) {
                    Thread.sleep(1_000);

                    continue;
                }

                PipelineWorkload workload = queue.take().getTask();

                if (workload instanceof Batch)
                    handle((Batch)workload);
                else if (workload instanceof Recheck)
                    handle((Recheck)workload);
                else if (workload instanceof Repair)
                    handle((Repair)workload);
                else
                    throw new RuntimeException("TODO Unsupported type: " + workload);
            }

            if (fixMode)
                return null;
            else
                return prepareResult();
        }
        catch (InterruptedException e) {
            log.warning("Partition reconciliation was interrupted.", e);

            return new PartitionReconciliationResult(Collections.emptyMap());
        }
        catch (IgniteCheckedException e) {
            log.error("Unexpected error.", e);

            return new PartitionReconciliationResult(Collections.emptyMap());
        }
    }

    /**
     * @param workload
     */
    private void handle(Batch workload) {
        compute(
            CollectPartitionKeysByBatchTask.class,
            new PartitionBatchRequest(workload.cacheName(), workload.partitionId(), batchSize, workload.lowerKey(), startTopVer),
            futRes -> {
                T2<KeyCacheObject, Map<KeyCacheObject, Map<UUID, GridCacheVersion>>> res = futRes.get();

                KeyCacheObject nextBatchKey = res.get1();

                Map<KeyCacheObject, Map<UUID, GridCacheVersion>> recheckKeys = res.get2();

                if (nextBatchKey != null)
                    schedule(new Batch(workload.cacheName(), workload.partitionId(), nextBatchKey));

                if (!recheckKeys.isEmpty())
                    schedule(new Recheck(recheckKeys, workload.cacheName(), workload.partitionId(), 0),
                        RECHECK_DELAY,
                        TimeUnit.SECONDS
                    );
            }
        );
    }

    /**
     * @param workload
     */
    private void handle(Recheck workload) {
        compute(
            CollectPartitionKeysByRecheckRequestTask.class,
            new RecheckRequest(new ArrayList<>(workload.recheckKeys().keySet()), workload.cacheName(), workload.partitionId(), startTopVer),
            futRes -> {
                Map<KeyCacheObject, Map<UUID, VersionedValue>> actualKeys = futRes.get();

                Map<KeyCacheObject, Map<UUID, GridCacheVersion>> notResolvingConflicts
                    = checkConflicts(workload.recheckKeys(), actualKeys);

                if (!notResolvingConflicts.isEmpty()) {
                    if (workload.attempt() < recheckAttempts) {
                        schedule(new Recheck(
                                notResolvingConflicts,
                                workload.cacheName(),
                                workload.partitionId(),
                                workload.attempt() + 1
                            ),
                            10,
                            TimeUnit.SECONDS
                        );
                    }
                    else if (fixMode)
                        schedule(repair(workload.cacheName(), workload.partitionId(), notResolvingConflicts, actualKeys));
                    else
                        addToPrintResult(workload.cacheName(), workload.partitionId(), notResolvingConflicts);
                }
            });
    }

    /**
     * @param workload
     */
    private void handle(Repair workload) {
        //TODO repair task code.
    }

    /**
     *
     */
    private Repair repair(
        String cacheName,
        int partId,
        Map<KeyCacheObject, Map<UUID, GridCacheVersion>> notResolvingConflicts,
        Map<KeyCacheObject, Map<UUID, VersionedValue>> actualKeys
    ) {
        Map<KeyCacheObject, Map<UUID, VersionedValue>> res = new HashMap<>();

        for (KeyCacheObject key : notResolvingConflicts.keySet()) {
            Map<UUID, VersionedValue> versionedByNodes = actualKeys.get(key);
            if (versionedByNodes != null)
                res.put(key, versionedByNodes);
        }

        return new Repair(cacheName, partId, res);
    }

    /**
     *
     */
    private void addToPrintResult(
        String cacheName,
        int partId,
        Map<KeyCacheObject, Map<UUID, GridCacheVersion>> conflicts
    ) {
        CacheObjectContext ctx = ignite.cachex(cacheName).context().cacheObjectContext();

        synchronized (inconsistentKeys) {
            List<Map<UUID, PartitionReconciliationDataRowMeta>> brokenKeys = new ArrayList<>();

            for (Map.Entry<KeyCacheObject, Map<UUID, GridCacheVersion>> entry : conflicts.entrySet()) {
                KeyCacheObject key = entry.getKey();

                Map<UUID, PartitionReconciliationDataRowMeta> map = new HashMap<>();

                for (Map.Entry<UUID, GridCacheVersion> versionEntry : entry.getValue().entrySet()) {
                    UUID nodeId = versionEntry.getKey();

                    try {
                        map.put(nodeId,
                            new PartitionReconciliationDataRowMeta(
                                new PartitionReconciliationKeyMeta(key.valueBytes(ctx), key.toString(), versionEntry.getValue()),
                                null
                            ));
                    }
                    catch (IgniteCheckedException e) {
                        log.error("Broken key can't be added to result. ", e);
                    }
                }

                brokenKeys.add(map);
            }

            inconsistentKeys.computeIfAbsent(cacheName, k -> new HashMap<>())
                .computeIfAbsent(partId, k -> new ArrayList<>())
                .addAll(brokenKeys);
        }
    }

    /**
     *
     */
    private PartitionReconciliationResult prepareResult() {
        synchronized (inconsistentKeys) {
            return new PartitionReconciliationResult(inconsistentKeys);
        }
    }

    /**
     * @param taskCls Task class.
     * @param arg Argument.
     * @param lsnr Listener.
     */
    private <T extends CachePartitionRequest, R> void compute(Class<? extends ComputeTask<T, R>> taskCls, T arg,
        IgniteInClosure<? super IgniteFuture<R>> lsnr) {
        liveListeners.incrementAndGet();

        ignite.compute(partOwners(arg.cacheName(), arg.partitionId())).executeAsync(taskCls, arg).listen(futRes -> {
            try {
                lsnr.apply(futRes);
            }
            finally {
                liveListeners.decrementAndGet();
            }
        });
    }

    /**
     * Send a task object for immediate processing.
     *
     * @param task Task object.
     */
    private void schedule(PipelineWorkload task) {
        schedule(task, 0, TimeUnit.MILLISECONDS);
    }

    /**
     * Send a task object which will available after time.
     *
     * @param task Task object.
     * @param duration Wait time.
     * @param timeUnit Time unit.
     */
    private void schedule(PipelineWorkload task, long duration, TimeUnit timeUnit) {
        try {
            long finishTime = U.currentTimeMillis() + timeUnit.toMillis(duration);

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
