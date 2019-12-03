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
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.GridCachePartitionExchangeManager;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.checker.objects.PartitionBatchRequest;
import org.apache.ignite.internal.processors.cache.checker.objects.PartitionReconciliationResult;
import org.apache.ignite.internal.processors.cache.checker.objects.RecheckRequest;
import org.apache.ignite.internal.processors.cache.checker.objects.VersionedValue;
import org.apache.ignite.internal.processors.cache.checker.processor.workload.Batch;
import org.apache.ignite.internal.processors.cache.checker.processor.workload.Recheck;
import org.apache.ignite.internal.processors.cache.checker.processor.workload.Repair;
import org.apache.ignite.internal.processors.cache.checker.tasks.CollectPartitionKeysByBatchTask;
import org.apache.ignite.internal.processors.cache.checker.tasks.CollectPartitionKeysByRecheckRequestTask;
import org.apache.ignite.internal.processors.cache.verify.PartitionReconciliationDataRowMeta;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.T2;

import static org.apache.ignite.internal.processors.cache.checker.util.ConsistencyCheckUtils.checkConflicts;
import static org.apache.ignite.internal.processors.cache.checker.util.ConsistencyCheckUtils.mapPartitionReconciliation;

/**
 * The base point of partition reconciliation processing.
 */
public class PartitionReconciliationProcessor extends AbstractPipelineProcessor {
    /** Recheck delay seconds. */
    private static final int RECHECK_DELAY = 10;

    /**
     * Number of tasks works parallel.
     */
    private static final int PARALLELISM_LEVEL = 100;

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
    private final Map<String, Map<Integer, List<Map<UUID, PartitionReconciliationDataRowMeta>>>> inconsistentKeys = new HashMap<>();

    /**
     *
     */
    private final Map<String, Map<Integer, Map<KeyCacheObject, Map<UUID, GridCacheVersion>>>> fixedKeys = new HashMap<>();

    /**
     *
     */
    private final IgniteLogger log;

    /**
     *
     */
    public PartitionReconciliationProcessor(
        IgniteEx ignite,
        GridCachePartitionExchangeManager<Object, Object> exchMgr,
        Collection<String> caches,
        boolean fixMode,
        int throttlingIntervalMillis,
        int batchSize,
        int recheckAttempts
    ) throws IgniteCheckedException {
        super(ignite, exchMgr, PARALLELISM_LEVEL);
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

            boolean live = false;

            while (!isEmpty() || (live = hasLiveHandlers())) {
                if (topologyChanged()) {
                    log.warning("Topology was changed. Partition reconciliation task was stopped.");

                    return new PartitionReconciliationResult(Collections.emptyMap());
                }

                if (isEmpty() && live) {
                    Thread.sleep(1_000);

                    continue;
                }

                PipelineWorkload workload = takeTask();

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
    private void handle(Batch workload) throws InterruptedException {
        compute(
            CollectPartitionKeysByBatchTask.class,
            new PartitionBatchRequest(workload.cacheName(), workload.partitionId(), batchSize, workload.lowerKey(), startTopVer),
            futRes -> {
                T2<KeyCacheObject, Map<KeyCacheObject, Map<UUID, GridCacheVersion>>> res = futRes.get();

                KeyCacheObject nextBatchKey = res.get1();

                Map<KeyCacheObject, Map<UUID, GridCacheVersion>> recheckKeys = res.get2();

                assert nextBatchKey != null || recheckKeys.isEmpty();

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
    private void handle(Recheck workload) throws InterruptedException {
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
                        scheduleHighPriority(repair(workload.cacheName(), workload.partitionId(), notResolvingConflicts, actualKeys));
                    else
                        addToPrintResult(workload.cacheName(), workload.partitionId(), notResolvingConflicts, actualKeys);
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
        Map<KeyCacheObject, Map<UUID, GridCacheVersion>> conflicts,
        Map<KeyCacheObject, Map<UUID, VersionedValue>> actualKeys
    ) {
        CacheObjectContext ctx = ignite.cachex(cacheName).context().cacheObjectContext();

        synchronized (inconsistentKeys) {
            try {
                inconsistentKeys.computeIfAbsent(cacheName, k -> new HashMap<>())
                    .computeIfAbsent(partId, k -> new ArrayList<>())
                    .addAll(mapPartitionReconciliation(conflicts, actualKeys, ctx));
            }
            catch (IgniteCheckedException e) {
                log.error("Broken key can't be added to result. ", e);
            }
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
}
