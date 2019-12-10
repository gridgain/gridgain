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
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.GridCachePartitionExchangeManager;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.checker.objects.PartitionBatchRequest;
import org.apache.ignite.internal.processors.cache.checker.objects.PartitionKeyVersion;
import org.apache.ignite.internal.processors.cache.checker.objects.PartitionReconciliationResult;
import org.apache.ignite.internal.processors.cache.checker.objects.RecheckRequest;
import org.apache.ignite.internal.processors.cache.checker.objects.RepairRequest;
import org.apache.ignite.internal.processors.cache.checker.objects.RepairResult;
import org.apache.ignite.internal.processors.cache.checker.objects.VersionedValue;
import org.apache.ignite.internal.processors.cache.checker.processor.workload.Batch;
import org.apache.ignite.internal.processors.cache.checker.processor.workload.Recheck;
import org.apache.ignite.internal.processors.cache.checker.processor.workload.Repair;
import org.apache.ignite.internal.processors.cache.checker.tasks.CollectPartitionKeysByBatchTask;
import org.apache.ignite.internal.processors.cache.checker.tasks.CollectPartitionKeysByRecheckRequestTask;
import org.apache.ignite.internal.processors.cache.checker.tasks.RepairRequestTask;
import org.apache.ignite.internal.processors.cache.verify.PartitionReconciliationDataRowMeta;
import org.apache.ignite.internal.processors.cache.verify.PartitionReconciliationKeyMeta;
import org.apache.ignite.internal.processors.cache.verify.PartitionReconciliationRepairMeta;
import org.apache.ignite.internal.processors.cache.verify.PartitionReconciliationValueMeta;
import org.apache.ignite.internal.processors.cache.verify.RepairAlgorithm;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.processors.cache.checker.util.ConsistencyCheckUtils.checkConflicts;
import static org.apache.ignite.internal.processors.cache.checker.util.ConsistencyCheckUtils.mapPartitionReconciliation;
import static org.apache.ignite.internal.processors.cache.checker.util.ConsistencyCheckUtils.unmarshalKey;

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

    /** Amount of potentially inconsistent keys repair attempts. */
    private final int repairAttempts;

    /**
     * Specifies which fix algorithm to use: options {@code PartitionReconciliationRepairMeta.RepairAlg}
     * while repairing doubtful keys.
     */
    private RepairAlgorithm repairAlg;

    /**
     *
     */
    private final Map<String, Map<Integer, List<Map<UUID, PartitionReconciliationDataRowMeta>>>> inconsistentKeys = new HashMap<>();

    /**
     *
     */
    private final IgniteLogger log;

    /**
     *
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType") public PartitionReconciliationProcessor(
        IgniteEx ignite,
        GridCachePartitionExchangeManager<Object, Object> exchMgr,
        Collection<String> caches,
        boolean fixMode,
        int throttlingIntervalMillis,
        int batchSize,
        int recheckAttempts,
        int repairAttempts,
        RepairAlgorithm repairAlg
    ) throws IgniteCheckedException {
        super(ignite, exchMgr, PARALLELISM_LEVEL);
        log = ignite.log().getLogger(this);
        this.caches = caches;
        this.fixMode = fixMode;
        this.throttlingIntervalMillis = throttlingIntervalMillis;
        this.batchSize = batchSize;
        this.recheckAttempts = recheckAttempts;
        this.repairAttempts = repairAttempts;
        this.repairAlg = repairAlg;
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
                    schedule(
                        new Recheck(recheckKeys, workload.cacheName(), workload.partitionId(), 0, 0),
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
            new RecheckRequest(new ArrayList<>(workload.recheckKeys().keySet()), workload.cacheName(),
                workload.partitionId(), startTopVer),
            futRes -> {
                Map<KeyCacheObject, Map<UUID, VersionedValue>> actualKeys = futRes.get();

                Map<KeyCacheObject, Map<UUID, GridCacheVersion>> notResolvingConflicts
                    = checkConflicts(workload.recheckKeys(), actualKeys,
                    ignite.cachex(workload.cacheName()).context());

                if (!notResolvingConflicts.isEmpty()) {
                    if (workload.attempt() < recheckAttempts) {
                        schedule(new Recheck(
                                notResolvingConflicts,
                                workload.cacheName(),
                                workload.partitionId(),
                                workload.attempt() + 1,
                                workload.repairAttempt()
                            ),
                            10,
                            TimeUnit.SECONDS
                        );
                    }
                    else if (fixMode) {
                        scheduleHighPriority(repair(workload.cacheName(), workload.partitionId(), notResolvingConflicts,
                            actualKeys, workload.repairAttempt()));
                    }
                    else
                        addToPrintResult(workload.cacheName(), workload.partitionId(), notResolvingConflicts, actualKeys);
                }
            });
    }

    /**
     * @param workload
     */
    private void handle(Repair workload) throws InterruptedException {
        compute(
            RepairRequestTask.class,
            new RepairRequest(workload.data(), workload.cacheName(), workload.partitionId(), startTopVer, repairAlg,
                repairAttempts),
            futRes -> {
                RepairResult repairRes = futRes.get();

                if (!repairRes.repairedKeys().isEmpty())
                    addToPrintResult(workload.cacheName(), workload.partitionId(), repairRes.repairedKeys());
                else if (!repairRes.keysToRepair().isEmpty()) {

                    // Repack recheck keys.
                    Map<KeyCacheObject, Map<UUID, GridCacheVersion>> recheckKeys = new HashMap<>();

                    for (Map.Entry<PartitionKeyVersion, Map<UUID, VersionedValue>> dataEntry :
                        repairRes.keysToRepair().entrySet()) {
                        KeyCacheObject keyCacheObj;

                        try {
                            keyCacheObj = unmarshalKey(
                                dataEntry.getKey().getKey(),
                                ignite.cachex(workload.cacheName()).context());
                        }
                        catch (IgniteCheckedException e) {
                            U.error(log, "Unable to unmarshal key=[" + dataEntry.getKey().getKey() +
                                "], key is skipped.");

                            continue;
                        }

                        recheckKeys.put(keyCacheObj, dataEntry.getValue().entrySet().stream().
                            collect(Collectors.toMap(Map.Entry::getKey, e2 -> e2.getValue().version())));
                    }

                    if (workload.attempt() < repairAttempts) {
                        schedule(
                            new Recheck(
                                recheckKeys,
                                workload.cacheName(),
                                workload.partitionId(),
                                recheckAttempts,
                                workload.attempt() + 1
                            ));
                    }
                }
            });
    }

    /**
     *
     */
    private Repair repair(
        String cacheName,
        int partId,
        Map<KeyCacheObject, Map<UUID, GridCacheVersion>> notResolvingConflicts,
        Map<KeyCacheObject, Map<UUID, VersionedValue>> actualKeys,
        int repairAttempts
    ) {
        Map<KeyCacheObject, Map<UUID, VersionedValue>> res = new HashMap<>();

        for (KeyCacheObject key : notResolvingConflicts.keySet()) {
            Map<UUID, VersionedValue> versionedByNodes = actualKeys.get(key);
            if (versionedByNodes != null)
                res.put(key, versionedByNodes);
        }

        return new Repair(cacheName, partId, res, repairAttempts);
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
     * Add data to print result.
     * @param cacheName Cache name.
     * @param partId Partition Id.
     * @param repairedKeys Repaired keys.
     */
    private void addToPrintResult(
        String cacheName,
        int partId,
        Map<T2<PartitionKeyVersion, PartitionReconciliationRepairMeta>, Map<UUID, VersionedValue>> repairedKeys
    ) {
        CacheObjectContext ctx = ignite.cachex(cacheName).context().cacheObjectContext();

        synchronized (inconsistentKeys) {
            try {
                List<Map<UUID, PartitionReconciliationDataRowMeta>> res = new ArrayList<>();

                for (Map.Entry<T2<PartitionKeyVersion, PartitionReconciliationRepairMeta>, Map<UUID, VersionedValue>>
                    entry : repairedKeys.entrySet()) {
                    Map<UUID, PartitionReconciliationDataRowMeta> map = new HashMap<>();

                    for (Map.Entry<UUID, VersionedValue> uuidBasedEntry : entry.getValue().entrySet()) {
                        KeyCacheObject key = entry.getKey().get1().getKey();

                        Optional<CacheObject> cacheObjOpt = Optional.ofNullable(uuidBasedEntry.getValue().value());

                        Optional<String> valStr = cacheObjOpt
                            .flatMap(co -> Optional.ofNullable(co.value(ctx, false)))
                            .map(Object::toString);

                        key.finishUnmarshal(ctx, null);

                        Object keyVal = key.value(ctx, false);

                        map.put(uuidBasedEntry.getKey(),
                            new PartitionReconciliationDataRowMeta(
                                new PartitionReconciliationKeyMeta(
                                    key.valueBytes(ctx),
                                    keyVal != null ? keyVal.toString() : null,
                                    entry.getValue().get(uuidBasedEntry.getKey()).version()
                                ),
                                new PartitionReconciliationValueMeta(
                                    cacheObjOpt.isPresent() ? cacheObjOpt.get().valueBytes(ctx) : null,
                                    valStr.orElse(null)
                                ),
                                entry.getKey().get2()
                            ));
                    }
                    res.add(map);
                }

                inconsistentKeys.computeIfAbsent(cacheName, k -> new HashMap<>())
                    .computeIfAbsent(partId, k -> new ArrayList<>())
                    .addAll(res);
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
