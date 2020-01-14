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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.cache.configuration.Factory;
import javax.cache.expiry.EternalExpiryPolicy;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.GridCachePartitionExchangeManager;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.checker.objects.ExecutionResult;
import org.apache.ignite.internal.processors.cache.checker.objects.PartitionBatchRequest;
import org.apache.ignite.internal.processors.cache.checker.objects.PartitionKeyVersion;
import org.apache.ignite.internal.processors.cache.checker.objects.PartitionReconciliationResult;
import org.apache.ignite.internal.processors.cache.checker.objects.RecheckRequest;
import org.apache.ignite.internal.processors.cache.checker.objects.RepairRequest;
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
import org.apache.ignite.internal.processors.cache.verify.RepairMeta;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.IgniteSystemProperties.getLong;
import static org.apache.ignite.internal.processors.cache.checker.util.ConsistencyCheckUtils.checkConflicts;
import static org.apache.ignite.internal.processors.cache.checker.util.ConsistencyCheckUtils.mapPartitionReconciliation;
import static org.apache.ignite.internal.processors.cache.checker.util.ConsistencyCheckUtils.unmarshalKey;

/**
 * The base point of partition reconciliation processing.
 */
public class PartitionReconciliationProcessor extends AbstractPipelineProcessor {
    /** Session change message. */
    public static final String SESSION_CHANGE_MSG = "Reconciliation session was changed.";

    /** Topology change message. */
    public static final String TOPOLOGY_CHANGE_MSG = "Topology was changed. Partition reconciliation task was stopped.";

    /** Work progress message. */
    public static final String WORK_PROGRESS_MSG = "Partition reconciliation task [sesId=%s, total=%s, remaining=%s]";

    /** Start execution message. */
    public static final String START_EXECUTION_MSG = "Partition reconciliation started [fixMode: %s, repairAlg: %s, " +
        "batchSize: %s, recheckAttempts: %s, parallelismLevel: %s, caches: %s].";

    /** Error reason. */
    public static final String ERROR_REASON = "Reason [msg=%s, exception=%s]";

    /** Recheck delay seconds. */
    private final int recheckDelay;

    /** Caches. */
    private final Collection<String> caches;

    /** If {@code true} - Partition Reconciliation&Fix: update from Primary partition. */
    private final boolean fixMode;

    /** Amount of keys to retrieve within one job. */
    private final int batchSize;

    /** Amount of potentially inconsistent keys recheck attempts. */
    private final int recheckAttempts;

    /**
     * Specifies which fix algorithm to use: options {@code PartitionReconciliationRepairMeta.RepairAlg} while repairing
     * doubtful keys.
     */
    private final RepairAlgorithm repairAlg;

    /**
     *
     */
    private final Map<String, Map<Integer, List<PartitionReconciliationDataRowMeta>>> inconsistentKeys = new HashMap<>();

    /**
     *
     */
    private final IgniteLogger log;

    /**
     *
     */
    private final WorkProgress workProgress = new WorkProgress();

    /**
     *
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType") public PartitionReconciliationProcessor(
        long sesId,
        IgniteEx ignite,
        GridCachePartitionExchangeManager<Object, Object> exchMgr,
        Collection<String> caches,
        boolean fixMode,
        int parallelismLevel,
        int batchSize,
        int recheckAttempts,
        RepairAlgorithm repairAlg,
        int recheckDelay
    ) throws IgniteCheckedException {
        super(sesId, ignite, exchMgr, parallelismLevel);
        log = ignite.log().getLogger(this);
        this.recheckDelay = recheckDelay;
        this.caches = caches;
        this.fixMode = fixMode;
        this.batchSize = batchSize;
        this.recheckAttempts = recheckAttempts;
        this.repairAlg = repairAlg;
    }

    /**
     * @return
     */
    public ExecutionResult<PartitionReconciliationResult> execute() {
        log.info(String.format(START_EXECUTION_MSG, fixMode, repairAlg, batchSize, recheckAttempts, parallelismLevel, caches));

        try {
            for (String cache : caches) {
                IgniteInternalCache<Object, Object> cachex = ignite.cachex(cache);

                Factory expiryPlcFactory = cachex.configuration().getExpiryPolicyFactory();
                if (expiryPlcFactory != null && !(expiryPlcFactory.create() instanceof EternalExpiryPolicy)) {
                    log.warning("The cache '" + cache + "' was skipped because CacheConfiguration#setExpiryPolicyFactory exists.");

                    continue;
                }

                int[] partitions = ignite.affinity(cache).primaryPartitions(ignite.localNode());

                for (int partId : partitions) {
                    schedule(new Batch(UUID.randomUUID(), cache, partId, null));

                    workProgress.assignWork();
                }
            }

            boolean live = false;

            while (!isEmpty() || (live = hasLiveHandlers())) {
                if (topologyChanged())
                    throw new IgniteException(TOPOLOGY_CHANGE_MSG);

                if (isSessionExpired())
                    throw new IgniteException(SESSION_CHANGE_MSG);

                if (isInterrupted())
                    throw new IgniteException(error.get());

                if (isEmpty() && live) {
                    Thread.sleep(1_000);

                    continue;
                }

                PipelineWorkload workload = takeTask();

                workProgress.printWorkProgress();

                if (workload instanceof Batch)
                    handle((Batch)workload);
                else if (workload instanceof Recheck)
                    handle((Recheck)workload);
                else if (workload instanceof Repair)
                    handle((Repair)workload);
                else
                    log.error("Unsupported workload type: " + workload);
            }

            return new ExecutionResult<>(prepareResult());
        }
        catch (InterruptedException | IgniteException e) {
            String errMsg = "Partition reconciliation was interrupted.";

            waitWorkFinish();

            log.warning(errMsg, e);

            return new ExecutionResult<>(prepareResult(), errMsg + " " + String.format(ERROR_REASON, e.getMessage(), e.getClass()));
        }
        catch (Exception e) {
            String errMsg = "Unexpected error.";

            log.error(errMsg, e);

            return new ExecutionResult<>(prepareResult(), errMsg + " " + String.format(ERROR_REASON, e.getMessage(), e.getClass()));
        }
    }

    /**
     * @param workload
     */
    private void handle(Batch workload) throws InterruptedException {
        compute(
            CollectPartitionKeysByBatchTask.class,
            new PartitionBatchRequest(workload.getSessionId(), workload.cacheName(), workload.partitionId(), batchSize, workload.lowerKey(), startTopVer),
            res -> {
                KeyCacheObject nextBatchKey = res.get1();

                Map<KeyCacheObject, Map<UUID, GridCacheVersion>> recheckKeys = res.get2();

                assert nextBatchKey != null || recheckKeys.isEmpty();

                if (nextBatchKey != null)
                    schedule(new Batch(workload.getSessionId(), workload.cacheName(), workload.partitionId(), nextBatchKey));

                if (!recheckKeys.isEmpty())
                    schedule(
                        new Recheck(workload.getSessionId(), recheckKeys, workload.cacheName(), workload.partitionId(), 0, 0),
                        recheckDelay,
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
            new RecheckRequest(workload.getSessionId(), new ArrayList<>(workload.recheckKeys().keySet()), workload.cacheName(),
                workload.partitionId(), startTopVer),
            actualKeys -> {
                Map<KeyCacheObject, Map<UUID, GridCacheVersion>> notResolvingConflicts
                    = checkConflicts(workload.recheckKeys(), actualKeys,
                    ignite.cachex(workload.cacheName()).context(), startTopVer);

                if (!notResolvingConflicts.isEmpty()) {
                    if (workload.attempt() < recheckAttempts) {
                        schedule(new Recheck(
                                workload.getSessionId(),
                                notResolvingConflicts,
                                workload.cacheName(),
                                workload.partitionId(),
                                workload.attempt() + 1,
                                workload.repairAttempt()
                            ),
                            recheckDelay,
                            TimeUnit.SECONDS
                        );
                    }
                    else if (fixMode) {
                        scheduleHighPriority(repair(workload.getSessionId(), workload.cacheName(), workload.partitionId(), notResolvingConflicts,
                            actualKeys, workload.repairAttempt()));
                    }
                    else {
                        addToPrintResult(workload.cacheName(), workload.partitionId(), notResolvingConflicts, actualKeys);

                        workProgress.completeWork();
                    }
                }
            });
    }

    /**
     * @param workload
     */
    private void handle(Repair workload) throws InterruptedException {
        compute(
            RepairRequestTask.class,
            new RepairRequest(workload.getSessionId(), workload.data(), workload.cacheName(), workload.partitionId(), startTopVer, repairAlg,
                workload.attempt()),
            repairRes -> {
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

                    if (workload.attempt() < RepairRequestTask.MAX_REPAIR_ATTEMPTS) {
                        schedule(
                            new Recheck(
                                workload.getSessionId(),
                                recheckKeys,
                                workload.cacheName(),
                                workload.partitionId(),
                                recheckAttempts,
                                workload.attempt() + 1
                            ));

                        return;
                    }
                }

                workProgress.completeWork();
            });
    }

    /**
     *
     */
    private Repair repair(
        UUID sessionId,
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

        return new Repair(sessionId, cacheName, partId, res, repairAttempts);
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
     *
     * @param cacheName Cache name.
     * @param partId Partition Id.
     * @param repairedKeys Repaired keys.
     */
    private void addToPrintResult(
        String cacheName,
        int partId,
        Map<T2<PartitionKeyVersion, RepairMeta>, Map<UUID, VersionedValue>> repairedKeys
    ) {
        CacheObjectContext ctx = ignite.cachex(cacheName).context().cacheObjectContext();

        synchronized (inconsistentKeys) {
            try {
                List<PartitionReconciliationDataRowMeta> res = new ArrayList<>();

                for (Map.Entry<T2<PartitionKeyVersion, RepairMeta>, Map<UUID, VersionedValue>>
                    entry : repairedKeys.entrySet()) {
                    Map<UUID, PartitionReconciliationValueMeta> valMap = new HashMap<>();

                    for (Map.Entry<UUID, VersionedValue> uuidBasedEntry : entry.getValue().entrySet()) {
                        Optional<CacheObject> cacheObjOpt = Optional.ofNullable(uuidBasedEntry.getValue().value());

                        valMap.put(
                            uuidBasedEntry.getKey(),
                            cacheObjOpt.isPresent() ?
                                new PartitionReconciliationValueMeta(
                                    cacheObjOpt.get().valueBytes(ctx),
                                    Optional.ofNullable(cacheObjOpt.get().value(ctx, false)).map(Object::toString).orElse(null),
                                    uuidBasedEntry.getValue().version())
                                :
                                null);
                    }

                    KeyCacheObject key = entry.getKey().get1().getKey();

                    key.finishUnmarshal(ctx, null);

                    Object keyVal = key.value(ctx, false);

                    RepairMeta repairMeta = entry.getKey().get2();

                    Optional<CacheObject> cacheObjRepairValOpt = Optional.ofNullable(repairMeta.value());

                    res.add(
                        new PartitionReconciliationDataRowMeta(
                            new PartitionReconciliationKeyMeta(
                                key.valueBytes(ctx),
                                keyVal != null ? keyVal.toString() : null),
                            valMap,
                            new PartitionReconciliationRepairMeta(
                                repairMeta.fixed(),
                                cacheObjRepairValOpt.isPresent() ?
                                    new PartitionReconciliationValueMeta(
                                        cacheObjRepairValOpt.get().valueBytes(ctx),
                                        Optional.ofNullable(cacheObjRepairValOpt.get().value(ctx, false)).map(Object::toString).orElse(null),
                                        null)
                                    :
                                    null,
                                repairMeta.repairAlg())));
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
            return new PartitionReconciliationResult(
                ignite.cluster().nodes().stream().collect(Collectors.toMap(
                    ClusterNode::id,
                    n -> n.consistentId().toString())),
                inconsistentKeys);
        }
    }

    /**
     *
     */
    private class WorkProgress {
        /** Work progress print interval. */
        private final long WORK_PROGRESS_PRINT_INTERVAL = getLong("WORK_PROGRESS_PRINT_INTERVAL", 1000 * 60 * 3);

        /**
         *
         */
        private long total;

        /**
         *
         */
        private long remaining;

        /**
         *
         */
        private long printedTime;

        /**
         *
         */
        public void printWorkProgress() {
            long currTimeMillis = System.currentTimeMillis();

            if (currTimeMillis >= printedTime + WORK_PROGRESS_PRINT_INTERVAL) {
                log.info(String.format(WORK_PROGRESS_MSG, sesId, workProgress.getTotal(), workProgress.getRemaining()));

                printedTime = currTimeMillis;
            }
        }

        /**
         *
         */
        public void assignWork() {
            total++;
            remaining++;
        }

        /**
         *
         */
        public void completeWork() {
            remaining--;
        }

        /**
         *
         */
        public long getTotal() {
            return total;
        }

        /**
         *
         */
        public long getRemaining() {
            return remaining;
        }
    }
}
