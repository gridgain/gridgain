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
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.cache.expiry.EternalExpiryPolicy;
import javax.cache.expiry.ExpiryPolicy;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.checker.objects.CachePartitionRequest;
import org.apache.ignite.internal.processors.cache.checker.objects.ExecutionResult;
import org.apache.ignite.internal.processors.cache.checker.objects.PartitionBatchRequest;
import org.apache.ignite.internal.processors.cache.checker.objects.RecheckRequest;
import org.apache.ignite.internal.processors.cache.checker.objects.ReconciliationAffectedEntries;
import org.apache.ignite.internal.processors.cache.checker.objects.RepairRequest;
import org.apache.ignite.internal.processors.cache.checker.objects.VersionedKey;
import org.apache.ignite.internal.processors.cache.checker.objects.VersionedValue;
import org.apache.ignite.internal.processors.cache.checker.processor.workload.Batch;
import org.apache.ignite.internal.processors.cache.checker.processor.workload.Recheck;
import org.apache.ignite.internal.processors.cache.checker.processor.workload.Repair;
import org.apache.ignite.internal.processors.cache.checker.tasks.CollectPartitionKeysByBatchTask;
import org.apache.ignite.internal.processors.cache.checker.tasks.CollectPartitionKeysByRecheckRequestTask;
import org.apache.ignite.internal.processors.cache.checker.tasks.RepairRequestTask;
import org.apache.ignite.internal.processors.cache.verify.RepairAlgorithm;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.diagnostic.ReconciliationExecutionContext.ReconciliationStatisticsUpdateListener;
import org.apache.ignite.internal.util.typedef.internal.U;

import static java.util.Collections.EMPTY_SET;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.internal.processors.cache.checker.util.ConsistencyCheckUtils.checkConflicts;
import static org.apache.ignite.internal.processors.cache.checker.util.ConsistencyCheckUtils.unmarshalKey;

/**
 * The base point of partition reconciliation processing.
 */
public class PartitionReconciliationProcessor extends AbstractPipelineProcessor {
    /** Session change message. */
    public static final String SESSION_CHANGE_MSG = "Reconciliation session has changed.";

    /** Topology change message. */
    public static final String TOPOLOGY_CHANGE_MSG = "Topology has changed. Partition reconciliation task was stopped.";

    /** Start execution message. */
    private static final String START_EXECUTION_MSG = "Partition reconciliation has started [sesionId=%s, repair=%s, repairAlgorithm=%s, " +
        "fastCheck=%s, batchSize=%s, recheckAttempts=%s, parallelismLevel=%s, caches=%s]";

    /** Stop execution message. */
    private static final String STOP_EXECUTION_MSG = "Partition reconciliation has finished locally [sessionId=%s, conflicts=%s, " +
        "repaired=%s, totalTime=%s(sec)]";

    /** Session progress message. */
    private static final String SESSION_PROGRESS_MSG = "Partition reconciliation status [" +
        "sesId=%s, " +              // session identifier
        "caches=%s, " +             // total number of caches to be scanned, this number may be less than number of caches requested
                                    // by the tool due to the fact that some caches might be skipped because of the expiration policy or
                                    // the node does not own primary partitions of the cache
        "parts= %s, " +             // total number of primary partitions to be scanned
        "conflicts=%s, " +          // total number of conflicts found
        "repaired=%s " +            // total number of fixed conflicts
        ", caches=[";

    private static final String CACHE_PROGRESS_MSG = " [" +
        "name=%s, " +                   // cache name
        "primaryParts=%s, " +           // number of primary partitions owned by this node
        "processedPrimaryKeys=%s, " +   // number of primary keys scanned [owned by this node]
        "processedBackupKeys=%s, " +    // number of backup keys scanned [owned by this node]
        "conflicts=%s, " +              // number of conflicts found
        "repaired=%s, " +               // number of fixed conflicts
        "completed=%s]";                // true if primary partitions are fully scanned

    /** Error reason. */
    public static final String ERROR_REASON = "Reason [msg=%s, exception=%s]";

    /** Recheck delay seconds. */
    private final int recheckDelay;

    /** Caches. */
    private final Collection<String> caches;

    /** Indicates that inconsistent key values should be repaired in accordance with {@link #repairAlg}. */
    private final boolean repair;

    /**
     * Represents a cache group mapping to set of partitions which should be validated.
     * If this field is {@code null} all partitions will be validated.
     */
    private final Map<Integer, Set<Integer>> partsToValidate;

    /** Amount of keys to retrieve within one job. */
    private final int batchSize;

    /** Amount of potentially inconsistent keys recheck attempts. */
    private final int recheckAttempts;

    /**
     * Specifies which fix algorithm to use: options {@code PartitionReconciliationRepairMeta.RepairAlg} while repairing
     * doubtful keys.
     */
    private final RepairAlgorithm repairAlg;

    /** Tracks workload chains based on its lifecycle. */
    private final WorkloadTracker workloadTracker = new WorkloadTracker();

    /** Results collector. */
    final ReconciliationResultCollector collector;

    /** */
    private AtomicLong lastWorkProgressUpdateTime = new AtomicLong(0);

    /**
     * Creates a new instance of Partition reconciliation processor.
     *
     * @param sesId Session identifier that allows to identify different runs of the utility.
     * @param ignite Local Ignite instance to be used as an entry point for the execution of the utility.
     * @param caches Collection of cache names to be checked.
     * @param repair Flag indicates that inconsistencies should be repaired.
     * @param partsToValidate Optional collection of partition which shoulb be validated.
     *                        If value is {@code null} all partitions will be validated.
     * @param parallelismLevel Number of batches that can be handled simultaneously.
     * @param batchSize Amount of keys to retrieve within one job.
     * @param recheckAttempts Amount of potentially inconsistent keys recheck attempts.
     * @param repairAlg Repair algorithm to be used to fix inconsistency.
     * @param recheckDelay Specifies the time interval between two consequent attempts to check keys.
     * @param compact {@code true} if the result should be returned in compact form.
     * @param includeSensitive {@code true} if sensitive information should be included in the result.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public PartitionReconciliationProcessor(
        long sesId,
        IgniteEx ignite,
        Collection<String> caches,
        Map<Integer, Set<Integer>> partsToValidate,
        boolean repair,
        RepairAlgorithm repairAlg,
        int parallelismLevel,
        int batchSize,
        int recheckAttempts,
        int recheckDelay,
        boolean compact,
        boolean includeSensitive
    ) throws IgniteCheckedException {
        super(sesId, ignite, parallelismLevel);

        this.recheckDelay = recheckDelay;
        this.caches = caches;
        this.repair = repair;
        this.partsToValidate = partsToValidate;
        this.batchSize = batchSize;
        this.recheckAttempts = recheckAttempts;
        this.repairAlg = repairAlg;

        registerListener(workloadTracker.andThen(evtLsnr));

        collector = (compact) ?
            new ReconciliationResultCollector.Compact(ignite, log, sesId, includeSensitive) :
            new ReconciliationResultCollector.Simple(ignite, log, includeSensitive);
    }

    /**
     * @return Partition reconciliation result
     */
    public ExecutionResult<ReconciliationAffectedEntries> execute() {
        long startTime = U.currentTimeMillis();

        if (log.isInfoEnabled()) {
            log.info(String.format(
                START_EXECUTION_MSG,
                sessionId(),
                repair,
                repairAlg,
                partsToValidate != null,
                batchSize,
                recheckAttempts,
                parallelismLevel,
                caches));
        }

        try {
            for (String cache : caches) {
                IgniteInternalCache<Object, Object> cachex = ignite.cachex(cache);

                if (cachex == null) {
                    if (log.isInfoEnabled())
                        log.info("The cache '" + cache + "' was skipped because it does not exist.");

                    continue;
                }

                ExpiryPolicy expPlc = cachex.context().expiry();
                if (expPlc != null && !(expPlc instanceof EternalExpiryPolicy)) {
                    if (log.isInfoEnabled())
                        log.info("The cache '" + cache + "' was skipped because CacheConfiguration#setExpiryPolicyFactory is set.");

                    continue;
                }

                int[] partitions = partitions(cache);

                for (int partId : partitions) {
                    Batch workload = new Batch(sesId, UUID.randomUUID(), cache, partId, null);

                    workloadTracker.addTrackingChain(workload);

                    schedule(workload);
                }
            }

            ctx.diagnostic().reconciliationExecutionContext().listenMetricsUpdates(sessionId(), workloadTracker);

            boolean live = false;
            lastWorkProgressUpdateTime.set(U.currentTimeMillis());

            while (!isEmpty() || (live = hasLiveHandlers())) {
                if (topologyChanged())
                    throw new IgniteException(TOPOLOGY_CHANGE_MSG);

                if (isSessionExpired())
                    throw new IgniteException(SESSION_CHANGE_MSG);

                if (isInterrupted())
                    throw new IgniteException(error.get());

                printStatistics();

                if (isEmpty() && live) {
                    U.sleep(100);

                    continue;
                }

                PipelineWorkload workload = pollTask(workProgressPrintInterval / 5, MILLISECONDS);
                if (workload == null)
                    continue;

                if (workload instanceof Batch)
                    handle((Batch)workload);
                else if (workload instanceof Recheck)
                    handle((Recheck)workload);
                else if (workload instanceof Repair)
                    handle((Repair)workload);
                else {
                    String err = "Unsupported workload type: " + workload;

                    log.error(err);

                    throw new IgniteException(err);
                }
            }

            return new ExecutionResult<>(collector.result());
        }
        catch (InterruptedException | IgniteException e) {
            String errMsg = "Partition reconciliation was interrupted.";

            waitWorkFinish();

            log.warning(errMsg, e);

            return new ExecutionResult<>(collector.result(), errMsg + ' ' + String.format(ERROR_REASON, e.getMessage(), e.getClass()));
        }
        catch (Exception e) {
            String errMsg = "Unexpected error.";

            log.error(errMsg, e);

            return new ExecutionResult<>(collector.result(), errMsg + ' ' + String.format(ERROR_REASON, e.getMessage(), e.getClass()));
        }
        finally {
            ctx.diagnostic().reconciliationExecutionContext().removeMetricsUpdateListener(sessionId());

            // force print statistics
            lastWorkProgressUpdateTime.set(0);
            printStatistics();

            if (log.isInfoEnabled()) {
                log.info(String.format(
                    STOP_EXECUTION_MSG,
                    sessionId(),
                    collector.conflictedEntriesSize(),
                    collector.repairedEntriesSize(),
                    (U.currentTimeMillis() - startTime) / 1000));
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void printStatistics() {
        long currTimeMillis = U.currentTimeMillis();

        long lastUpdate = lastWorkProgressUpdateTime.get();

        if (currTimeMillis < lastUpdate + workProgressPrintInterval || !log.isInfoEnabled())
            return;

        if (!lastWorkProgressUpdateTime.compareAndSet(lastUpdate, currTimeMillis))
            return;

        StringBuilder sb = new StringBuilder();

        sb.append(String.format(
            SESSION_PROGRESS_MSG,
            sesId,
            workloadTracker.totalCaches(),
            workloadTracker.totalChains(),
            collector.conflictedEntriesSize(),
            collector.repairedEntriesSize()));

        for (Map.Entry<String, WorkloadTracker.CacheStatisticsDescriptor> e : workloadTracker.cacheStatistics.entrySet()) {
            WorkloadTracker.CacheStatisticsDescriptor cacheDesc = e.getValue();

            sb.append(String.format(
                CACHE_PROGRESS_MSG,
                e.getKey(),
                cacheDesc.chainsCnt.get(),
                cacheDesc.scannedPrimaryKeysCnt.get(),
                cacheDesc.scannedBackupKeysCnt.get(),
                collector.conflictedEntriesSize(cacheDesc.cacheName),
                collector.repairedEntriesSize(cacheDesc.cacheName),
                cacheDesc.chainsCnt.get() == cacheDesc.processedChainsCnt.get()));
        }

        sb.append(']');

        if (log.isInfoEnabled())
            log.info(sb.toString());
    }

    /**
     * @return Reconciliation result collector which can be used to store the result to a file.
     */
    public ReconciliationResultCollector collector() {
        return collector;
    }

    /**
     * Returns primary partitions that belong to the local node for the given cache name.
     *
     * @param name Cache name.
     * @return Primary partitions that belong to the local node.
     */
    private int[] partitions(String name) {
        int[] cacheParts = ignite.affinity(name).primaryPartitions(ignite.localNode());

        if (partsToValidate == null) {
            // All local primary partitions should be validated.
            return cacheParts;
        }

        Set<Integer> parts = partsToValidate.getOrDefault(ctx.cache().cacheDescriptor(name).groupId(), EMPTY_SET);

        return IntStream.of(cacheParts).filter(p -> parts.contains(p)).toArray();
    }

    /**
     * @param workload Workload.
     */
    private void handle(Batch workload) throws InterruptedException {
        compute(
            CollectPartitionKeysByBatchTask.class,
            new PartitionBatchRequest(workload.sessionId(), workload.workloadChainId(), workload.cacheName(), workload.partitionId(), batchSize, workload.lowerKey(), startTopVer),
            res -> {
                KeyCacheObject nextBatchKey = res.get1();

                Map<KeyCacheObject, Map<UUID, GridCacheVersion>> recheckKeys = res.get2();

                assert nextBatchKey != null || recheckKeys.isEmpty();

                if (nextBatchKey != null)
                    schedule(new Batch(workload.sessionId(), workload.workloadChainId(), workload.cacheName(), workload.partitionId(), nextBatchKey));

                if (!recheckKeys.isEmpty()) {
                    schedule(
                        new Recheck(workload.sessionId(), workload.workloadChainId(), recheckKeys,
                            workload.cacheName(), workload.partitionId(), 0, 0),
                        recheckDelay,
                        TimeUnit.SECONDS
                    );
                }
            }
        );
    }

    /**
     * @param workload Workload.
     */
    private void handle(Recheck workload) throws InterruptedException {
        compute(
            CollectPartitionKeysByRecheckRequestTask.class,
            new RecheckRequest(workload.sessionId(), workload.workloadChainId(), new ArrayList<>(workload.recheckKeys().keySet()), workload.cacheName(),
                workload.partitionId(), startTopVer),
            actualKeys -> {
                Map<KeyCacheObject, Map<UUID, GridCacheVersion>> conflicts
                    = checkConflicts(workload.recheckKeys(), actualKeys,
                    ignite.cachex(workload.cacheName()).context(), startTopVer);

                if (!conflicts.isEmpty()) {
                    if (workload.recheckAttempt() < recheckAttempts) {
                        schedule(new Recheck(
                                workload.sessionId(),
                                workload.workloadChainId(),
                                conflicts,
                                workload.cacheName(),
                                workload.partitionId(),
                                workload.recheckAttempt() + 1,
                                workload.repairAttempt()
                            ),
                            recheckDelay,
                            TimeUnit.SECONDS
                        );
                    }
                    else if (repair) {
                        scheduleHighPriority(repair(workload.sessionId(), workload.workloadChainId(), workload.cacheName(), workload.partitionId(), conflicts,
                            actualKeys, workload.repairAttempt()));
                    }
                    else {
                        collector.appendConflictedEntries(
                            workload.cacheName(),
                            workload.partitionId(),
                            conflicts,
                            actualKeys);
                    }
                }
            });
    }

    /**
     * @param workload Workload.
     */
    private void handle(Repair workload) throws InterruptedException {
        compute(
            RepairRequestTask.class,
            new RepairRequest(workload.sessionId(), workload.workloadChainId(), workload.data(), workload.cacheName(), workload.partitionId(), startTopVer, repairAlg,
                workload.repairAttempt()),
            repairRes -> {
                if (!repairRes.repairedKeys().isEmpty())
                    collector.appendRepairedEntries(workload.cacheName(), workload.partitionId(), repairRes.repairedKeys());

                if (!repairRes.keysToRepair().isEmpty()) {
                    // Repack recheck keys.
                    Map<KeyCacheObject, Map<UUID, GridCacheVersion>> recheckKeys = new HashMap<>();

                    for (Map.Entry<VersionedKey, Map<UUID, VersionedValue>> dataEntry :
                        repairRes.keysToRepair().entrySet()) {
                        KeyCacheObject keyCacheObj;

                        try {
                            keyCacheObj = unmarshalKey(
                                dataEntry.getKey().key(),
                                ignite.cachex(workload.cacheName()).context());
                        }
                        catch (IgniteCheckedException e) {
                            U.error(log, "Unable to unmarshal key=[" + dataEntry.getKey().key() +
                                "], key is skipped.");

                            continue;
                        }

                        recheckKeys.put(keyCacheObj, dataEntry.getValue().entrySet().stream().
                            collect(Collectors.toMap(Map.Entry::getKey, e2 -> e2.getValue().version())));
                    }

                    if (workload.repairAttempt() < RepairRequestTask.MAX_REPAIR_ATTEMPTS) {
                        schedule(
                            new Recheck(
                                workload.sessionId(),
                                workload.workloadChainId(),
                                recheckKeys,
                                workload.cacheName(),
                                workload.partitionId(),
                                recheckAttempts,
                                workload.repairAttempt() + 1
                            ));
                    }
                }
            });
    }

    /**
     *
     */
    private Repair repair(
        long sesId,
        UUID workloadChainId,
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

        return new Repair(sesId, workloadChainId, cacheName, partId, res, repairAttempts);
    }

    /**
     * This class allows tracking workload chains based on its lifecycle.
     */
    private class WorkloadTracker implements ReconciliationEventListener, ReconciliationStatisticsUpdateListener {
        /** Map of active trackable chains. */
        private final Map<UUID, ChainDescriptor> activeChains = new ConcurrentHashMap<>();

        /** Map of cache metrics. */
        private final Map<String, CacheStatisticsDescriptor> cacheStatistics = new ConcurrentHashMap<>();

        /** Total number of tracked chains. */
        private final AtomicInteger trackedChainsCnt = new AtomicInteger();

        /** Number of completed chains. */
        private final AtomicInteger completedChainsCnt = new AtomicInteger();

        /** {@inheritDoc} */
        @Override public void onEvent(WorkLoadStage stage, PipelineWorkload workload) {
            switch (stage) {
                case SCHEDULED:
                    attachWorkload(workload);

                    break;

                case SKIPPED:
                    skipWorkload(workload);

                    break;

                case FINISHED:
                    detachWorkload(workload);

                    break;
                default:
                    // There is no need to process other stages.
            }
        }

        /** {@inheritDoc} */
        @Override public void updateScannedPartition(long sesId, String cacheName, int partId, boolean primary, long keysCnt) {
            assert sesId == sessionId() : "Update partition scan metrics does not correspond to the current session " +
                "[currSesId=" + sessionId() + ", updateSesId=" + sesId + ']';

            CacheStatisticsDescriptor desc = cacheStatistics.get(cacheName);
            if (desc != null) {
                if (primary)
                    desc.scannedPrimaryKeysCnt.addAndGet(keysCnt);
                else
                    desc.scannedBackupKeysCnt.addAndGet(keysCnt);
            }
        }

        /**
         * @return Total number of tracked chains.
         */
        public Integer totalChains() {
            return trackedChainsCnt.get();
        }

        /**
         * @return Number of chains that are not completed yet.
         */
        public Integer remaningChains() {
            return trackedChainsCnt.get() - completedChainsCnt.get();
        }

        /**
         * @return Total number of caches.
         */
        public Integer totalCaches() {
            return cacheStatistics.size();
        }

        /**
         * Adds the provided chain to a set of trackable chains.
         *
         * @param batch Chain to track.
         */
        public void addTrackingChain(Batch batch) {
            assert batch.sessionId() == sesId : "New tracking chain does not correspond to the current session " +
                "[currSesId=" + sesId + ", chainSesId=" + batch.sessionId() + ", chainId=" + batch.workloadChainId() + ']';

            activeChains.putIfAbsent(
                batch.workloadChainId(),
                new ChainDescriptor(batch.workloadChainId(), batch.cacheName(), batch.partitionId()));

            CacheStatisticsDescriptor stat = cacheStatistics.computeIfAbsent(batch.cacheName(), CacheStatisticsDescriptor::new);

            stat.chainsCnt.incrementAndGet();

            trackedChainsCnt.incrementAndGet();
        }

        /**
         * Incremets the number of workloads for the corresponding chain if it is trackable.
         *
         * @param workload Workload to add.
         */
        private void attachWorkload(PipelineWorkload workload) {
            // It should be guaranteed that the workload can be scheduled
            // strictly before its parent workload is finished.
            Optional.ofNullable(activeChains.get(workload.workloadChainId()))
                .map(d -> d.workloadCnt.incrementAndGet());
        }

        /**
         * Decrements the number of workloads for the corresponding chain if it is trackable
         *
         * If the given workload is the last one for corresponding chain
         * then {@link #onChainCompleted(UUID, String, int)} is triggered.
         *
         * @param workload Workload to detach.
         */
        private void detachWorkload(PipelineWorkload workload) {
            // It should be guaranteed that the workload can be finished
            // strictly after all subsequent workloads are scheduled.
            ChainDescriptor desc = activeChains.get(workload.workloadChainId());

            if (desc != null && desc.workloadCnt.decrementAndGet() == 0) {
                completedChainsCnt.incrementAndGet();

                activeChains.remove(desc.chainId);

                onChainCompleted(desc.chainId, desc.cacheName, desc.partId);

                CacheStatisticsDescriptor stat = cacheStatistics.get(desc.cacheName);

                stat.processedChainsCnt.incrementAndGet();
            }
        }

        /**
         * Redirects processing of skipped workload to the collector.
         *
         * @param workload Workload to skip.
         */
        private void skipWorkload(PipelineWorkload workload) {
            if (workload instanceof CachePartitionRequest) {
                CachePartitionRequest partReqWorkload = (CachePartitionRequest) workload;

                collector.appendSkippedPartition(partReqWorkload.cacheName(), partReqWorkload.partitionId());
            }
        }

        /**
         * Callback that is triggered when the partition completelly processed.
         *
         * @param chainId Chain id.
         * @param cacheName Cache name.
         * @param partId Partition id.
         */
        private void onChainCompleted(UUID chainId, String cacheName, int partId) {
            collector.onPartitionProcessed(cacheName, partId);
        }

        /**
         * Workload chain descriptor.
         */
        private class ChainDescriptor {
            /** Chain identifier. */
            private final UUID chainId;

            /** Cache name. */
            private final String cacheName;

            /** Partition identifier. */
            private final int partId;

            /** Workload counter. */
            private final AtomicInteger workloadCnt = new AtomicInteger();

            /**
             * Creates a new instance of chain descriptor.
             *
             * @param chainId Chain id.
             * @param cacheName Cache name.
             * @param partId Partition id.
             */
            ChainDescriptor(UUID chainId, String cacheName, int partId) {
                this.chainId = chainId;
                this.cacheName = cacheName;
                this.partId = partId;
            }
        }

        /**
         * Cache statistics descriptor.
         */
        private class CacheStatisticsDescriptor {
            /** Cache name. */
            final String cacheName;

            /** Number of primary partitions that are onwned by this node. */
            final AtomicInteger chainsCnt = new AtomicInteger();

            /** This field indicates that cache processing is completed. */
            final AtomicInteger processedChainsCnt = new AtomicInteger();

            /** Number of scanned primary keys that are onwned by this node. */
            final AtomicLong scannedPrimaryKeysCnt = new AtomicLong();

            /** Number of scanned backup keys that are onwned by this node. */
            final AtomicLong scannedBackupKeysCnt = new AtomicLong();

            /**
             * Creates a new instance of cache descriptor.
             *
             * @param cacheName Cache name.
             */
            CacheStatisticsDescriptor(String cacheName) {
                this.cacheName = cacheName;
            }
        }
    }
}
