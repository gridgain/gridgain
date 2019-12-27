/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.checker.tasks;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheEntry;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.checker.objects.ExecutionResult;
import org.apache.ignite.internal.processors.cache.checker.objects.PartitionKeyVersion;
import org.apache.ignite.internal.processors.cache.checker.objects.RepairRequest;
import org.apache.ignite.internal.processors.cache.checker.objects.RepairResult;
import org.apache.ignite.internal.processors.cache.checker.objects.VersionedValue;
import org.apache.ignite.internal.processors.cache.verify.RepairAlgorithm;
import org.apache.ignite.internal.processors.cache.verify.RepairMeta;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_CACHE_REMOVED_ENTRIES_TTL;
import static org.apache.ignite.internal.processors.cache.checker.util.ConsistencyCheckUtils.unmarshalKey;

/**
 * Collects keys with their {@link GridCacheVersion} according to a recheck list.
 */
public class RepairRequestTask extends ComputeTaskAdapter<RepairRequest, ExecutionResult<RepairResult>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    public static final int MAX_REPAIR_ATTEMPTS = 3;

    /** Injected logger. */
    @SuppressWarnings("unused")
    @LoggerResource
    private IgniteLogger log;

    /** Ignite instance. */
    @SuppressWarnings("unused")
    @IgniteInstanceResource
    private IgniteEx ignite;

    /** Repair request. */
    private RepairRequest repairReq;

    /** {@inheritDoc} */
    @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,  RepairRequest arg)
        throws IgniteException {
        Map<ComputeJob, ClusterNode> jobs = new HashMap<>();

        repairReq = arg;

        Map<UUID, Map<KeyCacheObject, Map<UUID, VersionedValue>>> targetNodesToData = new HashMap<>();

        for (Map.Entry<KeyCacheObject, Map<UUID, VersionedValue>> dataEntry: repairReq.data().entrySet()) {
            KeyCacheObject keyCacheObject;

            try {
                keyCacheObject = unmarshalKey(dataEntry.getKey(), ignite.cachex(repairReq.cacheName()).context());
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Unable to unmarshal key=[" + dataEntry.getKey() + "], key is skipped.");

                continue;
            }

            Object key = keyCacheObject.value(
                ignite.cachex(repairReq.cacheName()).context().cacheObjectContext(), false);

            UUID primaryNodeId = ignite.affinity(
                repairReq.cacheName()).mapKeyToPrimaryAndBackups(key).iterator().next().id();

            targetNodesToData.putIfAbsent(primaryNodeId, new HashMap<>());

            targetNodesToData.get(primaryNodeId).put(keyCacheObject, dataEntry.getValue());
        }

        for (ClusterNode node : subgrid) {
            Map<KeyCacheObject, Map<UUID, VersionedValue>> data = targetNodesToData.remove(node.id());

            if (data != null && !data.isEmpty()) {
                // TODO: 03.12.19 PartitionKeyVersion is used in order to prevent finishUnmarshal problem, cause actually we only need keyCacheObject,
                // TODO: 03.12.19 consider using better wrapper here.
                jobs.put(
                    new RepairJob(data.entrySet().stream().collect(
                        Collectors.toMap(
                            entry -> new PartitionKeyVersion(null, entry.getKey(), null),
                            entry -> entry.getValue())),
                        arg.cacheName(),
                        repairReq.repairAlg(),
                        repairReq.repairAttempt(),
                        repairReq.startTopologyVersion(),
                        repairReq.partitionId()),
                    node);
            }
        }

        if (!targetNodesToData.isEmpty()) {
            // TODO: 03.12.19 Print warning that sort of affinity awareness is not possible, so that for all other data random node will be used.
            for (Map<KeyCacheObject, Map<UUID, VersionedValue>> data : targetNodesToData.values()) {
                // TODO: 03.12.19 Use random node instead.
                ClusterNode node = subgrid.iterator().next();
                // TODO: 03.12.19 PartitionKeyVersion is used in order to prevent finishUnmarshal problem, cause actually we only need keyCacheObject,
                // TODO: 03.12.19consider using better wrapper here.
                jobs.put(
                    new RepairJob(data.entrySet().stream().collect(
                        Collectors.toMap(
                            entry -> new PartitionKeyVersion(null, entry.getKey(), null),
                            entry -> entry.getValue())),
                        arg.cacheName(),
                        repairReq.repairAlg(),
                        repairReq.repairAttempt(),
                        repairReq.startTopologyVersion(),
                        repairReq.partitionId()),
                    node);
            }
        }

        return jobs;
    }

    /** {@inheritDoc} */
    @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) {
        ComputeJobResultPolicy superRes = super.result(res, rcvd);

        // Deny failover.
        if (superRes == ComputeJobResultPolicy.FAILOVER) {
            superRes = ComputeJobResultPolicy.WAIT;

            log.warning("CollectPartitionEntryHashesJob failed on node " +
                "[consistentId=" + res.getNode().consistentId() + "]", res.getException());
        }

        return superRes;
    }

    /** {@inheritDoc} */
    @Override public ExecutionResult<RepairResult> reduce(
        List<ComputeJobResult> results) throws IgniteException {
        RepairResult aggregatedRepairRes = new RepairResult();

        for (ComputeJobResult result : results) {
            if(result.getException() != null)
                return new ExecutionResult<>(result.getException().getMessage());

            ExecutionResult<RepairResult> excRes = result.getData();

            if(excRes.getErrorMessage() != null)
                return new ExecutionResult<>(excRes.getErrorMessage());

            RepairResult repairRes = excRes.getResult();

            aggregatedRepairRes.keysToRepair().putAll(repairRes.keysToRepair());
            aggregatedRepairRes.repairedKeys().putAll(repairRes.repairedKeys());
        }

        return new ExecutionResult<>(aggregatedRepairRes);
    }

    /**
     * Repair job.
     */
    protected static class RepairJob extends ComputeJobAdapter {
        /** */
        private static final long serialVersionUID = 0L;

        /** Ignite instance. */
        @SuppressWarnings("unused")
        @IgniteInstanceResource
        private IgniteEx ignite;

        /** Injected logger. */
        @SuppressWarnings("unused")
        @LoggerResource
        private IgniteLogger log;

        /** Partition key. */
        private final Map<PartitionKeyVersion, Map<UUID, VersionedValue>> data;

        /** Cache name. */
        private String cacheName;

        /** Repair attempt. */
        private int repairAttempt;

        /** Repair algorithm to use in case of fixing doubtful keys. */
        private RepairAlgorithm repairAlg;

        /** Start topology version. */
        private AffinityTopologyVersion startTopVer;

        /** Partition id. */
        private int partId;

        /**
         * Constructor.
         *
         * @param data Keys to repair with corresponding values and version per node.
         * @param cacheName Cache name.
         * @param repairAlg Repair algorithm to use in case of fixing doubtful keys.
         * @param repairAttempt Repair attempt.
         * @param startTopVer Start topology version.
         * @param partId Partition Id.
         */
        @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
        public RepairJob(Map<PartitionKeyVersion, Map<UUID, VersionedValue>> data, String cacheName,
            RepairAlgorithm repairAlg, int repairAttempt, AffinityTopologyVersion startTopVer, int partId) {
            this.data = data;
            this.cacheName = cacheName;
            this.repairAlg = repairAlg;
            this.repairAttempt = repairAttempt;
            this.startTopVer = startTopVer;
            this.partId = partId;
        }

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked") @Override public ExecutionResult<RepairResult> execute() throws IgniteException {
            Map<PartitionKeyVersion, Map<UUID, VersionedValue>> keysToRepairWithNextAttempt = new HashMap<>();

            Map<T2<PartitionKeyVersion, RepairMeta>, Map<UUID, VersionedValue>> repairedKeys =
                new HashMap<>();

            for (Map.Entry<PartitionKeyVersion, Map<UUID, VersionedValue>> dataEntry : data.entrySet()) {
                try {
                    GridCacheContext ctx = ignite.cachex(cacheName).context();

                    CacheObjectContext cacheObjCtx = ctx.cacheObjectContext();

                    Object key = unmarshalKey(dataEntry.getKey().getKey(), ctx).value(cacheObjCtx, false);

                    Map<UUID, VersionedValue> nodeToVersionedValues = dataEntry.getValue();

                    int ownersNodesSize = ctx.topology().owners(partId, startTopVer).size();

                    UUID primaryUUID = ctx.affinity().nodesByKey(key,startTopVer).get(0).id();

                    Boolean keyWasSuccessfullyFixed;

                    // TODO: 02.12.19
                    int rmvQueueMaxSize = 32;

                    CacheObject valToFixWith = null;

                    // Are there any nodes with missing key?
                    if (dataEntry.getValue().size() != ownersNodesSize) {
                        if (repairAlg == RepairAlgorithm.PRINT_ONLY)
                            keyWasSuccessfullyFixed = true;
                        else {
                            valToFixWith = calculateValueToFixWith(
                                repairAlg,
                                nodeToVersionedValues,
                                primaryUUID,
                                cacheObjCtx,
                                ownersNodesSize);

                            keyWasSuccessfullyFixed = ignite.cache(cacheName).<Boolean>invoke(
                                key,
                                new RepairEntryProcessor(
                                    valToFixWith,
                                    nodeToVersionedValues,
                                    rmvQueueMaxSize,
                                    true,
                                    startTopVer));

                            assert keyWasSuccessfullyFixed;
                        }
                    }
                    else {
                        // Is it last repair attempt?
                        if (repairAttempt == MAX_REPAIR_ATTEMPTS) {
                            valToFixWith = calculateValueToFixWith(
                                repairAlg,
                                nodeToVersionedValues,
                                primaryUUID,
                                cacheObjCtx,
                                ownersNodesSize);

                            keyWasSuccessfullyFixed = (Boolean)ignite.cache(cacheName).invoke(
                                key,
                                new RepairEntryProcessor(
                                    valToFixWith,
                                    nodeToVersionedValues,
                                    rmvQueueMaxSize,
                                    true,
                                    startTopVer));

                            assert keyWasSuccessfullyFixed;
                        }
                        else {
                            valToFixWith = calculateValueToFixWith(
                                RepairAlgorithm.MAX_GRID_CACHE_VERSION,
                                nodeToVersionedValues,
                                primaryUUID,
                                cacheObjCtx,
                                ownersNodesSize);

                            keyWasSuccessfullyFixed = (Boolean)ignite.cache(cacheName).invoke(
                                key,
                                new RepairEntryProcessor(
                                    valToFixWith,
                                    nodeToVersionedValues,
                                    rmvQueueMaxSize,
                                    false,
                                    startTopVer));
                        }
                    }

                    if (!keyWasSuccessfullyFixed)
                        keysToRepairWithNextAttempt.put(dataEntry.getKey(), dataEntry.getValue());
                    else {
                        repairedKeys.put(
                            new T2<>(
                                dataEntry.getKey(),
                                new RepairMeta(
                                    true,
                                    valToFixWith,
                                    repairAlg)
                            ),
                            dataEntry.getValue());
                    }
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Key [" + dataEntry.getKey().getKey() + "] was skipped during repair phase.",
                        e);
                }
            }

            return new ExecutionResult<>(new RepairResult(keysToRepairWithNextAttempt, repairedKeys));
        }

        /**
         * Calculate value
         * @param repairAlg RepairAlgorithm
         * @param nodeToVersionedValues Map of nodes to corresponding values with values.
         * @param primaryNodeID Primary node id.
         * @param cacheObjCtx Cache object context.
         * @param affinityNodesCnt nodes count according to affinity.
         * @return Value to repair with.
         * @throws IgniteCheckedException If failed to retrieve value from value CacheObject.
         */
        @Nullable protected CacheObject calculateValueToFixWith(RepairAlgorithm repairAlg,
            Map<UUID, VersionedValue> nodeToVersionedValues,
            UUID primaryNodeID,
            CacheObjectContext cacheObjCtx,
            int affinityNodesCnt) throws IgniteCheckedException {
            CacheObject valToFixWith = null;

            switch (repairAlg) {
                case PRIMARY:
                    VersionedValue versionedVal = nodeToVersionedValues.get(primaryNodeID);

                    return versionedVal != null ? versionedVal.value() : null;

                case MAJORITY:
                    if (affinityNodesCnt > nodeToVersionedValues.size() * 2)
                        return null;

                    Map<String, T2<Integer, CacheObject>> majorityCntr = new HashMap<>();

                    majorityCntr.put("", new T2<>(affinityNodesCnt - nodeToVersionedValues.size(), null));

                    for (VersionedValue versionedValue : nodeToVersionedValues.values()) {
                        byte[] valBytes = versionedValue.value().valueBytes(cacheObjCtx);

                        String valBytesStr = Arrays.toString(valBytes);

                        if (majorityCntr.putIfAbsent(valBytesStr, new T2<>(1, versionedValue.value())) == null)
                            continue;

                        T2<Integer, CacheObject> valTuple = majorityCntr.get(valBytesStr);

                        valTuple.set1(valTuple.getKey() + 1);
                    }

                    int maxMajority = -1;

                    for (T2<Integer, CacheObject> majorityValues : majorityCntr.values()) {
                        if (majorityValues.get1() > maxMajority) {
                            maxMajority = majorityValues.get1();
                            valToFixWith = majorityValues.get2();
                        }
                    }

                    return valToFixWith;

                case MAX_GRID_CACHE_VERSION:
                    GridCacheVersion maxVer = new GridCacheVersion(0, 0, 0);

                    for (VersionedValue versionedValue : nodeToVersionedValues.values()) {
                        if (versionedValue.version().compareTo(maxVer) > 0) {
                            maxVer = versionedValue.version();

                            valToFixWith = versionedValue.value();
                        }
                    }

                    return valToFixWith;

                default:
                    throw new IllegalArgumentException("Unsupported repair algorithm=[" + repairAlg + ']');
            }
        }

        /** Entry processor to repair inconsistent entries. */
        private static class RepairEntryProcessor implements EntryProcessor {

            /** Value to set. */
            private Object val;

            /** Map of nodes to corresponding versioned values */
            Map<UUID, VersionedValue> data;

            /** deferred delete queue max size. */
            private long rmvQueueMaxSize;

            /** Force repair flag. */
            private boolean forceRepair;

            /** Start topology version. */
            private AffinityTopologyVersion startTopVer;

            /** */
            @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
            public RepairEntryProcessor(
                Object val,
                Map<UUID, VersionedValue> data,
                long rmvQueueMaxSize,
                boolean forceRepair,
                AffinityTopologyVersion startTopVer) {
                this.val = val;
                this.data = data;
                this.rmvQueueMaxSize = rmvQueueMaxSize;
                this.forceRepair = forceRepair;
                this.startTopVer = startTopVer;
            }

            /**
             * Do repair logic.
             * @param entry Entry to fix.
             * @param arguments Arguments.
             * @return {@code True} if was successfully repaired, {@code False} otherwise.
             * @throws EntryProcessorException If failed.
             */
            @SuppressWarnings("unchecked")
            @Override public Object process(MutableEntry entry, Object... arguments) throws EntryProcessorException {
                CacheEntry verEntry = (CacheEntry)entry.unwrap(CacheEntry.class);

                Comparable currKeyGridCacheVer = verEntry.version();

                GridCacheContext cctx = (GridCacheContext)entry.unwrap(GridCacheContext.class);

                AffinityTopologyVersion currTopVer = cctx.affinity().affinityTopologyVersion();

                if (!cctx.shared().exchange().lastAffinityChangedTopologyVersion((currTopVer)).equals(startTopVer))
                    throw new EntryProcessorException("Topology version was changed");

                UUID locNodeId = cctx.localNodeId();

                VersionedValue versionedVal = data.get(locNodeId);

                if (forceRepair) {
                    if (val == null)
                        entry.remove();
                    else
                        entry.setValue(val);

                    return true;
                }

                if (versionedVal != null) {
                    if (currKeyGridCacheVer.compareTo(versionedVal.version()) == 0) {
                        if (val == null)
                            entry.remove();
                        else
                            entry.setValue(val);

                        return true;
                    }

                    // TODO: 23.12.19 Add optimizations here
                }
                else {
                    if (currKeyGridCacheVer.compareTo(new GridCacheVersion(0, 0, 0)) == 0) {
                        boolean inEntryTTLBounds =
                            (System.currentTimeMillis() - versionedVal.recheckStartTime()) <
                                Long.getLong(IGNITE_CACHE_REMOVED_ENTRIES_TTL);

                        long currUpdateCntr = cctx.topology().localPartition(
                            cctx.cache().affinity().partition(entry.getKey())).updateCounter();

                        boolean inDeferredDelQueueBounds = ((currUpdateCntr - versionedVal.updateCounter()) <
                            rmvQueueMaxSize);

                        if ((inEntryTTLBounds && inDeferredDelQueueBounds)) {
                            if (val == null)
                                entry.remove();
                            else
                                entry.setValue(val);

                            return true;
                        }

                        return false;
                    }
                }

                return false;
            }
        }
    }
}
