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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
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
import org.apache.ignite.internal.processors.cache.KeyCacheObjectImpl;
import org.apache.ignite.internal.processors.cache.checker.objects.ExecutionResult;
import org.apache.ignite.internal.processors.cache.checker.objects.VersionedKey;
import org.apache.ignite.internal.processors.cache.checker.objects.RepairRequest;
import org.apache.ignite.internal.processors.cache.checker.objects.RepairResult;
import org.apache.ignite.internal.processors.cache.checker.objects.VersionedValue;
import org.apache.ignite.internal.processors.cache.verify.RepairAlgorithm;
import org.apache.ignite.internal.processors.cache.verify.RepairMeta;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;

import static org.apache.ignite.internal.processors.cache.checker.util.ConsistencyCheckUtils.calculateValueToFixWith;
import static org.apache.ignite.internal.processors.cache.checker.util.ConsistencyCheckUtils.unmarshalKey;

/**
 * Collects keys with their {@link GridCacheVersion} according to a recheck list.
 */
@GridInternal
public class RepairRequestTask extends ComputeTaskAdapter<RepairRequest, ExecutionResult<RepairResult>> {
    /**
     *
     */
    private static final long serialVersionUID = 0L;

    /**
     *
     */
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
    @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, RepairRequest arg)
        throws IgniteException {
        Map<ComputeJob, ClusterNode> jobs = new HashMap<>();

        repairReq = arg;

        Map<UUID, Map<KeyCacheObject, Map<UUID, VersionedValue>>> targetNodesToData = new HashMap<>();

        for (Map.Entry<KeyCacheObject, Map<UUID, VersionedValue>> dataEntry : repairReq.data().entrySet()) {
            KeyCacheObject keyCacheObj;

            GridCacheContext<Object, Object> ctx = ignite.cachex(repairReq.cacheName()).context();

            try {
                keyCacheObj = unmarshalKey(dataEntry.getKey(), ctx);
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Unable to unmarshal key=[" + dataEntry.getKey() + "], key is skipped.", e);

                continue;
            }

            int part = ctx.affinity().partition(keyCacheObj);

            UUID primaryNodeId = ctx.affinity().nodesByPartition(
                part, AffinityTopologyVersion.NONE).get(0).id();

            targetNodesToData.putIfAbsent(primaryNodeId, new HashMap<>());

            targetNodesToData.get(primaryNodeId).put(keyCacheObj, dataEntry.getValue());
        }

        for (ClusterNode node : subgrid) {
            Map<KeyCacheObject, Map<UUID, VersionedValue>> data = targetNodesToData.remove(node.id());

            if (data != null && !data.isEmpty()) {
                jobs.put(
                    new RepairJob(data.entrySet().stream().collect(
                        Collectors.toMap(
                            entry -> new VersionedKey(null, entry.getKey(), null),
                            Map.Entry::getValue)),
                        arg.cacheName(),
                        repairReq.repairAlg(),
                        repairReq.repairAttempt(),
                        repairReq.startTopologyVersion(),
                        repairReq.partitionId()),
                    node);
            }
        }

        if (!targetNodesToData.isEmpty()) {
            for (Map<KeyCacheObject, Map<UUID, VersionedValue>> data : targetNodesToData.values()) {
                ClusterNode node = subgrid.iterator().next();
                jobs.put(
                    new RepairJob(data.entrySet().stream().collect(
                        Collectors.toMap(
                            entry -> new VersionedKey(null, entry.getKey(), null),
                            Map.Entry::getValue)),
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
            if (result.getException() != null)
                return new ExecutionResult<>(result.getException().getMessage());

            ExecutionResult<RepairResult> excRes = result.getData();

            if (excRes.errorMessage() != null)
                return new ExecutionResult<>(excRes.errorMessage());

            RepairResult repairRes = excRes.result();

            aggregatedRepairRes.keysToRepair().putAll(repairRes.keysToRepair());
            aggregatedRepairRes.repairedKeys().putAll(repairRes.repairedKeys());
        }

        return new ExecutionResult<>(aggregatedRepairRes);
    }

    /**
     * Repair job.
     */
    protected static class RepairJob extends ComputeJobAdapter {
        /**
         *
         */
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
        private final Map<VersionedKey, Map<UUID, VersionedValue>> data;

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
        public RepairJob(Map<VersionedKey, Map<UUID, VersionedValue>> data, String cacheName,
            RepairAlgorithm repairAlg, int repairAttempt, AffinityTopologyVersion startTopVer, int partId) {
            this.data = data;
            this.cacheName = cacheName;
            this.repairAlg = repairAlg;
            this.repairAttempt = repairAttempt;
            this.startTopVer = startTopVer;
            this.partId = partId;
        }

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override public ExecutionResult<RepairResult> execute() throws IgniteException {
            Map<VersionedKey, Map<UUID, VersionedValue>> keysToRepairWithNextAttempt = new HashMap<>();

            Map<VersionedKey, RepairMeta> repairedKeys =
                new HashMap<>();

            GridCacheContext ctx = ignite.cachex(cacheName).context();
            CacheObjectContext cacheObjCtx = ctx.cacheObjectContext();

            // TODO: 02.12.19
            final int rmvQueueMaxSize = 32;
            final int ownersNodesSize = owners(ctx);

            for (Map.Entry<VersionedKey, Map<UUID, VersionedValue>> dataEntry : data.entrySet()) {
                try {
                    Object key = keyValue(ctx, dataEntry.getKey().key());
                    Map<UUID, VersionedValue> nodeToVersionedValues = dataEntry.getValue();

                    UUID primaryUUID = primaryNodeId(ctx, key);

                    RepairEntryProcessor.RepairStatus keyWasSuccessfullyFixed;

                    CacheObject valToFixWith = null;

                    RepairAlgorithm usedRepairAlg = repairAlg;

                    // Are there any nodes with missing key?
                    if (dataEntry.getValue().size() != ownersNodesSize && repairAttempt != MAX_REPAIR_ATTEMPTS) {
                        if (repairAlg == RepairAlgorithm.PRINT_ONLY)
                            keyWasSuccessfullyFixed = RepairEntryProcessor.RepairStatus.SUCCESS;
                        else {
                            valToFixWith = calculateValueToFixWith(
                                repairAlg,
                                nodeToVersionedValues,
                                primaryUUID,
                                cacheObjCtx,
                                ownersNodesSize);

                            keyWasSuccessfullyFixed = ignite.cache(cacheName).withKeepBinary().<RepairEntryProcessor.RepairStatus>invoke(
                                key,
                                new RepairEntryProcessor(
                                    valToFixWith,
                                    nodeToVersionedValues,
                                    rmvQueueMaxSize,
                                    false,
                                    startTopVer));
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

                            keyWasSuccessfullyFixed = (RepairEntryProcessor.RepairStatus)ignite.cache(cacheName).withKeepBinary().invoke(
                                key,
                                new RepairEntryProcessor(
                                    valToFixWith,
                                    nodeToVersionedValues,
                                    rmvQueueMaxSize,
                                    true,
                                    startTopVer));
                        }
                        else {
                            usedRepairAlg = RepairAlgorithm.LATEST;

                            valToFixWith = calculateValueToFixWith(
                                RepairAlgorithm.LATEST,
                                nodeToVersionedValues,
                                primaryUUID,
                                cacheObjCtx,
                                ownersNodesSize);

                            keyWasSuccessfullyFixed = (RepairEntryProcessor.RepairStatus)ignite.cache(cacheName).withKeepBinary().invoke(
                                key,
                                new RepairEntryProcessor(
                                    valToFixWith,
                                    nodeToVersionedValues,
                                    rmvQueueMaxSize,
                                    false,
                                    startTopVer));
                        }
                    }

                    if (keyWasSuccessfullyFixed == RepairEntryProcessor.RepairStatus.FAIL)
                        keysToRepairWithNextAttempt.put(dataEntry.getKey(), dataEntry.getValue());
                    else {
                        repairedKeys.put(
                            dataEntry.getKey(),
                            new RepairMeta(
                                true,
                                valToFixWith,
                                usedRepairAlg,
                                dataEntry.getValue()
                            ));
                    }
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Key [" + dataEntry.getKey().key() + "] was skipped during repair phase.",
                        e);
                }
            }

            return new ExecutionResult<>(new RepairResult(keysToRepairWithNextAttempt, repairedKeys));
        }

        /**
         *
         */
        protected UUID primaryNodeId(GridCacheContext ctx, Object key) {
            return ctx.affinity().nodesByKey(key, startTopVer).get(0).id();
        }

        /**
         *
         */
        protected int owners(GridCacheContext ctx) {
            return ctx.topology().owners(partId, startTopVer).size();
        }

        /**
         *
         */
        protected Object keyValue(GridCacheContext ctx, KeyCacheObject key) throws IgniteCheckedException {
            KeyCacheObject unmarshalledKey = unmarshalKey(key, ctx);

            if (unmarshalledKey instanceof KeyCacheObjectImpl)
                return unmarshalledKey.value(ctx.cacheObjectContext(), false);

            return key;
        }
    }
}
