/*
 * Copyright 2021 GridGain Systems, Inc. and Contributors.
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
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObjectImpl;
import org.apache.ignite.internal.processors.cache.checker.objects.ExecutionResult;
import org.apache.ignite.internal.processors.cache.checker.objects.NodePartitionSize;
import org.apache.ignite.internal.processors.cache.checker.objects.PartitionSizeRepairJobResult;
import org.apache.ignite.internal.processors.cache.checker.objects.PartitionSizeRepairRequest;
import org.apache.ignite.internal.processors.cache.checker.objects.PartitionSizeRepairTaskResult;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;

import static org.apache.ignite.internal.processors.cache.checker.util.ConsistencyCheckUtils.unmarshalKey;

/**
 * Collects keys with their {@link GridCacheVersion} according to a recheck list.
 */
@GridInternal
public class PartitionSizeRepairRequestTask extends ComputeTaskAdapter<PartitionSizeRepairRequest, ExecutionResult<PartitionSizeRepairTaskResult>> {
    /**
     *
     */
    private static final long serialVersionUID = 0L;

    /** Injected logger. */
    @SuppressWarnings("unused")
    @LoggerResource
    private IgniteLogger log;

    /** Ignite instance. */
    @SuppressWarnings("unused")
    @IgniteInstanceResource
    private IgniteEx ignite;

    /** Repair request. */
    private PartitionSizeRepairRequest repairReq;

    /** {@inheritDoc} */
    @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, PartitionSizeRepairRequest arg)
        throws IgniteException {
        Map<ComputeJob, ClusterNode> jobs = new HashMap<>();

        repairReq = arg;

        for (ClusterNode node : subgrid) {
                jobs.put(
                    new PartitionSizeRepairJob(
                        arg.cacheName(),
                        repairReq,
                        repairReq.startTopologyVersion(),
                        repairReq.partitionId(),
                        repairReq.isRepair(),
                        repairReq.partSizesMap()),
                    node);
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
    @Override public ExecutionResult<PartitionSizeRepairTaskResult> reduce(
        List<ComputeJobResult> results) throws IgniteException {
        PartitionSizeRepairTaskResult aggregatedRepairRes = new PartitionSizeRepairTaskResult();

        for (ComputeJobResult result : results) {
            if (result.getException() != null)
                return new ExecutionResult<>(result.getException().getMessage());

            ExecutionResult<PartitionSizeRepairJobResult> excRes = result.getData();

            if (excRes.errorMessage() != null)
                return new ExecutionResult<>(excRes.errorMessage());

            aggregatedRepairRes.getSizeMap().put(result.getNode().id(), excRes.result().getNodePartitionSize());
        }

        return new ExecutionResult<>(aggregatedRepairRes);
    }

    /**
     * Repair job.
     */
    protected static class PartitionSizeRepairJob extends ComputeJobAdapter {
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

        /** */
        private PartitionSizeRepairRequest repairReq;

        /** Cache name. */
        private String cacheName;

        /** Start topology version. */
        private AffinityTopologyVersion startTopVer;

        /** Partition id. */
        private int partId;

        private boolean repair;

        /** */
        private Map<UUID, NodePartitionSize> partSizesMap;

        /**
         * Constructor.
         *
         * @param cacheName Cache name.
         * @param startTopVer Start topology version.
         * @param partId Partition Id.
         */
        @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
        public PartitionSizeRepairJob(String cacheName,
            PartitionSizeRepairRequest repairReq,
            AffinityTopologyVersion startTopVer,
            int partId,
            boolean repair,
            Map<UUID, NodePartitionSize> partSizesMap
        ) {
            this.repairReq = repairReq;
            this.cacheName = cacheName;
            this.startTopVer = startTopVer;
            this.partId = partId;
            this.repair = repair;
            this.partSizesMap = partSizesMap;
        }

        /** {@inheritDoc} */
//        @SuppressWarnings("unchecked")
        @Override public ExecutionResult<PartitionSizeRepairJobResult> execute() throws IgniteException {
            GridCacheContext<Object, Object> cctx = ignite.context().cache().cache(repairReq.cacheName()).context();

            CacheGroupContext grpCtx = cctx.group();

            int cacheId = grpCtx.sharedGroup() ? cctx.cacheId() : CU.UNDEFINED_CACHE_ID;

            GridDhtLocalPartition part = grpCtx.topology().localPartition(repairReq.partitionId());

            IgniteCacheOffheapManager.CacheDataStore cacheDataStore = grpCtx.offheap().dataStore(part);

            NodePartitionSize nodePartitionSize = partSizesMap.get(ignite.localNode().id());

            assert part != null;

            part.reserve();

            try {
                if (repair)
                    cacheDataStore.flushReconciliationResult(cacheId, nodePartitionSize, true);
                else
                    cacheDataStore.flushReconciliationResult(cacheId, nodePartitionSize, false);
            }
            catch (Exception e) {
                String errMsg = "Batch [" + repairReq + "] can't processed. Broken cursor.";

                log.error(errMsg, e);

                return new ExecutionResult<>(errMsg + " " + e.getMessage());
            }
            finally {
                part.release();
            }

            PartitionSizeRepairJobResult result = new PartitionSizeRepairJobResult();

            result.setNodePartitionSize(nodePartitionSize);

            return new ExecutionResult<>(result);
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
