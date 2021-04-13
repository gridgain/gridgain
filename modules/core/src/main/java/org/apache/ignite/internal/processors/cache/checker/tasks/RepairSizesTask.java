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
import org.apache.ignite.internal.processors.cache.checker.objects.RepairSizesRequest;
import org.apache.ignite.internal.processors.cache.checker.objects.RepairSizesResult;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;

import static org.apache.ignite.internal.processors.cache.checker.util.ConsistencyCheckUtils.unmarshalKey;

/**
 * Collects keys with their {@link GridCacheVersion} according to a recheck list.
 */
@GridInternal
public class RepairSizesTask extends ComputeTaskAdapter<RepairSizesRequest, ExecutionResult<RepairSizesResult>> {
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
    private RepairSizesRequest repairSizesRequest;

    /** {@inheritDoc} */
    @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, RepairSizesRequest arg)
        throws IgniteException {
        System.out.println("qdsffter " + Thread.currentThread().getName());
        Map<ComputeJob, ClusterNode> jobs = new HashMap<>();

        Map<Integer, Map<Integer, Map<UUID, Long>>> partSizesMap = arg.partSizesMap();

        GridCacheContext<Object, Object> cctx = ignite.context().cache().cache(arg.cacheName()).context();

        CacheGroupContext grpCtx = cctx.group();

        int cacheId = cctx.cacheId();

        Map<Integer, Map<UUID, Long>> partSizes = partSizesMap.get(cacheId);

        Map<UUID, Long> sizes = partSizes.get(arg.partitionId());

        for (ClusterNode node : subgrid) {
            Long size = sizes.remove(node.id());

            jobs.put(
                new RepairSizesJob(
                    arg.cacheName(),
                    arg.startTopologyVersion(),
                    cacheId,
                    arg.partitionId(),
                    size
                ),
                node);
        }

        if (!sizes.isEmpty()) {
            for (ClusterNode node : subgrid) {
                Long size = sizes.remove(node.id());

                jobs.put(
                    new RepairSizesJob(
                        arg.cacheName(),
                        arg.startTopologyVersion(),
                        cacheId,
                        arg.partitionId(),
                        size
                    ),
                    subgrid.iterator().next());
            }
        }



//        for (Map.Entry<Integer, Map<UUID, Long>> entry : partSizes.entrySet()) {
//            Integer partId = entry.getKey();
//            Map<UUID, Long> sizes = entry.getValue();
//
//            for (ClusterNode node : subgrid) {
//                Long size = sizes.remove(node.id());
//
//                jobs.put(
//                    new RepairSizesJob(
//                        arg.cacheName(),
//                        arg.startTopologyVersion(),
//                        cacheId,
//                        partId,
//                        size
//                        ),
//                    node);
//            }
//
//            if (!sizes.isEmpty()) {
//                for (ClusterNode node : subgrid) {
//                    Long size = sizes.remove(node.id());
//
//                    jobs.put(
//                        new RepairSizesJob(
//                            arg.cacheName(),
//                            arg.startTopologyVersion(),
//                            cacheId,
//                            partId,
//                            size
//                        ),
//                        subgrid.iterator().next());
//                }
//            }
//
//        }

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
    @Override public ExecutionResult<RepairSizesResult> reduce(
        List<ComputeJobResult> results) throws IgniteException {

        return new ExecutionResult<>(new RepairSizesResult());
    }

    /**
     * Repair job.
     */
    protected static class RepairSizesJob extends ComputeJobAdapter {
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

        /** Cache name. */
        private String cacheName;

        /** Start topology version. */
        private AffinityTopologyVersion startTopVer;

        /** Partition id. */
        private int partId;

        /** */
        private int cacheId;

        /** */
        private long partSize;

        /**
         * Constructor.
         *
         * @param cacheName Cache name.
         * @param startTopVer Start topology version.
         * @param partId Partition Id.
         */
        @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
        public RepairSizesJob(String cacheName,
            AffinityTopologyVersion startTopVer, int cacheId, int partId, long partSize) {
            this.cacheName = cacheName;
            this.startTopVer = startTopVer;
            this.cacheId = cacheId;
            this.partId = partId;
            this.partSize = partSize;
        }

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override public ExecutionResult<RepairSizesResult> execute() throws IgniteException {
            GridCacheContext<Object, Object> cctx = ignite.context().cache().cache(cacheName).context();
            CacheGroupContext grpCtx = cctx.group();
//            grpCtx.offheap().dataStore(partId);

//            ((internalCache(ignite.cache(cacheName)).context().topology().localPartition(0).dataStore())).reconciliationCtx().storageSizeDelta(cacheId);

            IgniteCacheOffheapManager.CacheDataStore dataStore = grpCtx.topology().localPartition(partId).dataStore();

            System.out.println("qvredfgs RepairSizesJob flushReconciliationResult");

            dataStore.flushReconciliationResult();

            return new ExecutionResult<>(new RepairSizesResult());
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
