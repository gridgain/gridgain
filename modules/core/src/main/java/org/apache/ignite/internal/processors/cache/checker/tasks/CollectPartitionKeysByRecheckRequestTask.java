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

package org.apache.ignite.internal.processors.cache.checker.tasks;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.checker.objects.ExecutionResult;
import org.apache.ignite.internal.processors.cache.checker.objects.VersionedEntry;
import org.apache.ignite.internal.processors.cache.checker.objects.RecheckRequest;
import org.apache.ignite.internal.processors.cache.checker.objects.VersionedValue;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;

import static org.apache.ignite.internal.processors.cache.checker.util.ConsistencyCheckUtils.unmarshalKey;

/**
 * Collects keys with their {@link GridCacheVersion} according to a recheck list.
 */
@GridInternal
public class CollectPartitionKeysByRecheckRequestTask extends ComputeTaskAdapter<RecheckRequest, ExecutionResult<Map<KeyCacheObject, Map<UUID, VersionedValue>>>> {
    /**
     *
     */
    private static final long serialVersionUID = 0L;

    /** Injected logger. */
    @LoggerResource
    private IgniteLogger log;

    /** Ignite instance. */
    @IgniteInstanceResource
    private IgniteEx ignite;

    /**
     * Recheck request.
     */
    private RecheckRequest recheckReq;

    /** {@inheritDoc} */
    @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
        RecheckRequest arg) throws IgniteException {

        Map<ComputeJob, ClusterNode> jobs = new HashMap<>();

        recheckReq = arg;

        for (ClusterNode node : subgrid)
            jobs.put(new CollectRecheckJob(arg), node);

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
    @Override public ExecutionResult<Map<KeyCacheObject, Map<UUID, VersionedValue>>> reduce(
        List<ComputeJobResult> results) throws IgniteException {
        Map<KeyCacheObject, Map<UUID, VersionedValue>> res = new HashMap<>();

        GridCacheContext<Object, Object> ctx = ignite.cachex(recheckReq.cacheName()).context();

        for (ComputeJobResult result : results) {
            if (result.getException() != null)
                return new ExecutionResult<>(result.getException().getMessage());

            ExecutionResult<List<VersionedEntry>> excRes = result.getData();

            if (excRes.errorMessage() != null)
                return new ExecutionResult<>(excRes.errorMessage());

            List<VersionedEntry> partKeys = excRes.result();

            for (VersionedEntry key : partKeys) {
                try {
                    KeyCacheObject keyObj = unmarshalKey(key.key(), ctx);
                    res.computeIfAbsent(keyObj, k -> new HashMap<>()).put(
                        key.nodeId(),
                        new VersionedValue(key.val(), key.ver(), key.updateCntr(), key.recheckStartTime())
                    );
                }
                catch (Exception e) {
                    U.error(log, e.getMessage(), e);

                    return new ExecutionResult<>(e.getMessage());
                }
            }
        }

        return new ExecutionResult<>(res);
    }

    /**
     *
     */
    public static class CollectRecheckJob extends ReconciliationResourceLimitedJob {
        /**
         *
         */
        private static final long serialVersionUID = 0L;

        /** Partition key. */
        private final RecheckRequest recheckReq;

        /**
         * @param recheckReq Recheck request.
         */
        public CollectRecheckJob(RecheckRequest recheckReq) {
            this.recheckReq = recheckReq;
        }

        /** {@inheritDoc} */
        @Override protected long sessionId() {
            return recheckReq.sessionId();
        }

        /** {@inheritDoc} */
        @Override protected ExecutionResult<List<VersionedEntry>> execute0() {
            GridCacheContext<Object, Object> cctx = ignite.context().cache().cache(recheckReq.cacheName()).context();

            CacheGroupContext grpCtx = cctx.group();

            GridDhtLocalPartition part = grpCtx.topology().localPartition(recheckReq.partitionId());

            assert part != null;

            part.reserve();

            List<VersionedEntry> recheckedKeys = new ArrayList<>();

            long updateCntr = part.updateCounter();
            long recheckStartTime = System.currentTimeMillis();

            try {
                for (KeyCacheObject recheckKey : recheckReq.recheckKeys()) {
                    try {
                        KeyCacheObject key = unmarshalKey(recheckKey, cctx);

                        CacheDataRow row = grpCtx.offheap().dataStore(part).find(cctx, key);

                        if (row != null)
                            recheckedKeys.add(new VersionedEntry(ignite.localNode().id(), row.key(), row.version(), row.value(), updateCntr, recheckStartTime));
                    }
                    catch (IgniteCheckedException e) {
                        String errMsg = "Recheck key [key=" + recheckKey + "] was skipped.";

                        U.error(log, errMsg, e);

                        return new ExecutionResult<>(errMsg);
                    }
                }

                return new ExecutionResult<>(recheckedKeys);
            }
            finally {
                part.release();
            }
        }
    }
}

