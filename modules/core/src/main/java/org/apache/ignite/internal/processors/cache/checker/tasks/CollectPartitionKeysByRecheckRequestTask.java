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
import java.util.Collections;
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
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.checker.objects.PartitionKeyVersion;
import org.apache.ignite.internal.processors.cache.checker.objects.RecheckRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;

import static org.apache.ignite.internal.processors.cache.checker.util.ConsistencyCheckUtils.unmarshalKey;

/**
 * Collects keys with their {@link GridCacheVersion} according to a recheck list.
 */
public class CollectPartitionKeysByRecheckRequestTask extends ComputeTaskAdapter<RecheckRequest, Map<KeyCacheObject, Map<UUID, GridCacheVersion>>> {
    /** Injected logger. */
    @LoggerResource
    private IgniteLogger log;

    /** Ignite instance. */
    @IgniteInstanceResource
    private IgniteEx ignite;

    /**
     *
     */
    private RecheckRequest recheckRequest;

    /** {@inheritDoc} */
    @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
        RecheckRequest arg) throws IgniteException {

        Map<ComputeJob, ClusterNode> jobs = new HashMap<>();

        recheckRequest = arg;

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
    @Override public Map<KeyCacheObject, Map<UUID, GridCacheVersion>> reduce(
        List<ComputeJobResult> results) throws IgniteException {
        Map<KeyCacheObject, Map<UUID, GridCacheVersion>> res = new HashMap<>();

        GridCacheContext<Object, Object> ctx = ignite.cachex(recheckRequest.cacheName()).context();

        for (ComputeJobResult result : results) {
            List<PartitionKeyVersion> partKeys = result.getData();

            for (PartitionKeyVersion key : partKeys) {
                try {
                    KeyCacheObject keyObj = unmarshalKey(key.getKey(), ctx);
                    res.computeIfAbsent(keyObj, k -> new HashMap<>()).put(key.getNodeId(), key.getVersion());
                }
                catch (Exception e) {
                    U.error(log, "Updated recheck key [" + key + "] was skipped.", e);
                }
            }
        }

        return res;
    }

    /**
     *
     */
    public static class CollectRecheckJob extends ComputeJobAdapter {
        /**
         *
         */
        private static final long serialVersionUID = 0L;

        /** Ignite instance. */
        @IgniteInstanceResource
        private IgniteEx ignite;

        /** Injected logger. */
        @LoggerResource
        private IgniteLogger log;

        /** Partition key. */
        private final RecheckRequest recheckRequest;

        /**
         * @param recheckReq
         */
        public CollectRecheckJob(RecheckRequest recheckReq) {
            this.recheckRequest = recheckReq;
        }

        /** {@inheritDoc} */
        @Override public List<PartitionKeyVersion> execute() throws IgniteException {
            GridCacheContext<Object, Object> cctx = ignite.context().cache().cache(recheckRequest.cacheName()).context();

            CacheGroupContext grpCtx = cctx.group();

            GridDhtLocalPartition part = grpCtx.topology().localPartition(recheckRequest.partitionId());

            if (part.state() != GridDhtPartitionState.OWNING)
                return Collections.emptyList();

            List<PartitionKeyVersion> recheckedKeys = new ArrayList<>();

            for (KeyCacheObject recheckKey : recheckRequest.recheckKeys()) {
                try {
                    KeyCacheObject key = unmarshalKey(recheckKey, cctx);

                    CacheDataRow row = grpCtx.offheap().dataStore(part).find(cctx, key);

                    if (row != null)
                        recheckedKeys.add(new PartitionKeyVersion(ignite.localNode().id(), row.key(), row.version()));
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Recheck key [" + recheckKey + "] was skipped.", e);
                }
            }

            return recheckedKeys;
        }
    }
}
