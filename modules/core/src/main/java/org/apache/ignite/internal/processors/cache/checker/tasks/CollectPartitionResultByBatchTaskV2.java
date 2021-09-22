/*
 * Copyright 2021 GridGain Systems, Inc. and Contributors.
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
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
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
import org.apache.ignite.internal.IgniteFeatures;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.checker.ReconciliationContext;
import org.apache.ignite.internal.processors.cache.checker.objects.ExecutionResult;
import org.apache.ignite.internal.processors.cache.checker.objects.NodePartitionSize;
import org.apache.ignite.internal.processors.cache.checker.objects.PartitionBatchRequestV2;
import org.apache.ignite.internal.processors.cache.checker.objects.PartitionExecutionJobResultByBatch;
import org.apache.ignite.internal.processors.cache.checker.objects.PartitionExecutionTaskResultByBatch;
import org.apache.ignite.internal.processors.cache.checker.objects.VersionedKey;
import org.apache.ignite.internal.processors.cache.checker.util.KeyComparator;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static java.lang.Thread.sleep;
import static org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager.RECONCILIATION;
import static org.apache.ignite.internal.processors.cache.checker.ReconciliationContext.SizeReconciliationState.IN_PROGRESS;
import static org.apache.ignite.internal.processors.cache.checker.util.ConsistencyCheckUtils.unmarshalKey;

/**
 * Collects and returns a set of keys that have conflicts with {@link GridCacheVersion}.
 */
@GridInternal
public class CollectPartitionResultByBatchTaskV2 extends ComputeTaskAdapter<PartitionBatchRequestV2, ExecutionResult<PartitionExecutionTaskResultByBatch>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private static final KeyComparator KEY_COMPARATOR = new KeyComparator();

    /** Injected logger. */
    @LoggerResource
    private IgniteLogger log;

    /** Ignite instance. */
    @IgniteInstanceResource
    private IgniteEx ignite;

    /** Partition batch. */
    private volatile PartitionBatchRequestV2 partBatch;

    /**
     * {@code true} - if partition reconciliation with cache size consistency and
     * partition counter consistency support.
     */
    private boolean sizeReconciliationSupport;

    /** {@inheritDoc} */
    @NotNull @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
        PartitionBatchRequestV2 partBatch) throws IgniteException {
        Map<ComputeJob, ClusterNode> jobs = new HashMap<>();

        this.partBatch = partBatch;

        sizeReconciliationSupport = IgniteFeatures.allNodesSupports(
            ignite.context(),
            ignite.context().discovery().allNodes(),
            IgniteFeatures.PARTITION_RECONCILIATION_V2);

        for (ClusterNode node : subgrid)
            jobs.put(new CollectPartitionResultByBatchJobV2(partBatch), node);

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
    @Override public @Nullable ExecutionResult<PartitionExecutionTaskResultByBatch> reduce(
        List<ComputeJobResult> results) throws IgniteException {
        assert partBatch != null;

        GridCacheContext<Object, Object> ctx = ignite.context().cache().cache(partBatch.cacheName()).context();

        Map<KeyCacheObject, Map<UUID, GridCacheVersion>> totalRes = new HashMap<>();

        KeyCacheObject lastKey = null;

        if (sizeReconciliationSupport) {

            Map<UUID, NodePartitionSize> partSizesMap = new HashMap<>();

            for (int i = 0; i < results.size(); i++) {
                UUID nodeId = results.get(i).getNode().id();

                IgniteException exc = results.get(i).getException();

                if (exc != null)
                    return new ExecutionResult<>(exc.getMessage());

                ExecutionResult<PartitionExecutionJobResultByBatch> nodeRes = results.get(i).getData();

                if (nodeRes.errorMessage() != null)
                    return new ExecutionResult<>(nodeRes.errorMessage());

                for (VersionedKey partKeyVer : nodeRes.result().versionedKeys()) {
                    try {
                        KeyCacheObject key = unmarshalKey(partKeyVer.key(), ctx);

                        if (lastKey == null || KEY_COMPARATOR.compare(lastKey, key) < 0)
                            lastKey = key;

                        Map<UUID, GridCacheVersion> map = totalRes.computeIfAbsent(key, k -> new HashMap<>());
                        map.put(partKeyVer.nodeId(), partKeyVer.ver());

                        if (i == (results.size() - 1) && map.size() == results.size() && !hasConflict(map.values()))
                            totalRes.remove(key);
                    }
                    catch (IgniteCheckedException e) {
                        U.error(log, e.getMessage(), e);

                        return new ExecutionResult<>(e.getMessage());
                    }
                }

                partSizesMap.put(nodeId, nodeRes.result().nodePartitionSize());
            }

            return new ExecutionResult<>(new PartitionExecutionTaskResultByBatch(lastKey, totalRes, partSizesMap));
        }
        else {
            for (int i = 0; i < results.size(); i++) {
                IgniteException exc = results.get(i).getException();

                if (exc != null)
                    return new ExecutionResult<>(exc.getMessage());

                ExecutionResult<List<VersionedKey>> nodeRes = results.get(i).getData();

                if (nodeRes.errorMessage() != null)
                    return new ExecutionResult<>(nodeRes.errorMessage());

                for (VersionedKey partKeyVer : nodeRes.result()) {
                    try {
                        KeyCacheObject key = unmarshalKey(partKeyVer.key(), ctx);

                        if (lastKey == null || KEY_COMPARATOR.compare(lastKey, key) < 0)
                            lastKey = key;

                        Map<UUID, GridCacheVersion> map = totalRes.computeIfAbsent(key, k -> new HashMap<>());
                        map.put(partKeyVer.nodeId(), partKeyVer.ver());

                        if (i == (results.size() - 1) && map.size() == results.size() && !hasConflict(map.values()))
                            totalRes.remove(key);
                    }
                    catch (IgniteCheckedException e) {
                        U.error(log, e.getMessage(), e);

                        return new ExecutionResult<>(e.getMessage());
                    }
                }
            }

            return new ExecutionResult<>(new PartitionExecutionTaskResultByBatch(lastKey, totalRes, null));
        }
    }

    /**
     *
     */
    private boolean hasConflict(Collection<GridCacheVersion> keyVersions) {
        assert !keyVersions.isEmpty();

        Iterator<GridCacheVersion> iter = keyVersions.iterator();
        GridCacheVersion ver = iter.next();

        while (iter.hasNext()) {
            if (!ver.equals(iter.next()))
                return true;
        }

        return false;
    }

    /**
     *
     */
    private static class CollectPartitionResultByBatchJobV2 extends ReconciliationResourceLimitedJob {
        /**
         *
         */
        private static final long serialVersionUID = 0L;

        /** Partition key. */
        private PartitionBatchRequestV2 partBatch;

        /**
         * @param partBatch Partition key.
         */
        private CollectPartitionResultByBatchJobV2(PartitionBatchRequestV2 partBatch) {
            this.partBatch = partBatch;
        }

        /** {@inheritDoc} */
        @Override protected long sessionId() {
            return partBatch.sessionId();
        }

        /** {@inheritDoc} */
        @Override protected ExecutionResult<PartitionExecutionJobResultByBatch> execute0() {
            boolean consistencyReconciliation = partBatch.dataReconciliation();

            GridCacheContext<Object, Object> cctx = ignite.context().cache().cache(partBatch.cacheName()).context();

            CacheGroupContext grpCtx = cctx.group();

            NodePartitionSize nodePartitionSize = partBatch.partSizesMap().get(ignite.localNode().id());

            boolean sizeReconciliation = partBatch.cacheSizeReconciliation() &&
                (nodePartitionSize == null ||
                    nodePartitionSize.state() == NodePartitionSize.SizeReconciliationState.IN_PROGRESS);

            int cacheId = grpCtx.sharedGroup() ? cctx.cacheId() : CU.UNDEFINED_CACHE_ID;

            final KeyCacheObject lastKeyForConsistency;

            try {
                lastKeyForConsistency = unmarshalKey(partBatch.lowerKey(), cctx);
            }
            catch (IgniteCheckedException e) {
                String errMsg = "Batch [" + partBatch + "] can't processed. Broken key.";

                log.error(errMsg, e);

                return new ExecutionResult<>(errMsg + " " + e.getMessage());
            }

            GridDhtLocalPartition part = grpCtx.topology().localPartition(partBatch.partitionId());

            assert part != null;

            IgniteCacheOffheapManager.CacheDataStore cacheDataStore = grpCtx.offheap().dataStore(part);

            ReconciliationContext partReconciliationCtx = null;

            NodePartitionSize nodeSize = nodePartitionSize == null ? new NodePartitionSize(partBatch.cacheName()) : nodePartitionSize;

            KeyCacheObject lastKeyForSizes = null;

            part.reserve();

            try {
                if (sizeReconciliation) {
                    try {
                        if (nodePartitionSize == null)
                            cacheDataStore.reconciliationCtxInit();

                        partReconciliationCtx = cacheDataStore.reconciliationCtx();
                    }
                    catch (IgniteCheckedException e) {
                        throw new IgniteException(e);
                    }

                    if (partReconciliationCtx.sizeReconciliationState(cacheId) == null) {
                        nodeSize.state(NodePartitionSize.SizeReconciliationState.IN_PROGRESS);

                        partReconciliationCtx = cacheDataStore.startReconciliation(cacheId);
                    }

                    lastKeyForSizes = partReconciliationCtx.lastKey(cacheId);
                }

                KeyCacheObject keyToStart = firstKeyForReconciliationCursor(consistencyReconciliation, sizeReconciliation, lastKeyForConsistency, lastKeyForSizes);

                if (consistencyReconciliation || sizeReconciliation) {
                    try (GridCursor<? extends CacheDataRow> cursor = keyToStart == null ?
                        grpCtx.offheap().dataStore(part).cursor(cctx.cacheId(), RECONCILIATION) :
                        grpCtx.offheap().dataStore(part).cursor(cctx.cacheId(), keyToStart, null, RECONCILIATION)) {

                        List<VersionedKey> partEntryHashRecords = new ArrayList<>();

                        boolean hasNext = cursor.next();

                        for (int i = 0; (i < partBatch.batchSize() && hasNext); i++) {
                            CacheDataRow row = cursor.get();

                            if (consistencyReconciliation && lastKeyForConsistency != null && KEY_COMPARATOR.compare(lastKeyForConsistency, row.key()) >= 0)
                                i--;
                            else if (consistencyReconciliation && (lastKeyForConsistency == null || KEY_COMPARATOR.compare(lastKeyForConsistency, row.key()) < 0)) {
                                partEntryHashRecords.add(new VersionedKey(
                                    ignite.localNode().id(),
                                    row.key(),
                                    row.version()
                                ));
                            }

                            hasNext = cursor.next();
                        }

                        if (sizeReconciliation && !hasNext) {
                            nodeSize.state(NodePartitionSize.SizeReconciliationState.FINISHED);

                            partReconciliationCtx.sizeReconciliationCursorState(cacheId, false);
                        }

                        return new ExecutionResult<>(new PartitionExecutionJobResultByBatch(partEntryHashRecords, nodeSize));
                    }
                    catch (Exception e) {
                        String errMsg = "Batch [" + partBatch + "] can't processed. Broken cursor.";

                        log.error(errMsg, e);

                        return new ExecutionResult<>(errMsg + " " + e.getMessage());
                    }
                }
                else
                    return new ExecutionResult<>(new PartitionExecutionJobResultByBatch(new ArrayList<>(), nodeSize));
            }
            finally {
                part.release();
            }
        }

        private KeyCacheObject firstKeyForReconciliationCursor(boolean reconConsist, boolean reconSize, KeyCacheObject lowerKey, KeyCacheObject lastKeyForSizes) {
            if (reconConsist && lowerKey != null && reconSize && lastKeyForSizes != null)
                return KEY_COMPARATOR.compare(lowerKey, lastKeyForSizes) < 0 ? lowerKey : lastKeyForSizes;
            else if (reconConsist && lowerKey != null)
                return lowerKey;
            else if (reconSize && lastKeyForSizes != null)
                return lastKeyForSizes;
            else
                return null;
        }
    }
}
