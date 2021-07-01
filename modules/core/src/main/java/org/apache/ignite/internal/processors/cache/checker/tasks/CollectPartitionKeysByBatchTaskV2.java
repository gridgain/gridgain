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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
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
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManagerImpl;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.checker.objects.ExecutionResult;
import org.apache.ignite.internal.processors.cache.checker.objects.NodePartitionSize;
import org.apache.ignite.internal.processors.cache.checker.objects.PartitionBatchRequestV2;
import org.apache.ignite.internal.processors.cache.checker.objects.VersionedKey;
import org.apache.ignite.internal.processors.cache.checker.util.KeyComparator;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.verify.RepairAlgorithm;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager.RECONCILIATION;
import static org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManagerImpl.CacheDataStoreImpl.ReconciliationContext.SizeReconciliationState.FINISHED;
import static org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManagerImpl.CacheDataStoreImpl.ReconciliationContext.SizeReconciliationState.IN_PROGRESS;
import static org.apache.ignite.internal.processors.cache.checker.util.ConsistencyCheckUtils.unmarshalKey;

/**
 * Collects and returns a set of keys that have conflicts with {@link GridCacheVersion}.
 */
@GridInternal
public class CollectPartitionKeysByBatchTaskV2 extends ComputeTaskAdapter<PartitionBatchRequestV2, ExecutionResult<T3<KeyCacheObject, Map<KeyCacheObject, Map<UUID, GridCacheVersion>>, Map<UUID, NodePartitionSize>>>> {
    /**
     *
     */
    private static final long serialVersionUID = 0L;

    /**
     *
     */
    private static final KeyComparator KEY_COMPARATOR = new KeyComparator();

    /** Injected logger. */
    @LoggerResource
    private IgniteLogger log;

    /** Ignite instance. */
    @IgniteInstanceResource
    private IgniteEx ignite;

    /** Partition batch. */
    private volatile PartitionBatchRequestV2 partBatch;

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
            jobs.put(new CollectPartitionKeysByBatchJobV2(partBatch), node);

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
    @Override public @Nullable ExecutionResult<T3<KeyCacheObject, Map<KeyCacheObject, Map<UUID, GridCacheVersion>>, Map<UUID, NodePartitionSize>>> reduce(
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

                ExecutionResult<T2<List<VersionedKey>, NodePartitionSize>> nodeRes = results.get(i).getData();

                if (nodeRes.errorMessage() != null)
                    return new ExecutionResult<>(nodeRes.errorMessage());

                for (VersionedKey partKeyVer : nodeRes.result().get1()) {
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

                partSizesMap.put(nodeId, nodeRes.result().get2());
            }

            return new ExecutionResult<>(new T3<>(lastKey, totalRes, partSizesMap));
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

            return new ExecutionResult<>(new T3<>(lastKey, totalRes, null));
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
    public static class CollectPartitionKeysByBatchJobV2 extends ReconciliationResourceLimitedJob {
        /**
         *
         */
        private static final long serialVersionUID = 0L;

        /** Partition key. */
        private PartitionBatchRequestV2 partBatch;

        /**
         * @param partBatch Partition key.
         */
        private CollectPartitionKeysByBatchJobV2(PartitionBatchRequestV2 partBatch) {
            this.partBatch = partBatch;
        }

        /** {@inheritDoc} */
        @Override protected long sessionId() {
            return partBatch.sessionId();
        }

        /** {@inheritDoc} */
        @Override protected ExecutionResult<T2<List<VersionedKey>, NodePartitionSize>> execute0() {
            boolean reconConsist = partBatch.reconConsist;

            NodePartitionSize nodePartitionSize = partBatch.partSizesMap().get(ignite.localNode().id());
            boolean reconSize = partBatch.reconSize &&
                (nodePartitionSize == null || nodePartitionSize.inProgress());

            GridCacheContext<Object, Object> cctx = ignite.context().cache().cache(partBatch.cacheName()).context();

            CacheGroupContext grpCtx = cctx.group();

            int cacheId = grpCtx.sharedGroup() ? cctx.cacheId() : CU.UNDEFINED_CACHE_ID;

            final int batchSize = partBatch.batchSize();
            final KeyCacheObject lowerKey;
            KeyCacheObject newLowerKey = null;

            try {
                lowerKey = unmarshalKey(partBatch.lowerKey(), cctx);
            }
            catch (IgniteCheckedException e) {
                String errMsg = "Batch [" + partBatch + "] can't processed. Broken key.";

                log.error(errMsg, e);

                return new ExecutionResult<>(errMsg + " " + e.getMessage());
            }

            GridDhtLocalPartition part = grpCtx.topology().localPartition(partBatch.partitionId());

            IgniteCacheOffheapManager.CacheDataStore cacheDataStore = grpCtx.offheap().dataStore(part);

            assert part != null;

            part.reserve();

            IgniteCacheOffheapManagerImpl.CacheDataStoreImpl.ReconciliationContext partReconciliationCtx = null;

            NodePartitionSize nodeSize = nodePartitionSize == null ? new NodePartitionSize(partBatch.cacheName()) : nodePartitionSize;

            KeyCacheObject lastKeyForSizes = null;

            AtomicLong partSize = null;

            KeyCacheObject oldBorderKey = null;

            Map<KeyCacheObject, Boolean> tempMap = null;

            if (reconSize) {
                try {
                    partReconciliationCtx = cacheDataStore.reconciliationCtx();
                }
                catch (IgniteCheckedException e) {
                    throw new RuntimeException(e);
                }

                if (partReconciliationCtx != null &&
                    partReconciliationCtx.sizeReconciliationState(cacheId) == null &&
                    partReconciliationCtx.lastKey(cacheId) == null) {

                    cacheDataStore.block();

                    try {
                        nodeSize.inProgress(true);
                        partReconciliationCtx = cacheDataStore.startReconciliation(cacheId);
                    }
                    finally {
                        cacheDataStore.unblock();
                    }

                    partReconciliationCtx.sizes.putIfAbsent(cacheId, new AtomicLong());

                    partReconciliationCtx.tempMap.putIfAbsent(cacheId, new ConcurrentHashMap<>());
                }
                else {
                    if (nodePartitionSize != null)
                        nodeSize = nodePartitionSize;
                }

                lastKeyForSizes = partReconciliationCtx.lastKey(cacheId);

                partSize = partReconciliationCtx.sizes.get(cacheId);

                oldBorderKey = partReconciliationCtx.lastKey(cacheId);

                tempMap = partReconciliationCtx.tempMap.get(cacheId);
            }

            KeyCacheObject keyToStart = null;

            if (reconConsist && lowerKey != null && reconSize && lastKeyForSizes != null)
                keyToStart = KEY_COMPARATOR.compare(lowerKey, lastKeyForSizes) < 0 ? lowerKey : lastKeyForSizes;
            else if (reconConsist && lowerKey != null)
                keyToStart = lowerKey;
            else if (reconSize && lastKeyForSizes != null)
                keyToStart = lastKeyForSizes;

            if (reconConsist || reconSize) {
                try (GridCursor<? extends CacheDataRow> cursor = keyToStart == null ?
                        grpCtx.offheap().dataStore(part).cursor(cctx.cacheId(), RECONCILIATION) :
                        grpCtx.offheap().dataStore(part).cursor(cctx.cacheId(), keyToStart, null, RECONCILIATION)) {

                    List<VersionedKey> partEntryHashRecords = new ArrayList<>();

                    boolean hasNext = cursor.next();

                    for (int i = 0; (i < batchSize && hasNext); i++) {
                        CacheDataRow row = cursor.get();

                        if (reconConsist && lowerKey != null && KEY_COMPARATOR.compare(lowerKey, row.key()) >= 0)
                            i--;
                        else if (reconConsist && (lowerKey == null || KEY_COMPARATOR.compare(lowerKey, row.key()) < 0)) {
                            newLowerKey = row.key();

                            partEntryHashRecords.add(new VersionedKey(
                                ignite.localNode().id(),
                                row.key(),
                                row.version()
                            ));
                        }

                        hasNext = cursor.next();
                    }

                    if (reconSize && !hasNext &&
                        ((partReconciliationCtx.lastKey(cacheId) == null || partReconciliationCtx.lastKey(cacheId).equals(oldBorderKey)) &&
                            (lowerKey == null || lowerKey.equals(newLowerKey))) &&
                        partReconciliationCtx.sizeReconciliationState(cacheId) == IN_PROGRESS) {

                        nodeSize.lastKey((partReconciliationCtx.lastKey(cacheId)));

                        cacheDataStore.block();

                        try {
                            partReconciliationCtx.sizeReconciliationState(cacheId, FINISHED);

                            Iterator<Map.Entry<KeyCacheObject, Boolean>> tempMapIter = tempMap.entrySet().iterator();

                            while (tempMapIter.hasNext()) {
                                tempMapIter.next();

                                partSize.incrementAndGet();
                            }

                            if (partBatch.repairAlg != RepairAlgorithm.PRINT_ONLY)
                                cacheDataStore.flushReconciliationResult(cacheId, nodeSize, true);
                            else
                                cacheDataStore.flushReconciliationResult(cacheId, nodeSize, false);

                            nodeSize.lastKey(null);

                            nodeSize.inProgress(false);
                        }
                        finally {
                            cacheDataStore.unblock();
                        }
                    }

                    return new ExecutionResult<>(new T2<>(partEntryHashRecords, nodeSize));
                }
                catch (Exception e) {
                    String errMsg = "Batch [" + partBatch + "] can't processed. Broken cursor.";

                    log.error(errMsg, e);

                    return new ExecutionResult<>(errMsg + " " + e.getMessage());
                }
                finally {
                    part.release();
                }
            }
            else
                return new ExecutionResult<>(new T2<>(new ArrayList<>(), nodeSize));
        }
    }
}
