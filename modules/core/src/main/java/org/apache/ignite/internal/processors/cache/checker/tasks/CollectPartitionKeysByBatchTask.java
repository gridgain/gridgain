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
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.checker.objects.ExecutionResult;
import org.apache.ignite.internal.processors.cache.checker.objects.PartitionBatchRequest;
import org.apache.ignite.internal.processors.cache.checker.objects.VersionedKey;
import org.apache.ignite.internal.processors.cache.checker.util.KeyComparator;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager.DATA;
import static org.apache.ignite.internal.processors.cache.checker.util.ConsistencyCheckUtils.unmarshalKey;

/**
 * Collects and returns a set of keys that have conflicts with {@link GridCacheVersion}.
 */
@GridInternal
public class CollectPartitionKeysByBatchTask extends ComputeTaskAdapter<PartitionBatchRequest, ExecutionResult<T2<KeyCacheObject, Map<KeyCacheObject, Map<UUID, GridCacheVersion>>>>> {
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
    private volatile PartitionBatchRequest partBatch;

    /** {@inheritDoc} */
    @NotNull @Override public Map<? extends ComputeJob, ClusterNode> map(
        List<ClusterNode> subgrid,
        PartitionBatchRequest partBatch
    ) throws IgniteException {
        Map<ComputeJob, ClusterNode> jobs = new HashMap<>();

        this.partBatch = partBatch;

        for (ClusterNode node : subgrid)
            jobs.put(new CollectPartitionKeysByBatchJob(partBatch), node);

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
    @Override public @Nullable ExecutionResult<T2<KeyCacheObject, Map<KeyCacheObject, Map<UUID, GridCacheVersion>>>> reduce(
        List<ComputeJobResult> results
    ) throws IgniteException {
        assert partBatch != null;

        IgniteInternalCache<Object, Object> cache = ignite.context().cache().cache(partBatch.cacheName());

        if (cache == null)
            return new ExecutionResult<>("Cache not found (was stopped) [name=" + partBatch.cacheName() + ']');

        GridCacheContext<Object, Object> ctx = cache.context();

        Map<KeyCacheObject, Map<UUID, GridCacheVersion>> totalRes = new HashMap<>();

        KeyCacheObject lastKey = null;

        for (int i = 0; i < results.size(); i++) {
            IgniteException exc = results.get(i).getException();

            if (exc != null)
                return new ExecutionResult<>(exc.getMessage());

            ExecutionResult<List<VersionedKey>> nodeRes = results.get(i).getData();

            if (nodeRes.errorMessage() != null)
                return new ExecutionResult<>(nodeRes.errorMessage());

            // This variable is used to store the last observed key for results.get(i).getNode().
            KeyCacheObject lastNodeKey = null;
            for (VersionedKey partKeyVer : nodeRes.result()) {
                try {
                    KeyCacheObject key = unmarshalKey(partKeyVer.key(), ctx);

                    if (lastNodeKey == null || KEY_COMPARATOR.compare(lastNodeKey, key) < 0)
                        lastNodeKey = key;

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

            // Choose the minimum key of all last observed keys from all nodes.
            // This allows to fix the issue with non-processed keys under certian conditions.
            // For example,
            // the primary partition contains all keys [1, ..., 200] and the backup only has half of the keys [100, ..., 200],
            // and the batch size is 50. In this case, the primary node will return [1, ..., 50] and backup will return [100, ..., 150].
            // The range of keys to re-check is [1, ..., 50, 100, ..., 150] and the next batch should start from 150
            // because 150 is the last observed key. It means that the range [51, ..., 99] will be skipped.
            // So, the lastKey should be the minimum key of all last observed keys. This may lead to re-checking the same keys several times,
            // but it is better than skipping keys.
            if (lastKey == null || (lastNodeKey != null && KEY_COMPARATOR.compare(lastNodeKey, lastKey) < 0))
                lastKey = lastNodeKey;
        }

        return new ExecutionResult<>(new T2<>(lastKey, totalRes));
    }

    /**
     * Returns {@code true} if there are conflicting versions.
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
    public static class CollectPartitionKeysByBatchJob extends ReconciliationResourceLimitedJob {
        /** */
        private static final long serialVersionUID = 0L;

        /** Partition key. */
        private PartitionBatchRequest partBatch;

        /**
         * @param partBatch Partition key.
         */
        private CollectPartitionKeysByBatchJob(PartitionBatchRequest partBatch) {
            this.partBatch = partBatch;
        }

        /** {@inheritDoc} */
        @Override protected long sessionId() {
            return partBatch.sessionId();
        }

        /** {@inheritDoc} */
        @Override protected ExecutionResult<List<VersionedKey>> execute0() {
            GridCacheContext<Object, Object> cctx = ignite.context().cache().cache(partBatch.cacheName()).context();

            CacheGroupContext grpCtx = cctx.group();

            final int batchSize = partBatch.batchSize();
            final KeyCacheObject lowerKey;

            try {
                lowerKey = unmarshalKey(partBatch.lowerKey(), cctx);
            }
            catch (IgniteCheckedException e) {
                String errMsg = "Batch [" + partBatch + "] can't processed. Broken key.";

                log.error(errMsg, e);

                return new ExecutionResult<>(errMsg + " " + e.getMessage());
            }

            GridDhtLocalPartition part = grpCtx.topology().localPartition(partBatch.partitionId());

            assert part != null;

            part.reserve();

            boolean primary = cctx.affinity().primaryPartitions(ignite.localNode().id(), partBatch.startTopVer()).contains(part.id());

            try (GridCursor<? extends CacheDataRow> cursor = lowerKey == null ?
                grpCtx.offheap().dataStore(part).cursor(cctx.cacheId(), DATA) :
                grpCtx.offheap().dataStore(part).cursor(cctx.cacheId(), lowerKey, null)) {

                List<VersionedKey> partEntryHashRecords = new ArrayList<>();

                for (int i = 0; i < batchSize && cursor.next(); i++) {
                    CacheDataRow row = cursor.get();

                    if (lowerKey == null || KEY_COMPARATOR.compare(lowerKey, row.key()) != 0) {
                        partEntryHashRecords.add(new VersionedKey(
                            ignite.localNode().id(),
                            row.key(),
                            row.version()
                        ));
                    }
                    else
                        i--;
                }

                ignite.context().diagnostic().reconciliationExecutionContext().updatePartitionStatistics(
                    partBatch.sessionId(),
                    partBatch.cacheName(),
                    partBatch.partitionId(),
                    primary,
                    partEntryHashRecords.size());

                return new ExecutionResult<>(partEntryHashRecords);
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
    }
}
