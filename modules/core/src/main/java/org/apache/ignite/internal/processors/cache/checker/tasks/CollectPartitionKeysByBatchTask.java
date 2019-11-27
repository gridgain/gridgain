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
import java.util.Iterator;
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
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.checker.util.KeyComparator;
import org.apache.ignite.internal.processors.cache.checker.objects.PartitionBatchRequest;
import org.apache.ignite.internal.processors.cache.checker.objects.PartitionKeyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
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

import static org.apache.ignite.internal.processors.cache.checker.util.ConsistencyCheckUtils.unmarshalKey;

/**
 * Collects and returns a set of keys that have conflicts with {@link GridCacheVersion}.
 */
@GridInternal
public class CollectPartitionKeysByBatchTask extends ComputeTaskAdapter<PartitionBatchRequest, T2<KeyCacheObject, Map<KeyCacheObject, Map<UUID, GridCacheVersion>>>> {
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
    private volatile PartitionBatchRequest partBatch;

    /** {@inheritDoc} */
    @NotNull @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
        PartitionBatchRequest partBatch) throws IgniteException {
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
    @Nullable @Override public T2<KeyCacheObject, Map<KeyCacheObject, Map<UUID, GridCacheVersion>>> reduce(
        List<ComputeJobResult> results) throws IgniteException {
        assert partBatch != null;

        CacheObjectContext ctxo = ignite.context().cache().cache(partBatch.cacheName()).context().cacheObjectContext();

        Map<KeyCacheObject, Map<UUID, GridCacheVersion>> totalRes = new HashMap<>();

        // N = PartCount * batchSize; Time: O( N * logN ), space: 2 * N;
        // TODO use just HashMap;
        List<PartitionKeyVersion> temp = new ArrayList<>();

        int partNum = 0;

        for (ComputeJobResult res : results) {
            T2<GridDhtPartitionState, List<PartitionKeyVersion>> nodeRes = res.getData();
            temp.addAll(nodeRes.get2());

            if (nodeRes.get1() == GridDhtPartitionState.OWNING)
                partNum++;
        }

        Iterator<PartitionKeyVersion> iter = temp.iterator();

        while (iter.hasNext()) {
            PartitionKeyVersion partKeyVer = iter.next();
            try {
                partKeyVer.getKey().finishUnmarshal(ctxo, null);
            }
            catch (IgniteCheckedException e) {
                iter.remove();

                U.error(log, "Key cache object can't unashamed.", e);
            }
        }

        if (temp.isEmpty())
            return new T2<>(null, totalRes);

        temp.sort((o1, o2) -> KEY_COMPARATOR.compare(o1.getKey(), o2.getKey()));

        boolean needRmv = true;

        //TODO last iteration
        for (int i = 0; i < temp.size() - 1; i++) {
            PartitionKeyVersion cur = temp.get(i);
            PartitionKeyVersion next = temp.get(i + 1);

            totalRes.computeIfAbsent(cur.getKey(), k -> new HashMap<>())
                .put(cur.getNodeId(), cur.getVersion());

            if (KEY_COMPARATOR.compare(cur.getKey(), next.getKey()) == 0) {
                if (!cur.getVersion().equals(next.getVersion()))
                    needRmv = false;
            }
            else { // finish pair
                if (needRmv && totalRes.get(cur.getKey()).size() == partNum)
                    totalRes.remove(cur.getKey());
                else
                    needRmv = true;
            }
        }

        final PartitionKeyVersion lastElement = temp.get(temp.size() - 1);
        final KeyCacheObject lastKey = lastElement.getKey();

        totalRes.computeIfAbsent(lastKey, k -> new HashMap<>()).put(lastElement.getNodeId(), lastElement.getVersion());

        if (needRmv && totalRes.get(lastKey).size() == partNum)
            totalRes.remove(lastKey);

        return new T2<>(lastKey, totalRes);
    }

    /**
     *
     */
    public static class CollectPartitionKeysByBatchJob extends ComputeJobAdapter {
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
        private PartitionBatchRequest partBatch;

        /**
         * @param partKey Partition key.
         */
        private CollectPartitionKeysByBatchJob(PartitionBatchRequest partBatch) {
            this.partBatch = partBatch;
        }

        /** {@inheritDoc} */
        @Override public T2<GridDhtPartitionState, List<PartitionKeyVersion>> execute() throws IgniteException {
            GridCacheContext<Object, Object> cctx = ignite.context().cache().cache(partBatch.cacheName()).context();

            CacheGroupContext grpCtx = cctx.group();

            final int batchSize = partBatch.batchSize();
            final KeyCacheObject lowerKey;

            try {
                lowerKey = unmarshalKey(partBatch.lowerKey(), cctx);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Batch [" + partBatch + "] can't processed. Broken key.", e);
            }

            GridDhtLocalPartition part = grpCtx.topology().localPartition(partBatch.partitionId());

            if (part.state() != GridDhtPartitionState.OWNING)
                return new T2<>(part.state(), Collections.emptyList());

            try (GridCursor<? extends CacheDataRow> cursor = lowerKey == null ?
                grpCtx.offheap().dataStore(part).cursor(cctx.groupId()) :
                grpCtx.offheap().dataStore(part).cursor(cctx.groupId(), lowerKey, null)) {

                List<PartitionKeyVersion> partEntryHashRecords = new ArrayList<>();

                for (int i = 0; i < batchSize && cursor.next(); i++) {
                    CacheDataRow row = cursor.get();

                    if (lowerKey == null || KEY_COMPARATOR.compare(lowerKey, row.key()) != 0)
                        partEntryHashRecords.add(new PartitionKeyVersion(
                            ignite.localNode().id(),
                            row.key(),
                            row.version()
                        ));
                    else
                        i--; //TODO fix it.
                }

                return new T2<>(part.state(), partEntryHashRecords);
            }
            catch (Exception e) {
                throw new IgniteException("Batch [" + partBatch + "] can't processed. Broken cursor.", e);
            }
        }
    }
}
