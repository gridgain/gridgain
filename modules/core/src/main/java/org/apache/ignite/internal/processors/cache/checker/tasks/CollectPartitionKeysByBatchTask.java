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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCache;
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
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManagerImpl;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObjectImpl;
import org.apache.ignite.internal.processors.cache.checker.objects.ExecutionResult;
import org.apache.ignite.internal.processors.cache.checker.objects.NodePartitionSize;
import org.apache.ignite.internal.processors.cache.checker.objects.PartitionBatchRequest;
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

import static java.lang.Thread.sleep;
import static org.apache.ignite.internal.processors.cache.checker.util.ConsistencyCheckUtils.unmarshalKey;

/**
 * Collects and returns a set of keys that have conflicts with {@link GridCacheVersion}.
 */
@GridInternal
public class CollectPartitionKeysByBatchTask extends ComputeTaskAdapter<PartitionBatchRequest, ExecutionResult<T3<KeyCacheObject, Map<KeyCacheObject, Map<UUID, GridCacheVersion>>, Map<UUID, NodePartitionSize>>>> {
    /**
     *
     */
    private static final long serialVersionUID = 0L;

//    public static int i;
//    public static int iCleanup;
//    public static int iUpdate;
//
//    public static final CountDownLatch latch = new CountDownLatch(1);
//
//    public static final Map<Integer, String> msg = new ConcurrentHashMap<>();
//    public static final Mqap<Integer, String> msg1 = new ConcurrentHashMap<>();

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
    @Override public @Nullable ExecutionResult<T3<KeyCacheObject, Map<KeyCacheObject, Map<UUID, GridCacheVersion>>, Map<UUID, NodePartitionSize>>> reduce(
        List<ComputeJobResult> results) throws IgniteException {
        assert partBatch != null;

        GridCacheContext<Object, Object> ctx = ignite.context().cache().cache(partBatch.cacheName()).context();

        Map<KeyCacheObject, Map<UUID, GridCacheVersion>> totalRes = new HashMap<>();

        KeyCacheObject lastKey = null;

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

//        System.out.println("qgrtsngd " + results);

//        if (lastKey == null)
//            System.out.println("qgrtsngd null");
//        else
//            System.out.println("qgrtsngd " + ((KeyCacheObjectImpl)lastKey).value());
//        System.out.println("qgrtsngd " + lastKey);

        return new ExecutionResult<>(new T3<>(lastKey, totalRes, partSizesMap));
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

    static volatile int i;

    /**
     *
     */
    public static class CollectPartitionKeysByBatchJob extends ReconciliationResourceLimitedJob {
        /**
         *
         */
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
        @Override protected ExecutionResult<T2<List<VersionedKey>, NodePartitionSize>> execute0() {
            boolean reconConsist = partBatch.reconConsist;

            NodePartitionSize nodePartitionSize = partBatch.partSizesMap().get(ignite.localNode().id());
            boolean reconSize = partBatch.reconSize &&
                (nodePartitionSize == null || nodePartitionSize.inProgress);

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

            NodePartitionSize nodeSize = nodePartitionSize == null ? new NodePartitionSize() : nodePartitionSize;

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
                    !partReconciliationCtx.isReconciliationInProgress(cacheId) &&
                    partReconciliationCtx.lastKey(cacheId) == null &&
                    partReconciliationCtx.isReconciliationIsFinished.get(cacheId) == null) {

                    cacheDataStore.block();

//                    System.out.println("qdsaftpg start first busy lock");

                    try {
                        nodeSize.inProgress = true;
                        partReconciliationCtx = cacheDataStore.startReconciliation(cacheId);

//                        System.out.println("qlopfots set isReconciliationInProgress to true");
                    }
                    finally {
//                        System.out.println("qdsaftpg end first busy lock");
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

//            System.out.println("asdijfliuue keyToStart " + (keyToStart == null ? "null" : ((KeyCacheObjectImpl)keyToStart).value()));
//            System.out.println("fhdjmrtjut lowerKey " + (lowerKey == null ? "null" : ((KeyCacheObjectImpl)lowerKey).value()));
//            System.out.println("rsathtyjtq45 lastKeyForSizes " + (lastKeyForSizes == null ? "null" : ((KeyCacheObjectImpl)lastKeyForSizes).value()));

            if (reconConsist || reconSize) {
                try (GridCursor<? extends CacheDataRow> cursor = keyToStart == null ?
                    cacheDataStore.reconCursor(cacheId, null, null, null, null, IgniteCacheOffheapManager.DATA)  :
                    cacheDataStore.reconCursor(cacheId, keyToStart, null, null, null, IgniteCacheOffheapManager.DATA)) {
//                    System.out.println("qdsadfvers start cursor " + cursor.getClass().getName());

                    List<VersionedKey> partEntryHashRecords = new ArrayList<>();

                    boolean hasNext = true;

                    hasNext = cursor.next();

                    for (int i = 0; (i < batchSize && hasNext); i++) {
//                    try {
//                        sleep(1);
//                    }
//                    catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }

                        CacheDataRow row = cursor.get();
//                        System.out.println("qkljiddfvbj " + ((KeyCacheObjectImpl)row.key()).value());

                        if (reconConsist && lowerKey != null && KEY_COMPARATOR.compare(lowerKey, row.key()) >= 0)
                            i--;
                        else if (reconConsist && (lowerKey == null || KEY_COMPARATOR.compare(lowerKey, row.key()) < 0)) {
//                            System.out.println("asdghfgjioiyil " + ((KeyCacheObjectImpl)row.key()).value());

                            newLowerKey = row.key();

                            partEntryHashRecords.add(new VersionedKey(
                                ignite.localNode().id(),
                                row.key(),
                                row.version()
                            ));
                        }

                        hasNext = cursor.next();
                    }

//                    System.out.println("wfgsbfdgb reconSize " + reconSize);
//                    System.out.println("fsgbfdgnhn partReconciliationCtx.lastKey(cacheId) " + partReconciliationCtx.lastKey(cacheId));
//                    System.out.println("fgnnytnjm partReconciliationCtx.isReconciliationInProgress(cacheId) " + partReconciliationCtx.isReconciliationInProgress(cacheId));

                    if (reconSize && !hasNext/*partReconciliationCtx.endOfPart.get(cacheId) != null*/ && ((partReconciliationCtx.lastKey(cacheId) == null || /*oldBorderKey == null ||*/ partReconciliationCtx.lastKey(cacheId).equals(oldBorderKey)) && (lowerKey == null || lowerKey.equals(newLowerKey))) && partReconciliationCtx.isReconciliationInProgress(cacheId)) {

//                        System.out.println("qvdrftga2 after iteration partSize " + partSize.get());

                        nodeSize.lastKey = (partReconciliationCtx.lastKey(cacheId));

                        cacheDataStore.block();

//                        System.out.println("qdsaftpg start second busy lock");

//                        System.out.println("qdresdvscs tempMap " + tempMap);

                        try {
                            partReconciliationCtx.isReconciliationInProgress(cacheId, false);
//                        *******************************************
                            Iterator<Map.Entry<KeyCacheObject, Boolean>> tempMapIter = tempMap.entrySet().iterator();

//                        System.out.println("tempMap " + partReconciliationCtx.tempMap.size() + partReconciliationCtx.tempMap);

                            while (tempMapIter.hasNext()) {
                                Map.Entry<KeyCacheObject, Boolean> entry = tempMapIter.next();

                                partSize.incrementAndGet();

//                                System.out.println("qkoplstfo in recon final: increment reconSize tempMap iter add delta and remove key: key " + ((KeyCacheObjectImpl)entry.getKey()).value() + " reconSize " + partSize);

                            }

                            partReconciliationCtx.isReconciliationInProgress(cacheId, false);

                            i++;
//                            System.out.println("qfvdiohiodf " + i + " " + Thread.currentThread().getName());
//                            System.out.println("qhopluindh old size ************************* partBatch.partitionId() " + partBatch.partitionId() + " cacheId " + cacheId + " " + part);
//                            System.out.println("qpijkhdikg old size ************************* " + cacheDataStore.storageSize.get());
//                            System.out.println("qpooikjgns partSize ************************* " + partSize);

                            nodeSize.oldSize = partSize.get();

                            if (partBatch.repairAlg != RepairAlgorithm.PRINT_ONLY)
                                cacheDataStore.flushReconciliationResult(cacheId);
                            nodeSize.newSize = partSize.get();

                            nodeSize.lastKey = null;

                            nodeSize.inProgress = false;

//                        partEntryHashRecords.clear();
                            partReconciliationCtx.isReconciliationIsFinished.put(cacheId, true);

//                            cacheDataStore.removeReconciliationCtx();
                        }
                        finally {
//                            System.out.println("qdsaftpg end second busy lock");
                            cacheDataStore.unblock();
                        }
                    }

//                    System.out.println("iuhresiughi nodeSize " + Thread.currentThread().getName() + ": " + nodeSize);

                    return new ExecutionResult<>(new T2<>(partEntryHashRecords, nodeSize));
                }
                catch (Exception e) {
                    String errMsg = "Batch [" + partBatch + "] can't processed. Broken cursor.";

                    log.error(errMsg, e);

//                    System.out.println("kiekcuovgs nodeSize " + Thread.currentThread().getName() + ": " + nodeSize);

                    return new ExecutionResult<>(errMsg + " " + e.getMessage());
                }
                finally {

                    part.release();
                }
            }
            else {
//                System.out.println("doduckgsocke nodeSize " + Thread.currentThread().getName() + ": " + nodeSize);

                return new ExecutionResult<>(new T2<>(new ArrayList<>(), nodeSize));
            }
        }
    }
}
