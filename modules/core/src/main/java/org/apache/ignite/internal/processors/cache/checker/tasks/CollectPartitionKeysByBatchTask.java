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

import com.sun.org.apache.xpath.internal.operations.Bool;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import javax.cache.Cache;
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
import org.apache.ignite.internal.processors.cache.checker.objects.PartitionBatchRequest;
import org.apache.ignite.internal.processors.cache.checker.objects.VersionedKey;
import org.apache.ignite.internal.processors.cache.checker.processor.workload.RepairSizes;
import org.apache.ignite.internal.processors.cache.checker.util.KeyComparator;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
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
public class CollectPartitionKeysByBatchTask extends ComputeTaskAdapter<PartitionBatchRequest, ExecutionResult<T3<KeyCacheObject, Map<KeyCacheObject, Map<UUID, GridCacheVersion>>, Map<UUID, Long>>>> {
    /**
     *
     */
    private static final long serialVersionUID = 0L;

    public static int i;
    public static int iCleanup;
    public static int iUpdate;

    public static final CountDownLatch latch = new CountDownLatch(1);

    public static final Map<Integer, String> msg = new ConcurrentHashMap<>();
    public static final Map<Integer, String> msg1 = new ConcurrentHashMap<>();

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
    @Override public @Nullable ExecutionResult<T3<KeyCacheObject, Map<KeyCacheObject, Map<UUID, GridCacheVersion>>, Map<UUID, Long>>> reduce(
        List<ComputeJobResult> results) throws IgniteException {
        assert partBatch != null;

        GridCacheContext<Object, Object> ctx = ignite.context().cache().cache(partBatch.cacheName()).context();

        Map<KeyCacheObject, Map<UUID, GridCacheVersion>> totalRes = new HashMap<>();

        KeyCacheObject lastKey = null;

        Map<UUID, Long> partSizesMap = new HashMap<>();

        for (int i = 0; i < results.size(); i++) {
            UUID nodeId = results.get(i).getNode().id();

            IgniteException exc = results.get(i).getException();

            if (exc != null)
                return new ExecutionResult<>(exc.getMessage());

            ExecutionResult<T2<List<VersionedKey>, Long>> nodeRes = results.get(i).getData();

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

        System.out.println("qgrtsngd " + results);

        if (lastKey == null)
            System.out.println("qgrtsngd null");
        else
            System.out.println("qgrtsngd " + ((KeyCacheObjectImpl)lastKey).value());
        System.out.println("qgrtsngd " + lastKey);

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
        @Override protected ExecutionResult<T2<List<VersionedKey>, Long>> execute0() {
            System.out.println("qdsvdsdfd start recon");
            IgniteCache<Object, Object> cache = ignite.cache(partBatch.cacheName());

            GridCacheContext<Object, Object> cctx = ignite.context().cache().cache(partBatch.cacheName()).context();

            CacheGroupContext grpCtx = cctx.group();

//            int cacheId = cctx.cacheId();
            int cacheId = grpCtx.sharedGroup() ? cctx.cacheId() : CU.UNDEFINED_CACHE_ID;

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

            IgniteCacheOffheapManagerImpl.CacheDataStoreImpl cacheDataStore = (IgniteCacheOffheapManagerImpl.CacheDataStoreImpl) grpCtx.offheap().dataStore(part);

            assert part != null;

            part.reserve();

            IgniteCacheOffheapManagerImpl.CacheDataStoreImpl.ReconciliationContext partReconciliationCtx = cacheDataStore.reconciliationCtx();

            if (!partReconciliationCtx.isReconciliationInProgress(cacheId) && partReconciliationCtx.lastKey(cacheId) == null) {
                cacheDataStore.busyLock.block();
                System.out.println("qdsaftpg start first busy lock");

                try {
                    partReconciliationCtx.isReconciliationInProgress(cacheId, true);
                    System.out.println("qlopfots set isReconciliationInProgress to true");
//                    partReconciliationCtx.cacheId = cacheId;
                }
                finally {
                    System.out.println("qdsaftpg end first busy lock");
                    cacheDataStore.busyLock.unblock();
                }
            }

            KeyCacheObject lastKeyForSizes = partReconciliationCtx.lastKey(cacheId);

            KeyCacheObject keyToStart = null;

//            if (lowerKey != null && lastKeyForSizes != null)
//                keyToStart = KEY_COMPARATOR.compare(lowerKey, lastKeyForSizes) < 0 ? lowerKey : lastKeyForSizes;
//            else if (lowerKey != null)
//                keyToStart = lowerKey;
            /*else */if (lastKeyForSizes != null)
                keyToStart = lastKeyForSizes;

            partReconciliationCtx.sizes.putIfAbsent(cacheId, new AtomicLong());

            AtomicLong partSize = partReconciliationCtx.sizes.get(cacheId);

            KeyCacheObject oldBorderKey = partReconciliationCtx.lastKey(cacheId);

            KeyCacheObject newLastKey = null;

            partReconciliationCtx.tempMap.putIfAbsent(cacheId, new ConcurrentHashMap<>());

            Map<KeyCacheObject, Boolean> tempMap = partReconciliationCtx.tempMap.get(cacheId);

            CacheDataRow lastRow = null;

            i++;

            try (GridCursor<? extends CacheDataRow> cursor = keyToStart == null ?
                ((IgniteCacheOffheapManagerImpl.CacheDataStoreImpl) grpCtx.offheap().dataStore(part)).reconCursor(cacheId, null, null, null, null, IgniteCacheOffheapManager.DATA) :
                ((IgniteCacheOffheapManagerImpl.CacheDataStoreImpl) grpCtx.offheap().dataStore(part)).reconCursor(cacheId, keyToStart, null, null, null, IgniteCacheOffheapManager.DATA)) {
                System.out.println("qdsadfvers start cursor");

                List<VersionedKey> partEntryHashRecords = new ArrayList<>();

                boolean isEmptyCursor = true;

                int iters = 0;

                for (int i = 0; (i < batchSize /*&& (newLastKey == null || !newLastKey.equals(partReconciliationCtx.lastKey(cacheId)))*/ && cursor.next()); i++) {
                    isEmptyCursor = false;

                    iters++;

//                    try {
//                        sleep(1);
//                    }
//                    catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }

                    CacheDataRow row = cursor.get();
                    newLastKey = partReconciliationCtx.lastKey(cacheId);//row.key();

                    if (oldBorderKey != null && KEY_COMPARATOR.compare(oldBorderKey, newLastKey) <= 0)
                        i--;

                    lastRow = row;

                    System.out.println("qwdsfsf newLastKey " + newLastKey);
                    System.out.println("qwdsfsf end of iteration " + iters + " || " + newLastKey + " || " + partReconciliationCtx.lastKey(cacheId) + " " + Thread.currentThread().getName());

                    latch.countDown();
                }

                newLastKey = partReconciliationCtx.lastKey(cacheId);

                if (newLastKey != null && (oldBorderKey == null || KEY_COMPARATOR.compare(oldBorderKey, newLastKey) < 0)) {
                    partEntryHashRecords.add(new VersionedKey(
                        ignite.localNode().id(),
//                            row.key(),
                        partReconciliationCtx.lastKey(cacheId),
                        new GridCacheVersion()
                    ));
                }

                System.out.println("qzsdfvfe after newLastKey iters " + iters);

//                if (newLastKey != null)
//                    partReconciliationCtx.lastKeys().put(cacheId, newLastKey);

                System.out.println("qvdrftga2 after iteration partSize " + partSize.get());
                System.out.println("qvdrftga2 after iteration newLastKey " + newLastKey + " oldBorderKey " + oldBorderKey);

                if ((partReconciliationCtx.lastKey(cacheId) == null || /*oldBorderKey == null ||*/ partReconciliationCtx.lastKey(cacheId).equals(oldBorderKey)) && partReconciliationCtx.isReconciliationInProgress(cacheId)) {
//                if (partSize.get() == 300) {
                    cacheDataStore.busyLock.block();
                    System.out.println("qdsaftpg start second busy lock");

                    System.out.println("qdresdvscs tempMap " + tempMap);

                    try {
                        partReconciliationCtx.isReconciliationInProgress(cacheId, false);
//                        *******************************************
                        Iterator<Map.Entry<KeyCacheObject, Boolean>> tempMapIter = tempMap.entrySet().iterator();

//                        System.out.println("tempMap " + part.tempMap.size() + part.tempMap);

                        while (tempMapIter.hasNext()) {
                            Map.Entry<KeyCacheObject, Boolean> entry = tempMapIter.next();

                            partSize.addAndGet(1);

                            System.out.println("qkoplstfo in recon final increment: key " + entry.getKey() + " reconSize " + partSize);

                        }

                        partReconciliationCtx.isReconciliationInProgress(cacheId, false);

                        System.out.println("qfgtopes old size ************************* " + cacheDataStore.storageSize.get());
                        System.out.println("qfgtopes partSize ************************* " + partSize);

//                        cacheDataStore.storageSize.set(partSize.get());
                        cacheDataStore.flushReconciliationResult();
                    }
                    finally {
                        System.out.println("qdsaftpg end second busy lock");
                        cacheDataStore.busyLock.unblock();
                    }
                }

                return new ExecutionResult<>(new T2<>(partEntryHashRecords, partSize.get()));
            }
            catch (Exception e) {
                String errMsg = "Batch [" + partBatch + "] can't processed. Broken cursor.";

                log.error(errMsg, e);

                return new ExecutionResult<>(errMsg + " " + e.getMessage());
            }
            finally {

                part.release();
            }
//            }
        }
    }
}
