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

//         if (lastKey == null)
//             System.out.println("qgrtsngd null");
//         else
//             System.out.println("qgrtsngd " + ((KeyCacheObjectImpl)lastKey).value());

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
            IgniteCache<Object, Object> cache = ignite.cache(partBatch.cacheName());

            GridCacheContext<Object, Object> cctx = ignite.context().cache().cache(partBatch.cacheName()).context();

            CacheGroupContext grpCtx = cctx.group();

            int cacheId = cctx.cacheId();

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

            IgniteCacheOffheapManager.CacheDataStore cacheDataStore = grpCtx.offheap().dataStore(part);

            assert part != null;

            part.reserve();

            IgniteCacheOffheapManagerImpl.CacheDataStoreImpl.ReconciliationContext partReconciliationCtx = cacheDataStore.reconciliationCtx();

//            if (lowerKey == null)
//                synchronized (partReconciliationCtx.reconciliationMux()) {
//                    partReconciliationCtx.isReconciliationInProgress(true);
//                }

            KeyCacheObject lastKeyForSizes = partReconciliationCtx.lastKey(cacheId);

            KeyCacheObject keyToStart = null;

            if (lowerKey != null && lastKeyForSizes != null)
                keyToStart = KEY_COMPARATOR.compare(lowerKey, lastKeyForSizes) < 0 ? lowerKey : lastKeyForSizes;
            else if (lowerKey != null)
                keyToStart = lowerKey;
            else if (lastKeyForSizes != null)
                keyToStart = lastKeyForSizes;


//            synchronized (partReconciliationCtx.reconciliationMux()) {

            GridCursor<? extends CacheDataRow> cursor = null;

            CacheDataRow row = null;

            boolean first = false;

//            try {
//                sleep(2000);
//            }
//            catch (InterruptedException e) {
//                e.printStackTrace();
//            }

//            try {
//                GridCursor<? extends CacheDataRow> cursor1 = grpCtx.offheap().dataStore(part).cursor(cacheId, null, null);
//
//                List<KeyCacheObject> keys = new ArrayList<>();
//
//                while (cursor1.next()) {
//                    KeyCacheObject key = cursor1.get().key();
//                    keys.add(key);
//                }
//                cursor1.close();
//            }
//            catch (Exception e) {
//                e.printStackTrace();
//            }

            partReconciliationCtx.keysInBatch.putIfAbsent(cacheId, Collections.newSetFromMap(new ConcurrentHashMap<KeyCacheObject, Boolean>()));

            Set<KeyCacheObject> keysInBatch = partReconciliationCtx.keysInBatch.get(cacheId);

            AtomicLong partSize = new AtomicLong();

            try {
                partReconciliationCtx.lock.writeLock().lock();

                try /*synchronized (partReconciliationCtx.reconciliationMux())*/ {

                    Long partSize0 = partBatch.partSizesMap().get(ignite.localNode().id());

                    if (partSize0 != null)
                        partSize.set(partSize0);
                    else
                        System.out.println("qefdsrsdf");

                    partReconciliationCtx.keysAfterCounters.putIfAbsent(cacheId, new AtomicLong());

                    partReconciliationCtx.keysAfter.putIfAbsent(cacheId, new ConcurrentHashMap<>());

                    System.out.println("qcdsfre2 keysAfter " + partReconciliationCtx.keysAfter);

////                    if (!keysInBatch.isEmpty()) {
//                        partReconciliationCtx.keysAfter.get(cacheId).entrySet().forEach(e -> {
//                            if (KEY_COMPARATOR.compare(e.getKey(), partReconciliationCtx.lastKey(cacheId)) <= 0 &&
//                                KEY_COMPARATOR.compare(e.getKey(), partReconciliationCtx.firstKey(cacheId)) >= 0) {
//                                long count = e.getValue().get();
//                                if (count > 0 && !keysInBatch.contains(e.getKey())) {
//                                    partSize.addAndGet(count);
//                                    System.out.println("qwqwfdeds keysAfter after check increment" + ((KeyCacheObjectImpl)e.getKey()).value());
//                                }
//                                if (count < 0 && keysInBatch.contains(e.getKey())) {
//                                    partSize.addAndGet(count);
//                                    System.out.println("qcdsvrdsv keysAfter after check decrement" + ((KeyCacheObjectImpl)e.getKey()).value());
//                                }
//                                System.out.println("qwddxvfgrt2 key: " + ((KeyCacheObjectImpl)e.getKey()).value() + ", value: " + count);
//                            }
//                        });
//
//                        keysInBatch.clear();
////                    }

//                    partReconciliationCtx.keysAfter.get(cacheId).clear();

//                    partReconciliationCtx.keysInBatch.get(cacheId).clear();

//                    partReconciliationCtx.keysAfter.get(cacheId).clear();

                    partReconciliationCtx.isReconciliationInProgress(true);
                    partReconciliationCtx.isBatchesInProgress(true);

//                    if (partReconciliationCtx.lastKey(cacheId) != null)
//                        partReconciliationCtx.firstKey(cacheId, partReconciliationCtx.lastKey(cacheId));

//                    partReconciliationCtx.isBatchInProgress = true;

                    System.out.println("qfbaftgr before cursor partSize " + partSize.get());

//                    System.out.println("qfdvgrtd value of key0: " + cache.get(0));

                    cursor = keyToStart == null ?
                        grpCtx.offheap().dataStore(part).cursor(cacheId, null, null) :
                        grpCtx.offheap().dataStore(part).cursor(cacheId, keyToStart, null);

                    System.out.println("qfbaftgr after cursor");

                    if (cursor.next()) {
                        row = cursor.get();

                        if (partReconciliationCtx.lastKey(cacheId) != null && partReconciliationCtx.lastKey(cacheId).equals(row.key())) {
                            row = null;

                            if (cursor.next())
                                row = cursor.get();
                        }

                        System.out.println("qefrasgbdt1");
                    }

                    if (row != null) {
                        first = true;

//                    if (!keysInBatch.isEmpty()) {
                        partReconciliationCtx.keysAfter.get(cacheId).entrySet().forEach(e -> {
                            if (KEY_COMPARATOR.compare(e.getKey(), partReconciliationCtx.lastKey(cacheId)) <= 0 &&
                                KEY_COMPARATOR.compare(e.getKey(), partReconciliationCtx.firstKey(cacheId)) >= 0) {
                                long count = e.getValue().get();
                                if (count > 0 && !keysInBatch.contains(e.getKey())) {
                                    partSize.addAndGet(count);
                                    System.out.println("qwqwfdeds keysAfter after check increment" + ((KeyCacheObjectImpl)e.getKey()).value());
                                }
                                if (count < 0 && keysInBatch.contains(e.getKey())) {
                                    partSize.addAndGet(count);
                                    System.out.println("qcdsvrdsv keysAfter after check decrement" + ((KeyCacheObjectImpl)e.getKey()).value());
                                }
                                System.out.println("qwddxvfgrt2 key: " + ((KeyCacheObjectImpl)e.getKey()).value() + ", value: " + count);
                            }
                        });

                        keysInBatch.clear();

                        partReconciliationCtx.keysAfter.get(cacheId).clear();
//                    }

                        keysInBatch.add(row.key());

                        partReconciliationCtx.firstKey(cacheId, row.key());

                        partReconciliationCtx.lastKey(cacheId, row.key());

//                            partReconciliationCtx.keysAfter.clear();

                    }
                    else {
//                        partReconciliationCtx.removeFirstKey(cacheId);
//                        partReconciliationCtx.isBatchesInProgress(false);
                    }

                    CacheDataRow row0 = row;

                    System.out.println("qffvsre1 before iteration partSize " + partSize.get());

//                    synchronized (partReconciliationCtx.reconciliationMux()) {
//                        System.out.println("qsfvrsv1 " + partReconciliationCtx.keysAfter);
//                        partReconciliationCtx.keysAfter.entrySet().forEach(e -> {
//                            if (row0 == null|| partReconciliationCtx.lastKey(cacheId) == null || KEY_COMPARATOR.compare(e.getKey(), partReconciliationCtx.lastKey(cacheId)) <= 0) {
//                                long count = e.getValue().get();
//                                partSize.addAndGet(count);
////                                if (count > 0 && !keysInBatch.contains(e.getKey())) {
////                                    partSize.addAndGet(count);
//                                    System.out.println("qsdvsd keysAfter first after check increment" + ((KeyCacheObjectImpl)e.getKey()).value());
////                                }
////                                if (count < 0 && keysInBatch.contains(e.getKey())) {
////                                    partSize.addAndGet(count);
////                                    System.out.println("qcdsvrdsv keysAfter first after check decrement" + ((KeyCacheObjectImpl)e.getKey()).value());
////                                }
//                                System.out.println("qdsvrafdv " + ((KeyCacheObjectImpl)e.getKey()).value());
//                            }
//                        });
//
//                        partReconciliationCtx.keysAfterCounters.get(cacheId).set(0);
//
//                        partReconciliationCtx.keysAfter.clear();
//                    }

//                    if (row == null) {
//                        grpCtx.offheap().dataStore(part).flushReconciliationResult(cacheId, partSize.get());
//                    }

                    System.out.println("qsersvsdd1 partSize " + partSize.get());
                }
                finally {
                    partReconciliationCtx.lock.writeLock().unlock();
                }
            }
            catch (IgniteCheckedException e) {
                e.printStackTrace();
            }

            try {
                List<VersionedKey> partEntryHashRecords = new ArrayList<>();

//                synchronized (partReconciliationCtx.reconciliationMux()) {

                for (int i = 0; i < batchSize && (row != null || cursor.next()); i++) {
//                    System.out.println("qfvndrfg");

                    try {
                        sleep(2);
                    }
                    catch (InterruptedException e) {
                        e.printStackTrace();
                    }

//                    synchronized (partReconciliationCtx.reconciliationMux()) {
                        if (row == null) {
                            row = cursor.get();

                            System.out.println("qefrasgbdt2");
                        }

                    keysInBatch.add(row.key());

                        if (first || /*partReconciliationCtx.firstKey(cacheId) == null ||*/ KEY_COMPARATOR.compare(partReconciliationCtx.lastKey(cacheId), row.key()) < 0) {
                            partSize.incrementAndGet();
                        }

                        if (first || /*partReconciliationCtx.firstKey(cacheId) == null ||*/ KEY_COMPARATOR.compare(partReconciliationCtx.lastKey(cacheId), row.key()) < 0) {

                            partReconciliationCtx.keysAfterCounters.get(cacheId).set(0);


//                            partReconciliationCtx.keysCheckedInBatch.get(cacheId).add(row.key());

                            partReconciliationCtx.lastKey(cacheId, row.key());

//                            partReconciliationCtx.firstKey(cacheId, row.key());
//                            System.out.println("qqedfks1 " + ignite.localNode().id() +
//                                " reconcilation execute0 if. _cacheDataStore.lastKey()_: " + (cacheDataStore.lastKey() == null ? "null" : cacheDataStore.lastKey()) +
//                                " ||| _row.key()_:" + row.key() +
//                                " ||| compare: " + (cacheDataStore.lastKey() == null ? "null" : KEY_COMPARATOR.compare(cacheDataStore.lastKey(), row.key())) +
//                                " ||| partId: " + part +
//                                " ||| partSize: " + partSize);
                            System.out.println("qqedfks2 " + ignite.localNode().id() +
                                " reconcilation execute0 if. _cacheDataStore.lastKey()_: " + (((KeyCacheObjectImpl)partReconciliationCtx.lastKey(cacheId)).value() == null ? "null" : ((KeyCacheObjectImpl)partReconciliationCtx.lastKey(cacheId)).value()) +
                                " ||| _row.key()_:" + ((KeyCacheObjectImpl)row.key()).value() +
                                " ||| compare: " + (((KeyCacheObjectImpl)partReconciliationCtx.lastKey(cacheId)).value() == null ? "null" : ((Integer)((KeyCacheObjectImpl)partReconciliationCtx.lastKey(cacheId)).value()) > ((Integer)((KeyCacheObjectImpl)row.key()).value())) +
                                " ||| partId: " + part +
                                " ||| partSize: " + partSize);
                        }
                        else {
//                            System.out.println("qftsbg1 " + ignite.localNode().id() +
//                                " reconcilation execute0 else. _cacheDataStore.lastKey()_: " + (cacheDataStore.lastKey() == null ? "null" : cacheDataStore.lastKey()) +
//                                " ||| _row.key()_:" + cacheDataStore.lastKey() +
//                                " ||| compare: " + (cacheDataStore.lastKey() == null ? "null" : KEY_COMPARATOR.compare(cacheDataStore.lastKey(), row.key())) +
//                                " ||| partId: " + part +
//                                " ||| partSize: " + partSize);
                            System.out.println("qftsbg2 " + ignite.localNode().id() +
                                " reconcilation execute0 else. _cacheDataStore.lastKey()_: " + (((KeyCacheObjectImpl)partReconciliationCtx.lastKey(cacheId)).value() == null ? "null" : ((KeyCacheObjectImpl)partReconciliationCtx.lastKey(cacheId)).value()) +
                                " ||| _row.key()_:" + ((KeyCacheObjectImpl)row.key()).value() +
                                " ||| compare: " + (((KeyCacheObjectImpl)partReconciliationCtx.lastKey(cacheId)).value() == null ? "null" : ((Integer)((KeyCacheObjectImpl)partReconciliationCtx.lastKey(cacheId)).value()) > ((Integer)((KeyCacheObjectImpl)row.key()).value())) +
                                " ||| partId: " + part +
                                " ||| partSize: " + partSize);
                        }
//                        }

//                    System.out.println("qdvrfgad " + ignite.localNode().id() + " _row.key()_:" + ((KeyCacheObjectImpl)row.key()).value() + ", lowerKey: " + (lowerKey == null ? "null" : ((KeyCacheObjectImpl)lowerKey).value()) + ", row.key(): " + row.key() + ", lowerKey: " + lowerKey);
                        if (lowerKey == null || KEY_COMPARATOR.compare(lowerKey, row.key()) < 0) {
//                    if (lowerKey == null || !((KeyCacheObjectImpl)row.key()).value().equals(((KeyCacheObjectImpl)lowerKey).value())) {
//                        partSize++;
//                        System.out.println("qdrvgsrwe partSize: " + partSize);
//                            System.out.println("qwerdcs " + ignite.localNode().id() + " _row.key()_:" + ((KeyCacheObjectImpl)row.key()).value());
                            partEntryHashRecords.add(new VersionedKey(
                                ignite.localNode().id(),
                                row.key(),
                                row.version()
                            ));
                        }
                        else
                            i--;
//                    }

                    row = null;

                    first = false;
                }

//                try {
//                    sleep(2);
//                }
//                catch (InterruptedException e) {
//                    e.printStackTrace();
//                }

                System.out.println("qvdrftga2 after iteration " + partSize.get());

//                if (partReconciliationCtx.firstKey(cacheId) != null) {

//                try {
//                    sleep(100);
//                }
//                catch (InterruptedException e) {
//                    e.printStackTrace();
//                }

//                partReconciliationCtx.lock.writeLock().lock();
//
//                try /*synchronized (partReconciliationCtx.reconciliationMux())*/ {
//                        System.out.println("qfrskotd2 " + partReconciliationCtx.keysAfter);
//
////                    if (row != null) {
////                    if (!keysInBatch.isEmpty()) {
////                        partReconciliationCtx.keysAfter.get(cacheId).entrySet().forEach(e -> {
////                            if (KEY_COMPARATOR.compare(e.getKey(), partReconciliationCtx.lastKey(cacheId)) <= 0 &&
////                                KEY_COMPARATOR.compare(e.getKey(), partReconciliationCtx.firstKey(cacheId)) >= 0) {
////                                long count = e.getValue().get();
////                                if (count > 0 && !keysInBatch.contains(e.getKey())) {
////                                    partSize.addAndGet(count);
////                                    System.out.println("qwqwfdeds keysAfter after check increment" + ((KeyCacheObjectImpl)e.getKey()).value());
////                                }
////                                if (count < 0 && keysInBatch.contains(e.getKey())) {
////                                    partSize.addAndGet(count);
////                                    System.out.println("qcdsvrdsv keysAfter after check decrement" + ((KeyCacheObjectImpl)e.getKey()).value());
////                                }
////                                System.out.println("qwddxvfgrt2 key: " + ((KeyCacheObjectImpl)e.getKey()).value() + ", value: " + count);
////                            }
////                        });
////
////                        partReconciliationCtx.keysAfter.get(cacheId).clear();
////                    }
//
////                        if (keysInBatch.isEmpty())
////                            partReconciliationCtx.isBatchesInProgress(false);
//
//                        partReconciliationCtx.keysAfterCounters.get(cacheId).set(0);
//
//
//                    System.out.println("qcfdrfae2 " + partSize.get());
//
//                    }
//                    finally {
//                        partReconciliationCtx.lock.writeLock().unlock();
//                }

//                    if (row == null) {
//                        try {
//                            sleep(100);
//                        }
//                        catch (InterruptedException e) {
//                            e.printStackTrace();
//                        }
//
//
//                    }
//                }
//                else {
//                    scheduleHighPriority(new RepairSizes(partBatch.sessionId(), partBatch.workloadChainId(), partBatch.cacheName(), partBatch.partitionId(), partBatch.partSizesMap()));
//                }

//                try {
//                    sleep(2);
//                }
//                catch (InterruptedException e) {
//                    e.printStackTrace();
//                }

//                System.out.println("qflyruc cursor.next(): " + cursor.next());

                return new ExecutionResult<>(new T2<>(partEntryHashRecords, partSize.get()));
            }
            catch (Exception e) {
                String errMsg = "Batch [" + partBatch + "] can't processed. Broken cursor.";

                log.error(errMsg, e);

                return new ExecutionResult<>(errMsg + " " + e.getMessage());
            }
            finally {

                try {
                    cursor.close();
                }
                catch (Exception e) {
                    e.printStackTrace();
                }

                part.release();
            }
//            }
        }
    }
}
