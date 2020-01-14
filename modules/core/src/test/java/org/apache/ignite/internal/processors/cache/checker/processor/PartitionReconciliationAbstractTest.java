/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*      http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.ignite.internal.processors.cache.checker.processor;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.CacheObjectImpl;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.KeyCacheObjectImpl;
import org.apache.ignite.internal.processors.cache.checker.objects.PartitionReconciliationResult;
import org.apache.ignite.internal.processors.cache.checker.objects.ReconciliationResult;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.verify.PartitionReconciliationDataRowMeta;
import org.apache.ignite.internal.processors.cache.verify.checker.tasks.PartitionReconciliationProcessorTask;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cacheobject.IgniteCacheObjectProcessor;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.checker.VisorPartitionReconciliationTaskArg;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class PartitionReconciliationAbstractTest extends GridCommonAbstractTest {
    public static ReconciliationResult partitionReconciliation(Ignite ig, boolean fixMode, String... caches) {
        return partitionReconciliation(
            ig,
            new VisorPartitionReconciliationTaskArg.Builder()
                .caches(new HashSet<>(Arrays.asList(caches)))
                .fixMode(fixMode)
        );
    }

    public static ReconciliationResult partitionReconciliation(
        Ignite ig,
        VisorPartitionReconciliationTaskArg.Builder argBuilder
    ) {
        IgniteEx ig0 = (IgniteEx)ig;

        ClusterNode node = !ig0.localNode().isClient() ? ig0.localNode() : ig0.cluster().forServers().forRandom().node();

        if (node == null)
            throw new IgniteException("No server node for verification.");

        return ig.compute().execute(
            PartitionReconciliationProcessorTask.class.getName(),
            argBuilder.build()
        );
    }

    public static <T> Set<T> conflictKeys(
        IgniteEx ig,
        ReconciliationResult res,
        String cacheName,
        byte keyType
    ) {
        PartitionReconciliationResult res0 = res.partitionReconciliationResult();

        HashSet<T> keys = new HashSet<>();

        Map<String, Map<Integer, List<PartitionReconciliationDataRowMeta>>> inconsistentKeys = res0.inconsistentKeys();

        IgniteInternalCache<Object, Object> cachex = ig.cachex(cacheName);

        CacheObjectContext coCtx = cachex.context().cacheObjectContext();

        IgniteCacheObjectProcessor coProc = ig.context().cacheObjects();

        for (Map.Entry<String, Map<Integer, List<PartitionReconciliationDataRowMeta>>> cacheEntry : inconsistentKeys.entrySet()) {
            for (Map.Entry<Integer, List<PartitionReconciliationDataRowMeta>> partitionEntry : cacheEntry.getValue().entrySet()) {
                for (PartitionReconciliationDataRowMeta meta : partitionEntry.getValue()) {
                    try {
                        Object key = coProc.toKeyCacheObject(coCtx, keyType, meta.keyMeta().binaryView());

                        keys.add((T)key);
                    }
                    catch (IgniteCheckedException e) {
                        throw new IgniteException(e);
                    }
                }
            }
        }

        return keys;
    }

    public static void assertResultContainsConflictKeys(
        IgniteEx ig,
        ReconciliationResult res,
        String cacheName,
        Collection<?> keys,
        byte keyType
    ) {
        PartitionReconciliationResult res0 = res.partitionReconciliationResult();

        HashSet<Object> unmatchedKeys = new HashSet<>();

        Map<String, Map<Integer, List<PartitionReconciliationDataRowMeta>>> inconsistentKeys = res0.inconsistentKeys();

        IgniteInternalCache<Object, Object> cachex = ig.cachex(cacheName);

        CacheObjectContext coCtx = cachex.context().cacheObjectContext();

        IgniteCacheObjectProcessor coProc = ig.context().cacheObjects();

        for (Map.Entry<String, Map<Integer, List<PartitionReconciliationDataRowMeta>>> cacheEntry : inconsistentKeys.entrySet()) {
            for (Map.Entry<Integer, List<PartitionReconciliationDataRowMeta>> partitionEntry : cacheEntry.getValue().entrySet()) {
                for (PartitionReconciliationDataRowMeta meta : partitionEntry.getValue()) {
                    try {
                        Object key = coProc.toKeyCacheObject(coCtx, keyType, meta.keyMeta().binaryView());

                        unmatchedKeys.remove(key);
                    }
                    catch (IgniteCheckedException e) {
                        fail("Failed to unmarshal key from partition reconciliation result: " + e);
                    }
                }
            }
        }

        assertTrue("The following keys are not present in the result: " + unmatchedKeys, unmatchedKeys.isEmpty());
    }

    public static void simulateOutdatedVersionCorruption(GridCacheContext<?, ?> ctx, Object key) {
        corruptDataEntry(ctx, key, false, true, new GridCacheVersion(0, 0, 0L), "_broken");
    }

    public static void simulateMissingEntryCorruption(GridCacheContext<?, ?> ctx, Object key) {
        GridCacheAdapter<Object, Object> cache = (GridCacheAdapter<Object, Object>)ctx.cache();

        cache.clearLocally(key);
    }

    /**
     * TODO we need to stop copypasting this code to every test.
     *
     * Corrupts data entry.
     *
     * @param ctx Context.
     * @param key Key.
     * @param breakCntr Break counter.
     * @param breakData Break data.
     * @param ver GridCacheVersion to use.
     * @param brokenValPostfix Postfix to add to value if breakData flag is set to true.
     */
    private static void corruptDataEntry(
        GridCacheContext<?, ?> ctx,
        Object key,
        boolean breakCntr,
        boolean breakData,
        GridCacheVersion ver,
        String brokenValPostfix
    ) {
        int partId = ctx.affinity().partition(key);

        try {
            long updateCntr = ctx.topology().localPartition(partId).updateCounter();

            Object valToPut = ctx.cache().keepBinary().get(key);

            if (breakCntr)
                updateCntr++;

            if (breakData)
                valToPut = valToPut.toString() + brokenValPostfix;

            // Create data entry
            DataEntry dataEntry = new DataEntry(
                ctx.cacheId(),
                new KeyCacheObjectImpl(key, null, partId),
                new CacheObjectImpl(valToPut, null),
                GridCacheOperation.UPDATE,
                new GridCacheVersion(),
                ver,
                0L,
                partId,
                updateCntr
            );

            IgniteCacheDatabaseSharedManager db = ctx.shared().database();

            db.checkpointReadLock();

            try {
                U.invoke(GridCacheDatabaseSharedManager.class, db, "applyUpdate", ctx, dataEntry,
                    false);
            }
            finally {
                db.checkpointReadUnlock();
            }
        }
        catch (IgniteCheckedException e) {
            e.printStackTrace();
        }
    }
}
