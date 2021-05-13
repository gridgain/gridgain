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

package org.apache.ignite.internal.processors.cache.checker.processor;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.checker.objects.ReconciliationResult;
import org.apache.ignite.internal.processors.cache.verify.PartitionReconciliationDataRowMeta;
import org.apache.ignite.internal.processors.cache.verify.PartitionReconciliationKeyMeta;
import org.apache.ignite.internal.processors.cache.verify.ReconType;
import org.apache.ignite.internal.processors.cache.verify.RepairAlgorithm;
import org.apache.ignite.internal.processors.cache.verify.checker.tasks.PartitionReconciliationProcessorTask;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.checker.VisorPartitionReconciliationTaskArg;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.junits.GridAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.log4j.Logger;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.TestStorageUtils.corruptDataEntry;

/**
 * Abstract utility class for partition reconciliation testing.
 */
public class PartitionReconciliationAbstractTest extends GridCommonAbstractTest {
    static final long BROKEN_PART_SIZE = 10;

    /** */
    public static ReconciliationResult partitionReconciliation(
        Ignite ig,
        boolean repair,
        @Nullable RepairAlgorithm repairAlgorithm,
        int parallelism,
        Set<ReconType> reconTypes,
        String... caches
    ) {
        return partitionReconciliation(
            ig,
            new VisorPartitionReconciliationTaskArg.Builder()
                .caches(new HashSet<>(Arrays.asList(caches)))
                .recheckDelay(1)
                .parallelism(parallelism)
                .repair(repair)
                .repairAlg(repairAlgorithm)
                .reconTypes(reconTypes)
        );
    }

    /**
     *
     */
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

    /**
     *
     */
    public static Set<Integer> conflictKeys(ReconciliationResult res, String cacheName) {
        return res.partitionReconciliationResult().inconsistentKeys().get(cacheName)
            .values()
            .stream()
            .flatMap(Collection::stream)
            .map(PartitionReconciliationDataRowMeta::keyMeta)
            .map(k -> (String)U.field(k, "strView"))
            .map(Integer::valueOf)
            .collect(Collectors.toSet());
    }

    /**
     *
     */
    public static <T> Set<T> conflictKeys(ReconciliationResult res, String cacheName, Function<String, T> map) {
        return res.partitionReconciliationResult().inconsistentKeys().get(cacheName)
            .values()
            .stream()
            .flatMap(Collection::stream)
            .map(PartitionReconciliationDataRowMeta::keyMeta)
            .map(k -> (String)U.field(k, "strView"))
            .map(map)
            .collect(Collectors.toSet());
    }

    /**
     *
     */
    public static Set<PartitionReconciliationKeyMeta> conflictKeyMetas(ReconciliationResult res, String cacheName) {
        return res.partitionReconciliationResult().inconsistentKeys().get(cacheName)
            .values()
            .stream()
            .flatMap(Collection::stream)
            .map(PartitionReconciliationDataRowMeta::keyMeta)
            .collect(Collectors.toSet());
    }

    /**
     *
     */
    public static void assertResultContainsConflictKeys(
        ReconciliationResult res,
        String cacheName,
        Set<Integer> keys
    ) {
        for (Integer key : keys)
            assertTrue("Key doesn't contain: " + key, conflictKeys(res, cacheName).contains(key));
    }

    /**
     *
     */
    public static <T> void assertResultContainsConflictKeys(
        ReconciliationResult res,
        String cacheName,
        Function<String, T> map,
        Set<T> keys
    ) {
        for (T key : keys)
            assertTrue("Key doesn't contain: " + key, conflictKeys(res, cacheName, map).contains(key));
    }

    /**
     *
     */
    public static void simulateOutdatedVersionCorruption(GridCacheContext<?, ?> ctx, Object key) {
        simulateOutdatedVersionCorruption(ctx, key, false);
    }

    /**
     *
     */
    public static void simulateOutdatedVersionCorruption(GridCacheContext<?, ?> ctx, Object key, boolean lockEntry) {
        corruptDataEntry(ctx, key, false, true,
            new GridCacheVersion(0, 0, 0L), "_broken", lockEntry);
    }

    /**
     *
     */
    public static void simulateMissingEntryCorruption(GridCacheContext<?, ?> ctx, Object key) {
        GridCacheAdapter<Object, Object> cache = (GridCacheAdapter<Object, Object>)ctx.cache();

        cache.clearLocally(key);
    }

    protected void setPartitionSize(IgniteEx grid, String cacheName, int partId, int delta) {

        GridCacheContext<Object, Object> cctx = grid.context().cache().cache(cacheName).context();

        int cacheId = cctx.cacheId();

        cctx.group().topology().localPartition(partId).dataStore().updateSize(cacheId, delta);
    }

    protected void updatePartitionsSize(IgniteEx grid, String cacheName) {

        GridCacheContext<Object, Object> cctx = grid
            .context()
            .cache()
            .cache(cacheName)
            .context();

        int cacheId = cctx.cacheId();

        cctx.group().topology().localPartitions().forEach(part -> part.dataStore().updateSize(cacheId, BROKEN_PART_SIZE/*Math.abs(rnd.nextInt())*/));
    }

    protected long getFullPartitionsSizeForCacheGroup(List<IgniteEx> nodes, String cacheName) {
        return nodes.stream().mapToLong(node -> getFullPartitionsSizeForCacheGroup(node, cacheName)).sum();
    }

    protected long getFullPartitionsSizeForCacheGroup(IgniteEx grid, String cacheName) {

        GridCacheContext<Object, Object> cctx = grid.context().cache().cache(cacheName).context();

        return cctx.group().topology().localPartitions().stream().mapToLong(part -> part.dataStore().fullSize()).sum();
    }

    protected long getPartitionsSizeForCache(IgniteEx grid, String cacheName) {

        GridCacheContext<Object, Object> cctx = grid.context().cache().cache(cacheName).context();

        int cacheId = cctx.cacheId();

        return cctx.group().topology().localPartitions()
            .stream()
            .mapToLong(part -> {
                    if (cctx.group().sharedGroup())
                        return part
                            .dataStore()
                            .cacheSizes()
                            .get(cacheId);
                    else
                        return part.dataStore().fullSize();
                }
            )
//            .mapToLong(part -> Optional.of(part.dataStore().cacheSizes().get(cacheId)).orElseGet(() -> 0L))
            .sum();
    }

    protected void breakCacheSizes(List<IgniteEx> nodes, List<String> cacheNames) {
        nodes.forEach(node -> {
            cacheNames.forEach(cacheName -> {
                updatePartitionsSize(node, cacheName);
            });
        });
    }

    protected IgniteInternalFuture startAsyncLoad0(AtomicReference<ReconciliationResult> reconResult, IgniteCache cache,
        int startKey, int endKey, boolean clear) {
        Random rnd = new Random();

        return GridTestUtils.runAsync(() -> {
                int op = -1;

                long n = -1;

                while(reconResult.get() == null) {

                    if (clear)
                        op = rnd.nextInt(8);
                    else
                        op = rnd.nextInt(7);

                    switch (op) {
                        case 0:
                            Map map = new TreeMap();

                            n = startKey + rnd.nextInt(endKey - startKey);
                            map.put(n, n);
                            n = startKey + rnd.nextInt(endKey - startKey);
                            map.put(n, n);
                            n = startKey + rnd.nextInt(endKey - startKey);
                            map.put(n, n);
                            n = startKey + rnd.nextInt(endKey - startKey);
                            map.put(n, n);
                            n = startKey + rnd.nextInt(endKey - startKey);
                            map.put(n, n);

                            cache.putAll(map);

                            break;

                        case 1:
                            Set set = new TreeSet();

                            n = startKey + rnd.nextInt(endKey - startKey);
                            set.add(n);
                            n = startKey + rnd.nextInt(endKey - startKey);
                            set.add(n);
                            n = startKey + rnd.nextInt(endKey - startKey);
                            set.add(n);
                            n = startKey + rnd.nextInt(endKey - startKey);
                            set.add(n);
                            n = startKey + rnd.nextInt(endKey - startKey);
                            set.add(n);

                            cache.removeAll(set);

                            break;

                        case 2:
                            n = startKey + rnd.nextInt(endKey - startKey);

                            cache.put(n, n);

                            break;

                        case 3:
                            n = startKey + rnd.nextInt(endKey - startKey);

                            cache.remove(n);

                            break;

                        case 4:
                            n = startKey + rnd.nextInt(endKey - startKey);

                            cache.getAndPut(n, n+1);

                            break;

                        case 5:
                            n = startKey + rnd.nextInt(endKey - startKey);

                            cache.getAndRemove(n);

                            break;

                        case 6:
                            n = startKey + rnd.nextInt(endKey - startKey);

                            long n0 = n;

                            cache.invoke(n, (k, v) -> {
                                if (v == null)
                                    return n0;
                                else
                                    return null;
                            });

                            break;

                        case 7:
                            n = startKey + rnd.nextInt(endKey - startKey);

                            if (n % 10 == 0)
                                cache.clear();

                            break;
                    }
                }
            },
            "LoadThread");
    }
}
