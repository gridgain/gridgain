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
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.checker.objects.ReconciliationResult;
import org.apache.ignite.internal.processors.cache.verify.PartitionReconciliationDataRowMeta;
import org.apache.ignite.internal.processors.cache.verify.PartitionReconciliationKeyMeta;
import org.apache.ignite.internal.processors.cache.verify.RepairAlgorithm;
import org.apache.ignite.internal.processors.cache.verify.checker.tasks.PartitionReconciliationProcessorTask;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.checker.VisorPartitionReconciliationTaskArg;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.TestStorageUtils.corruptDataEntry;

/**
 * Abstract utility class for partition reconciliation testing.
 */
public class PartitionReconciliationAbstractTest extends GridCommonAbstractTest {
    /** */
    public static ReconciliationResult partitionReconciliation(
        Ignite ig,
        boolean repair,
        @Nullable RepairAlgorithm repairAlgorithm,
        int parallelism,
        int batchSize,
        String... caches
    ) {
        return partitionReconciliation(
            ig,
            new VisorPartitionReconciliationTaskArg.Builder()
                .caches(new HashSet<>(Arrays.asList(caches)))
                .recheckDelay(1)
                .batchSize(batchSize)
                .parallelism(parallelism)
                .repair(repair)
                .repairAlg(repairAlgorithm)
        );
    }

    /** */
    public static ReconciliationResult partitionReconciliation(
        Ignite ig,
        boolean repair,
        @Nullable RepairAlgorithm repairAlgorithm,
        int parallelism,
        String... caches
    ) {
        return partitionReconciliation(ig, repair, repairAlgorithm, parallelism, 1000, caches);
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
}
