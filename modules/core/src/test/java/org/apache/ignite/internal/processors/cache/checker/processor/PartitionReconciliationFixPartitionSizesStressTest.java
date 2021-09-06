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

package org.apache.ignite.internal.processors.cache.checker.processor;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.verify.ReconciliationType;
import org.apache.ignite.internal.visor.checker.VisorPartitionReconciliationTaskArg;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.internal.processors.cache.verify.ReconciliationType.CACHE_SIZE_CONSISTENCY;
import static org.apache.ignite.internal.processors.cache.verify.ReconciliationType.DATA_CONSISTENCY;

/**
 * Tests partition reconciliation of sizes with various cache configurations.
 */
public class PartitionReconciliationFixPartitionSizesStressTest extends PartitionReconciliationFixPartitionSizesStressAbstractTest {
    /**
     * <ul>
     * <li>Start nodes.</li>
     * <li>Start one or two caches.</li>
     * <li>Load some data.</li>
     * <li>Break cache sizes.</li>
     * <li>Start load threads.</li>
     * <li>Do size reconciliation.</li>
     * <li>Stop load threads.</li>
     * <li>Check size of primary/backup partitions in cluster.</li>
     * </ul>
     */
    @Test
    public void test() throws Exception {
        Set<ReconciliationType> reconciliationTypes = new HashSet<>();

        reconciliationTypes.add(CACHE_SIZE_CONSISTENCY);

        if (rnd.nextBoolean())
            reconciliationTypes.add(DATA_CONSISTENCY);

        log.info(">>> Reconciliation types: " + reconciliationTypes);

        ig = startGrids(nodesCnt);

        client = startClientGrid(nodesCnt);

        ig.cluster().active(true);

        List<IgniteCache<Object, Object>> caches = new ArrayList<>();

        caches.add(client.createCache(
            getCacheConfig(DEFAULT_CACHE_NAME + 0, cacheAtomicityMode, cacheMode, backupCnt, partCnt, cacheGrp)
        ));

        if (rnd.nextBoolean()) {
            caches.add(client.createCache(
                getCacheConfig(DEFAULT_CACHE_NAME + 1, cacheAtomicityMode, cacheMode, backupCnt, partCnt, cacheGrp)
            ));
        }

        log.info(">>> Cache count: " + caches.size());

        Set<String> cacheNames = caches.stream().map(IgniteCache::getName).collect(Collectors.toSet());

        for (long i = startKey; i < endKey; i++) {
            i += 1;
            if (i < endKey) {
                for (IgniteCache<Object, Object> cache : caches)
                    cache.put(i, i);
            }
        }

        List<Integer> startSizes = new ArrayList<>();

        for (IgniteCache<Object, Object> cache : caches)
            startSizes.add(cache.size());

        List<IgniteEx> grids = new ArrayList<>();

        for (int i = 0; i < nodesCnt; i++)
            grids.add(grid(i));

        breakCacheSizes(grids, cacheNames);

        for (int i = 0; i < caches.size(); i++)
            assertFalse(caches.get(i).size() == startSizes.get(i));

        VisorPartitionReconciliationTaskArg.Builder builder = new VisorPartitionReconciliationTaskArg.Builder();
        builder.repair(true);
        builder.parallelism(reconParallelism);
        builder.caches(cacheNames);
        builder.batchSize(reconBatchSize);
        builder.reconTypes(new HashSet(reconciliationTypes));

        reconResult = new AtomicReference<>();

        List<IgniteInternalFuture> loadFuts = new ArrayList<>();

        for (int i = 0; i < loadThreadsCnt; i++) {
            caches.forEach(cache -> {
                IgniteInternalFuture loadFut = startAsyncLoad(reconResult, client, cache, startKey, endKey, cacheClearOp);

                loadFuts.add(loadFut);
            });
        }

        GridTestUtils.runMultiThreadedAsync(() -> {
            reconResult.set(partitionReconciliation(client, builder));
        }, 1, "reconciliation");

        GridTestUtils.waitForCondition(() -> reconResult.get() != null, 120_000);

        List<String> errors = reconResult.get().errors();

        assertTrue(errors.isEmpty());

        for (IgniteInternalFuture fut : loadFuts)
            fut.get();

        awaitPartitionMapExchange();

        for (long i = startKey; i < endKey; i++) {
            for (IgniteCache<Object, Object> cache : caches)
                cache.put(i, i);
        }

        awaitPartitionMapExchange();

        long allKeysCountForCacheGroup;
        long allKeysCountForCache;

        for (String cacheName : cacheNames) {
            allKeysCountForCacheGroup = 0;
            allKeysCountForCache = 0;

            for (int i = 0; i < nodesCnt; i++) {
                long i0 = getFullPartitionsSizeForCacheGroup(grid(i), cacheName);
                allKeysCountForCacheGroup += i0;

                long i1 = getPartitionsSizeForCache(grid(i), cacheName);
                allKeysCountForCache += i1;
            }

            assertEquals(endKey, client.cache(cacheName).size());

            if (cacheMode == REPLICATED) {
                assertEquals((long)endKey * nodesCnt * (cacheGrp == null ? 1 : caches.size()), allKeysCountForCacheGroup);
                assertEquals((long)endKey * nodesCnt, allKeysCountForCache);
            }
            else {
                assertEquals((long)endKey * (1 + backupCnt) * (cacheGrp == null ? 1 : caches.size()), allKeysCountForCacheGroup);
                assertEquals((long)endKey * (1 + backupCnt), allKeysCountForCache);
            }
        }
    }
}
