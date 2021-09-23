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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.checker.objects.ReconciliationResult;
import org.apache.ignite.internal.processors.cache.verify.ReconciliationType;
import org.apache.ignite.internal.visor.checker.VisorPartitionReconciliationTaskArg;
import org.apache.ignite.testframework.GridTestUtils;

import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 * Tests partition reconciliation of sizes with various cache configurations.
 */
public class PartitionReconciliationFixPartitionSizesStressAbstractTest extends PartitionReconciliationAbstractTest {
    /** */
    protected int pageSize;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        DataStorageConfiguration storageConfiguration = new DataStorageConfiguration();
        storageConfiguration.setPageSize(pageSize);

        cfg.setDataStorageConfiguration(storageConfiguration);

        cfg.setConsistentId(name);

        return cfg;
    }

    /** */
    protected CacheConfiguration getCacheConfig(
        String name,
        CacheAtomicityMode atomicityMode,
        CacheMode cacheMode,
        CacheWriteSynchronizationMode syncMode,
        int backupCount,
        int partCount,
        String cacheGroup
    ) {
        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setName(name);

        if (cacheGroup != null)
            ccfg.setGroupName(cacheGroup);

        ccfg.setWriteSynchronizationMode(syncMode);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, partCount));
        ccfg.setBackups(backupCount);
        ccfg.setAtomicityMode(atomicityMode);
        ccfg.setCacheMode(cacheMode);

        return ccfg;
    }

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
    protected void test(
        int nodesCnt,
        int startKey,
        int endKey,
        CacheAtomicityMode atomicityMode,
        CacheMode cacheMode,
        CacheWriteSynchronizationMode syncMode,
        int backupCnt,
        int partCnt,
        String cacheGrp,
        int reconBatchSize,
        int reconParallelism,
        int loadThreadsCnt,
        Set<ReconciliationType> reconciliationTypes,
        boolean cacheClearOp,
        int cacheCount
    ) throws Exception {
        log.info("Parameters: pageSize=" + pageSize +
            ", nodesCnt=" + nodesCnt +
            ", startKey=" + startKey +
            ", endKey=" + endKey +
            ", atomicityMode=" + atomicityMode +
            ", cacheMode=" + cacheMode +
            ", syncMode=" + syncMode +
            ", backupCnt=" + backupCnt +
            ", partCnt=" + partCnt +
            ", cacheGrp=" + cacheGrp +
            ", reconBatchSize=" + reconBatchSize +
            ", reconParallelism=" + reconParallelism +
            ", loadThreadsCnt=" + loadThreadsCnt +
            ", reconciliationTypes=" + reconciliationTypes +
            ", cacheClearOp=" + cacheClearOp +
            ", cacheCount=" + cacheCount
        );

        IgniteEx ig = startGrids(nodesCnt);

        IgniteEx client = startClientGrid(nodesCnt);

        ig.cluster().active(true);

        List<IgniteCache<Object, Object>> caches = new ArrayList<>();
        List<IgniteCache<Object, Object>> reconCaches = new ArrayList<>();

        for (int i = 0; i < cacheCount; i++) {
            caches.add(client.createCache(
                getCacheConfig(DEFAULT_CACHE_NAME + i, atomicityMode, cacheMode, syncMode, backupCnt, partCnt, cacheGrp)
            ));
        }

        Set<String> cacheNames = caches.stream().map(IgniteCache::getName).collect(Collectors.toSet());

        Set<String> reconCachesNames = new HashSet<>();

        for (int i = 0; i < cacheCount; i++) {
            reconCaches.add(caches.get(i));
            reconCachesNames.add(caches.get(i).getName());

            if (cacheCount > 1 && cacheGrp != null)
                break;
        }

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

        breakCacheSizes(grids, reconCachesNames);

        if (cacheCount > 1 && cacheGrp != null) {
            assertFalse(caches.get(0).size() == startSizes.get(0));

            for (int i = 1; i < cacheCount; i++) {
                assertTrue(caches.get(i).size() == startSizes.get(i));
            }
        }
        else {
            for (int i = 0; i < caches.size(); i++)
                assertFalse(caches.get(i).size() == startSizes.get(i));
        }

        VisorPartitionReconciliationTaskArg.Builder builder = new VisorPartitionReconciliationTaskArg.Builder();
        builder.repair(true);
        builder.parallelism(reconParallelism);
        builder.caches(reconCachesNames);
        builder.batchSize(reconBatchSize);
        builder.reconTypes(new HashSet(reconciliationTypes));

        AtomicReference<ReconciliationResult> reconResult = new AtomicReference<>();

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

        if (!GridTestUtils.waitForCondition(() -> reconResult.get() != null, 120_000))
            throw new RuntimeException("Condition was not achieved");

        List<String> errors = reconResult.get().errors();

        assertTrue(errors.isEmpty());

        for (IgniteInternalFuture fut : loadFuts)
            fut.get();

        awaitPartitionMapExchange();
        doSleep(10000);
        cacheNames.forEach(cacheName -> assertPartitionsSame(idleVerify(grid(0), cacheName)));

        for (long i = startKey; i < endKey; i++) {
            for (IgniteCache<Object, Object> cache : caches)
                cache.put(i, i);
        }

        awaitPartitionMapExchange();
        doSleep(5000);
        cacheNames.forEach(cacheName -> assertPartitionsSame(idleVerify(grid(0), cacheName)));

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

    /**
     * <ul>
     * <li>Start nodes.</li>
     * <li>Create sql cache.</li>
     * <li>Load some data.</li>
     * <li>Break cache size.</li>
     * <li>Start load thread.</li>
     * <li>Do size reconciliation.</li>
     * <li>Stop load thread.</li>
     * <li>Check size of primary/backup partitions in cluster.</li>
     * </ul>
     */
    public void sqlTest(
        int nodesCnt,
        int startKey,
        int endKey,
        CacheAtomicityMode atomicityMode,
        CacheMode cacheMode,
        CacheWriteSynchronizationMode syncMode,
        int backupCnt,
        int partCnt,
        String cacheGrp,
        int reconBatchSize,
        int reconParallelism,
        int loadThreadsCnt,
        Set<ReconciliationType> reconciliationTypes,
        boolean cacheClearOp,
        int cacheCount
    ) throws Exception {
        IgniteEx ig = startGrids(nodesCnt);

        IgniteEx client = startClientGrid(nodesCnt);

        ig.cluster().active(true);

        String cacheName = DEFAULT_CACHE_NAME + 0;

        IgniteCache<Object, Object> cache = client.createCache(
            getCacheConfig(cacheName, atomicityMode, cacheMode, syncMode, backupCnt, partCnt, cacheGrp)
        );

        String tblName = "testtable";
        String sqlCacheName = "SQL_PUBLIC_TESTTABLE";

        cache.query(
            new SqlFieldsQuery("create table " + tblName + " (id integer primary key, p integer) with \"backups=" +
                backupCnt + ", template=" + cacheMode.name() + ", atomicity=" + atomicityMode + "\"")
        ).getAll();

        List<Long> keysInTable = new ArrayList<>(endKey);

        for (long i = startKey; i < endKey; i++) {
            i += 1;

            if (i < endKey) {
                if (!keysInTable.contains(i)) {
                    cache.query(new SqlFieldsQuery("insert into " + tblName + "(id, p) values (" + i + ", " + i + ")")).getAll();

                    keysInTable.add(i);
                }
            }
        }

        IgniteCache<Object, Object> sqlCache = client.cache(sqlCacheName);

        int sqlStartSize = sqlCache.size();

        List<IgniteEx> grids = new ArrayList<>();

        for (int i = 0; i < nodesCnt; i++)
            grids.add(grid(i));

        breakCacheSizes(grids, new HashSet<>(Collections.singletonList(sqlCacheName)));

        assertFalse(sqlCache.size() == sqlStartSize);

        VisorPartitionReconciliationTaskArg.Builder builder = new VisorPartitionReconciliationTaskArg.Builder();
        builder.repair(true);
        builder.parallelism(reconParallelism);
        builder.caches(new HashSet<>(Arrays.asList(sqlCacheName)));
        builder.batchSize(reconBatchSize);
        builder.reconTypes(new HashSet(reconciliationTypes));

        AtomicReference<ReconciliationResult> reconResult = new AtomicReference<>();

        IgniteInternalFuture loadFut = startAsyncSqlLoad(reconResult, client, sqlCache, tblName, keysInTable, startKey, endKey);

        GridTestUtils.runMultiThreadedAsync(() -> {
            reconResult.set(partitionReconciliation(client, builder));
        }, 1, "reconciliation");

        GridTestUtils.waitForCondition(() -> reconResult.get() != null, 120_000);

        List<String> errors = reconResult.get().errors();

        assertTrue(errors.isEmpty());

        loadFut.get();

        for (long i = startKey; i < endKey; i++) {
            if (!keysInTable.contains(i))
                cache.query(new SqlFieldsQuery("insert into " + tblName + "(id, p) values (" + i + ", " + i + ")")).getAll();
        }

        doSleep(3000);

        long allKeysCountForCacheGroup;
        long allKeysCountForCache;

        allKeysCountForCacheGroup = 0;
        allKeysCountForCache = 0;

        for (int i = 0; i < nodesCnt; i++) {
            long i0 = getFullPartitionsSizeForCacheGroup(grid(i), sqlCacheName);
            allKeysCountForCacheGroup += i0;

            long i1 = getPartitionsSizeForCache(grid(i), sqlCacheName);
            allKeysCountForCache += i1;
        }

        assertEquals(endKey, client.cache(sqlCacheName).size());

        if (cacheMode == REPLICATED) {
            assertEquals((long)endKey * nodesCnt, allKeysCountForCacheGroup);
            assertEquals((long)endKey * nodesCnt, allKeysCountForCache);
        }
        else {
            assertEquals((long)endKey * (1 + backupCnt), allKeysCountForCacheGroup);
            assertEquals((long)endKey * (1 + backupCnt), allKeysCountForCache);
        }
    }
}
