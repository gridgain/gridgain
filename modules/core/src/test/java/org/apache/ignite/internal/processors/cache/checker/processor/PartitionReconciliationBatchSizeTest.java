/*
 * Copyright 2024 GridGain Systems, Inc. and Contributors.
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

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.checker.objects.ReconciliationResult;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.processors.cache.checker.util.ConsistencyCheckUtils.RECONCILIATION_DIR;
import static org.apache.ignite.internal.processors.cache.verify.RepairAlgorithm.LATEST;

/**
 * Tests reconciliation with intersecting and non-intersecting values when the batch size is smaller than the gap between partition owners.
 */
public class PartitionReconciliationBatchSizeTest extends PartitionReconciliationAbstractTest {
    /** Nodes. */
    private static final int NODES_CNT = 3;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(false)
                .setMaxSize(300L * 1024 * 1024))
        );

        cfg.setConsistentId(igniteInstanceName);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        stopAllGrids();

        cleanPersistenceDir();

        startGrids(NODES_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<>();

        ccfg.setName(DEFAULT_CACHE_NAME);
        ccfg.setAtomicityMode(ATOMIC);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, 1));
        ccfg.setBackups(NODES_CNT - 1);

        grid(0).getOrCreateCache(ccfg);

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        grid(0).destroyCache(DEFAULT_CACHE_NAME);

        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), RECONCILIATION_DIR, false));
    }

    /**
     * Tests detecting and fixing conflicts when partitions have non-intersected values.
     *
     * owner0 - [0, ..., 33], owner1 - [34, ..., 66] and owner3 - [67, ..., 99]. batch size - 10.
     * expected number of conflicts - 100.
     */
    @Test
    public void testNonIntersectingValues() {
        IgniteCache<Integer, Integer> cache0 = grid(0).cache(DEFAULT_CACHE_NAME);
        IgniteCache<Integer, Integer> cache1 = grid(1).cache(DEFAULT_CACHE_NAME);
        IgniteCache<Integer, Integer> cache2 = grid(2).cache(DEFAULT_CACHE_NAME);

        int numberOfKeys = 100;
        List<Integer> singlePartKeys = partitionKeys(cache0, 0, numberOfKeys, 0);

        Set<Integer> keysToClear0 = new LinkedHashSet<>();
        Set<Integer> keysToClear1 = new LinkedHashSet<>();
        Set<Integer> keysToClear2 = new LinkedHashSet<>();

        for (int i = 0; i < singlePartKeys.size(); i++) {
            Integer key = singlePartKeys.get(i);

            cache0.put(key, key);

            if (i < numberOfKeys / 3)
                keysToClear0.add(key);
            else if (i < 2 * numberOfKeys / 3)
                keysToClear1.add(key);
            else
                keysToClear2.add(key);
        }

        cache0.localClearAll(keysToClear0);
        cache1.localClearAll(keysToClear1);
        cache2.localClearAll(keysToClear2);

        detectAndFixConflicts(new HashSet<>(singlePartKeys));
    }

    /**
     * Tests detecting and fixing conflicts when partitions have intersected values.
     *
     * owner0 - [0, ..., 99], owner1 - [34, ..., 66] and owner3 - [67, ..., 99]. batch size - 10.
     * expected number of conflicts - 100.
     */
    @Test
    public void testIntersectingValues() {
        IgniteCache<Integer, Integer> cache0 = grid(0).cache(DEFAULT_CACHE_NAME);
        IgniteCache<Integer, Integer> cache1 = grid(1).cache(DEFAULT_CACHE_NAME);
        IgniteCache<Integer, Integer> cache2 = grid(2).cache(DEFAULT_CACHE_NAME);

        int numberOfKeys = 100;
        List<Integer> singlePartKeys = partitionKeys(cache0, 0, numberOfKeys, 0);

        Set<Integer> keysToClear1 = new LinkedHashSet<>();
        Set<Integer> keysToClear2 = new LinkedHashSet<>();

        for (int i = 0; i < singlePartKeys.size(); i++) {
            Integer key = singlePartKeys.get(i);

            cache0.put(key, key);

            if (i < 2 * numberOfKeys / 3)
                keysToClear1.add(key);
            else
                keysToClear2.add(key);
        }

        cache1.localClearAll(keysToClear1);
        cache2.localClearAll(keysToClear2);

        detectAndFixConflicts(new HashSet<>(singlePartKeys));
    }

    private void detectAndFixConflicts(Set<Integer> conflictedKeys) {
        // Detect conflicts.
        ReconciliationResult res = partitionReconciliation(grid(0), false, null, 4, 10, DEFAULT_CACHE_NAME);

        assertEquals(conflictedKeys.size(), conflictKeys(res, DEFAULT_CACHE_NAME).size());
        assertResultContainsConflictKeys(res, DEFAULT_CACHE_NAME, conflictedKeys);

        // Detect and fix conflicts.
        partitionReconciliation(grid(0), true, LATEST, 4, 10, DEFAULT_CACHE_NAME);

        assertFalse(idleVerify(grid(0), DEFAULT_CACHE_NAME).hasConflicts());
    }
}
