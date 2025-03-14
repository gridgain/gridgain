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
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.AtomicConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.checker.objects.ReconciliationResult;
import org.apache.ignite.internal.processors.cache.verify.RepairAlgorithm;
import org.apache.ignite.internal.processors.datastructures.GridCacheInternalKeyImpl;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Tests the utility under loading.
 */
@RunWith(Parameterized.class)
public class PartitionReconciliationAtomicLongStressTest extends PartitionReconciliationAbstractTest {
    /** Nodes. */
    protected static final int NODES_CNT = 4;

    /** Keys count. */
    protected static final int KEYS_CNT = 2000;

    /** Corrupted keys count. */
    protected static final int BROKEN_KEYS_CNT = 500;

    /** Internal data structure cache name. */
    protected static final String INTERNAL_CACHE_NAME = "ignite-sys-atomic-cache@default-ds-group";

    /** Patten to search integer key for data structure key string representation. */
    protected static final Pattern intKeyPattern = Pattern.compile(".+name=(\\d+).+");

    /** Parts. */
    @Parameterized.Parameter(0)
    public int parts;

    /** Fix mode. */
    @Parameterized.Parameter(1)
    public boolean fixMode;

    /** Repair algorithm. */
    @Parameterized.Parameter(2)
    public RepairAlgorithm repairAlgorithm;

    /** Parallelism. */
    @Parameterized.Parameter(3)
    public int parallelism;

    /** Crd server node. */
    protected IgniteEx ig;

    /** Client. */
    protected IgniteEx client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(true)
                .setMaxSize(300L * 1024 * 1024))
        );

        cfg.setConsistentId(name);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        ig = startGrids(NODES_CNT);

        client = startClientGrid(NODES_CNT);

        ig.cluster().active(true);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * Makes different variations of input params.
     */
    @Parameterized.Parameters(
        name = "partitions = {0}, fixModeEnabled = {1}, repairAlgorithm = {2}, parallelism = {3}")
    public static List<Object[]> parameters() {
        ArrayList<Object[]> params = new ArrayList<>();

        int[] partitions = {1, 32};

        for (int parts : partitions)
            params.add(new Object[] {parts, false, null, 4});

        params.add(new Object[] {1, false, null, 1});
        params.add(new Object[] {32, false, null, 1});

        return params;
    }

    /**
     * Stress test for reconciliation under load
     *
     * @throws Exception If failed.
     */
    @Test
    public void testReconciliationOfColdKeysUnderLoad() throws Exception {
        AtomicConfiguration cfg = new AtomicConfiguration().setBackups(NODES_CNT - 1)
            .setAffinity(new RendezvousAffinityFunction(false, parts));

        Set<Integer> correctKeys = new HashSet<>();

        for (int i = 0; i < KEYS_CNT; i++) {
            client.atomicLong(Integer.toString(i), cfg, i, true);
            correctKeys.add(i);
        }

        log.info(">>>> Initial data loading finished");

        int firstBrokenKey = KEYS_CNT / 2;

        GridCacheContext[] nodeCacheCtxs = new GridCacheContext[NODES_CNT];

        for (int i = 0; i < NODES_CNT; i++)
            nodeCacheCtxs[i] = grid(i).cachex(INTERNAL_CACHE_NAME).context();

        Set<Integer> corruptedColdKeys = new HashSet<>();
        Set<Integer> corruptedHotKeys = new HashSet<>();

        for (int i = firstBrokenKey; i < firstBrokenKey + BROKEN_KEYS_CNT; i++) {
            if (isHotKey(i))
                corruptedHotKeys.add(i);
            else
                corruptedColdKeys.add(i);

            correctKeys.remove(i);

            GridCacheInternalKeyImpl key = new GridCacheInternalKeyImpl(Integer.toString(i), "default-ds-group");

            if (i % 3 == 0)
                simulateMissingEntryCorruption(nodeCacheCtxs[i % NODES_CNT], key);
            else
                simulateOutdatedVersionCorruption(nodeCacheCtxs[i % NODES_CNT], key, true);
        }

        log.info(">>>> Simulating data corruption finished");

        AtomicBoolean stopRandomLoad = new AtomicBoolean(false);

        IgniteInternalFuture<Long> randLoadFut = GridTestUtils.runMultiThreadedAsync(() -> {
            while (!stopRandomLoad.get()) {
                int i = (KEYS_CNT / 2) + ThreadLocalRandom.current().nextInt(BROKEN_KEYS_CNT);

                if (isHotKey(i)) {
                    // The following statement won't work: atomicLong.incrementAndGet().
                    // This happens because of internal null value verification that fails
                    // if simulateMissingEntryCorruption have been invoked.
                    // Therefore let's simulate new key creation.
                    client.atomicLong(Integer.toString(i), cfg, i * 2, true);
                }
            }
        }, 4, "rand-loader");

        ReconciliationResult res = partitionReconciliation(ig, fixMode, repairAlgorithm, parallelism, INTERNAL_CACHE_NAME);

        log.info(">>>> Partition reconciliation finished");

        stopRandomLoad.set(true);

        randLoadFut.get();

        assertResultContainsConflictKeys(res, INTERNAL_CACHE_NAME, this::keyMap, corruptedColdKeys);

        Set<Integer> conflictKeys = conflictKeys(res, INTERNAL_CACHE_NAME, this::keyMap);
        for (Integer correctKey : correctKeys)
            assertFalse("Correct key detected as broken: " + correctKey, conflictKeys.contains(correctKey));
    }

    /** */
    protected Integer keyMap(String keyStr) {
        Matcher m = intKeyPattern.matcher(keyStr);
        if (m.matches())
            return Integer.parseInt(m.group(1));
        return -1;
    }

    /** */
    protected static boolean isHotKey(int key) {
        return key % 13 == 5 || key % 13 == 7 || key % 13 == 11;
    }
}
