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
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.AtomicConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.checker.objects.ReconciliationResult;
import org.apache.ignite.internal.processors.cache.verify.RepairAlgorithm;
import org.apache.ignite.internal.processors.datastructures.GridCacheInternalKeyImpl;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;
import org.junit.runners.Parameterized;

import static org.apache.ignite.internal.processors.cache.verify.RepairAlgorithm.LATEST;
import static org.apache.ignite.internal.processors.cache.verify.RepairAlgorithm.MAJORITY;
import static org.apache.ignite.internal.processors.cache.verify.RepairAlgorithm.PRIMARY;
import static org.apache.ignite.internal.processors.cache.verify.RepairAlgorithm.REMOVE;

/**
 * Tests the utility under loading.
 */
public class PartitionReconciliationAtomicLongFixStressTest extends PartitionReconciliationAtomicLongStressTest {
    /**
     * Makes different variations of input params.
     */
    @Parameterized.Parameters(
        name = "partitions = {0}, fixModeEnabled = {1}, repairAlgorithm = {2}, parallelism = {3}")
    public static List<Object[]> parameters() {
        ArrayList<Object[]> params = new ArrayList<>();

        int[] partitions = {1, 32};
        RepairAlgorithm[] repairAlgorithms = {LATEST, PRIMARY, MAJORITY, REMOVE};

        for (int parts : partitions)
            for (RepairAlgorithm repairAlgorithm : repairAlgorithms)
                params.add(new Object[] {parts, true, repairAlgorithm, 4});

        params.add(new Object[] {1, true, MAJORITY, 1});
        params.add(new Object[] {32, true, REMOVE, 1});

        return params;
    }

    /**
     * Test #38 Maximum stress test with -fix and every key is corrupted
     *
     * @throws Exception If failed.
     */
    @Test
    @Override public void testReconciliationOfColdKeysUnderLoad() throws Exception {
        AtomicConfiguration cfg = new AtomicConfiguration().setBackups(NODES_CNT - 1)
            .setAffinity(new RendezvousAffinityFunction(false, parts));

        GridCacheContext[] nodeCacheCtxs = new GridCacheContext[NODES_CNT];

        for (int i = 0; i < KEYS_CNT; i++)
            client.atomicLong(Integer.toString(i), cfg, i, true);

        for (int i = 0; i < NODES_CNT; i++)
            nodeCacheCtxs[i] = grid(i).cachex(INTERNAL_CACHE_NAME).context();

        Set<Integer> corruptedKeys = new HashSet<>();

        for (int i = 0; i < KEYS_CNT; i++) {
            corruptedKeys.add(i);
            GridCacheInternalKeyImpl key = new GridCacheInternalKeyImpl(Integer.toString(i), "default-ds-group");

//            if (i % 3 == 0)
//                simulateMissingEntryCorruption(nodeCacheCtxs[i % NODES_CNT], key);
//            else
                simulateOutdatedVersionCorruption(nodeCacheCtxs[i % NODES_CNT], key);
        }

        AtomicBoolean stopRandomLoad = new AtomicBoolean(false);

        final Set<Integer>[] reloadedKeys = new Set[6];

        AtomicInteger threadCntr = new AtomicInteger(0);



        ReconciliationResult res = partitionReconciliation(ig, fixMode, repairAlgorithm, parallelism, INTERNAL_CACHE_NAME);

        log.info(">>>> Partition reconciliation finished");

        stopRandomLoad.set(true);




        assertResultContainsConflictKeys(res, INTERNAL_CACHE_NAME, this::keyMap, corruptedKeys);

        ReconciliationResult result = partitionReconciliation(ig, false, repairAlgorithm, parallelism, INTERNAL_CACHE_NAME);

        assertFalse(idleVerify(ig, INTERNAL_CACHE_NAME).hasConflicts());
    }
}
