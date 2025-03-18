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
import java.util.regex.Matcher;
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
import static org.apache.ignite.internal.processors.cache.verify.RepairAlgorithm.LATEST_SKIP_MISSING_PRIMARY;
import static org.apache.ignite.internal.processors.cache.verify.RepairAlgorithm.LATEST_TRUST_MISSING_PRIMARY;
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
        RepairAlgorithm[] repairAlgorithms =
            {LATEST, PRIMARY, MAJORITY, REMOVE, LATEST_SKIP_MISSING_PRIMARY, LATEST_TRUST_MISSING_PRIMARY};

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
        Set<Integer> missedKeysOnPrimary = new HashSet<>();

        for (int i = 0; i < KEYS_CNT; i++) {
            corruptedKeys.add(i);
            GridCacheInternalKeyImpl key = new GridCacheInternalKeyImpl(Integer.toString(i), "default-ds-group");

            GridCacheContext<Object, Object> ctx = nodeCacheCtxs[i % NODES_CNT];

            if (i % 3 == 0) {
                simulateMissingEntryCorruption(ctx, key);

                if (ctx.cache().cache().affinity().isPrimary(ctx.kernalContext().discovery().localNode(), key)) {
                    missedKeysOnPrimary.add(i);
                }
            }
            else
                simulateOutdatedVersionCorruption(ctx, key, true);
        }

        AtomicBoolean stopRandomLoad = new AtomicBoolean(false);

        final Set<Integer>[] reloadedKeys = new Set[6];

        AtomicInteger threadCntr = new AtomicInteger(0);

        IgniteInternalFuture<Long> randLoadFut = GridTestUtils.runMultiThreadedAsync(() -> {
            int threadId = threadCntr.incrementAndGet() - 1;
            reloadedKeys[threadId] = new HashSet<>();

            while (!stopRandomLoad.get()) {
                int i = ThreadLocalRandom.current().nextInt(KEYS_CNT);
                // The following statement won't work: atomicLong.incrementAndGet().
                // This happens because of internal null value verification that fails
                // if simulateMissingEntryCorruption have been invoked.
                // Therefore let's simulate new key creation.
                client.atomicLong(Integer.toString(i), cfg, i * 2, true);
                reloadedKeys[threadId].add(i);
            }
        }, 6, "rand-loader");

        ReconciliationResult res = partitionReconciliation(ig, fixMode, repairAlgorithm, parallelism, INTERNAL_CACHE_NAME);

        log.info(">>>> Partition reconciliation finished");

        stopRandomLoad.set(true);

        randLoadFut.get();

        for (Set<Integer> reloadedKey : reloadedKeys)
            corruptedKeys.removeAll(reloadedKey);

        assertResultContainsConflictKeys(res, INTERNAL_CACHE_NAME, this::keyMap, corruptedKeys);

        if (repairAlgorithm == LATEST_SKIP_MISSING_PRIMARY) {
            // In case when the value of the key is missing on the primary node,
            // this algorithm should not fix the conflict.

            Set<Integer> notFixedConflicts = notFixedKeys(res, INTERNAL_CACHE_NAME, view -> {
                Matcher matcher = intKeyPattern.matcher(view);

                if (matcher.matches())
                    return Integer.valueOf(matcher.group(1));
                else
                    throw new IllegalArgumentException("Unexpected key format [view=" + view + ']');
            });

            assertEquals(missedKeysOnPrimary, notFixedConflicts);

            if (notFixedConflicts.isEmpty())
                assertFalse(idleVerify(ig, INTERNAL_CACHE_NAME).hasConflicts());
            else
                assertTrue(idleVerify(ig, INTERNAL_CACHE_NAME).hasConflicts());
        }
        else
            assertFalse(idleVerify(ig, INTERNAL_CACHE_NAME).hasConflicts());
    }
}
