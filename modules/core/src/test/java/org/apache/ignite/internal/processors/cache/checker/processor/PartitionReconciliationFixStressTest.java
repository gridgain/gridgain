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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.internal.processors.cache.verify.RepairAlgorithm;
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
public class PartitionReconciliationFixStressTest extends PartitionReconciliationStressTest {
    /**
     * Makes different variations of input params.
     */
    @Parameterized.Parameters(
        name = "atomicity = {0}, partitions = {1}, fixModeEnabled = {2}, repairAlgorithm = {3}, parallelism = {4}, batchSize = {5}")
    public static List<Object[]> parameters() {
        ArrayList<Object[]> params = new ArrayList<>();

        CacheAtomicityMode[] atomicityModes = new CacheAtomicityMode[] {
            CacheAtomicityMode.ATOMIC, CacheAtomicityMode.TRANSACTIONAL};

        int[] partitions = {1, 32};
        RepairAlgorithm[] repairAlgorithms =
            {LATEST, PRIMARY, MAJORITY, REMOVE, LATEST_SKIP_MISSING_PRIMARY, LATEST_TRUST_MISSING_PRIMARY};

        for (CacheAtomicityMode atomicityMode : atomicityModes) {
            for (int parts : partitions)
                for (RepairAlgorithm repairAlgorithm : repairAlgorithms)
                    params.add(new Object[] {atomicityMode, parts, true, repairAlgorithm, 4, 1000});

            params.add(new Object[] {atomicityMode, partitions[1], true, repairAlgorithms[0], 4, 10});
        }

        params.add(new Object[] {CacheAtomicityMode.ATOMIC, 1, true, LATEST, 1, 1000});
        params.add(new Object[] {CacheAtomicityMode.TRANSACTIONAL, 32, true, PRIMARY, 1, 1000});

        return params;
    }

    /**
     * Test #36 Stress test for reconciliation with -fix under load
     *
     * @throws Exception If failed.
     */
     @Test
     @Override public void testReconciliationOfColdKeysUnderLoad() throws Exception {
         ReconciliationUnderLoadResult res = reconciliationOfColdKeysUnderLoad();

        if (repairAlgorithm == LATEST_SKIP_MISSING_PRIMARY) {
            // In case when the value of the key is missing on the primary node,
            // this algorithm should not fix the conflict.

            Set<Integer> notFixedConflicts = notFixedKeys(res.reconciliationResult, DEFAULT_CACHE_NAME);

            assertTrue(res.missedOnPrimaryKeys.containsAll(notFixedConflicts));

            if (!notFixedConflicts.isEmpty())
                assertTrue(idleVerify(ig, DEFAULT_CACHE_NAME).hasConflicts());
            else
                assertFalse(idleVerify(ig, DEFAULT_CACHE_NAME).hasConflicts());
        }
        else
            assertFalse(idleVerify(ig, DEFAULT_CACHE_NAME).hasConflicts());
    }
}
