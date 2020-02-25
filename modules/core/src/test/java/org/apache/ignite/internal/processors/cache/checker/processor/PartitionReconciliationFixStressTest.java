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
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.internal.processors.cache.verify.RepairAlgorithm;
import org.junit.Test;
import org.junit.runners.Parameterized;

import static org.apache.ignite.internal.processors.cache.verify.RepairAlgorithm.MAJORITY;
import static org.apache.ignite.internal.processors.cache.verify.RepairAlgorithm.LATEST;
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
        name = "atomicity = {0}, partitions = {1}, fixModeEnabled = {2}, repairAlgorithm = {3}, parallelism = {4}")
    public static List<Object[]> parameters() {
        ArrayList<Object[]> params = new ArrayList<>();

        CacheAtomicityMode[] atomicityModes = new CacheAtomicityMode[] {
            CacheAtomicityMode.ATOMIC, CacheAtomicityMode.TRANSACTIONAL};

        int[] partitions = {1, 32};
        RepairAlgorithm[] repairAlgorithms = {LATEST, PRIMARY, MAJORITY, REMOVE};

        for (CacheAtomicityMode atomicityMode : atomicityModes) {
            for (int parts : partitions)
                for (RepairAlgorithm repairAlgorithm : repairAlgorithms)
                    params.add(new Object[] {atomicityMode, parts, true, repairAlgorithm, 4});
        }

        params.add(new Object[] {CacheAtomicityMode.ATOMIC, 1, true, LATEST, 1});
        params.add(new Object[] {CacheAtomicityMode.TRANSACTIONAL, 32, true, PRIMARY, 1});

        return params;
    }

    /**
     * Test #36 Stress test for reconciliation with -fix under load
     *
     * @throws Exception If failed.
     */
     @Test
     @Override public void testReconciliationOfColdKeysUnderLoad() throws Exception {
        super.testReconciliationOfColdKeysUnderLoad();

        assertFalse(idleVerify(ig, DEFAULT_CACHE_NAME).hasConflicts());
    }
}
