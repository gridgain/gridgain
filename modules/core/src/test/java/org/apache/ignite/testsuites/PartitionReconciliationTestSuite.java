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

package org.apache.ignite.testsuites;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.internal.processors.cache.checker.ConsistencyCheckUtilsTest;
import org.apache.ignite.internal.processors.cache.checker.processor.PartitionReconciliationAtomicLongDataStructureTest;
import org.apache.ignite.internal.processors.cache.checker.processor.PartitionReconciliationAtomicLongFixStressTest;
import org.apache.ignite.internal.processors.cache.checker.processor.PartitionReconciliationAtomicLongStressTest;
import org.apache.ignite.internal.processors.cache.checker.processor.PartitionReconciliationBinaryObjectsTest;
import org.apache.ignite.internal.processors.cache.checker.processor.PartitionReconciliationCompactCollectorTest;
import org.apache.ignite.internal.processors.cache.checker.processor.PartitionReconciliationFastCheckTest;
import org.apache.ignite.internal.processors.cache.checker.processor.PartitionReconciliationFixPartitionSizesPersistenceStressParameterizedTest;
import org.apache.ignite.internal.processors.cache.checker.processor.PartitionReconciliationFixPartitionSizesStressParameterizedTest;
import org.apache.ignite.internal.processors.cache.checker.processor.PartitionReconciliationFixPartitionSizesStressParameterizedTest1;
import org.apache.ignite.internal.processors.cache.checker.processor.PartitionReconciliationFixPartitionSizesStressParameterizedTest2;
import org.apache.ignite.internal.processors.cache.checker.processor.PartitionReconciliationFixPartitionSizesStressParameterizedTest3;
import org.apache.ignite.internal.processors.cache.checker.processor.PartitionReconciliationFixPartitionSizesStressParameterizedTest4;
import org.apache.ignite.internal.processors.cache.checker.processor.PartitionReconciliationFixPartitionSizesStressParameterizedTest5;
import org.apache.ignite.internal.processors.cache.checker.processor.PartitionReconciliationFixPartitionSizesStressParameterizedTest6;
import org.apache.ignite.internal.processors.cache.checker.processor.PartitionReconciliationFixPartitionSizesStressParameterizedTest7;
import org.apache.ignite.internal.processors.cache.checker.processor.PartitionReconciliationFixPartitionSizesStressRandomizedTest;
import org.apache.ignite.internal.processors.cache.checker.processor.PartitionReconciliationFixPartitionSizesStressRandomizedTest1;
import org.apache.ignite.internal.processors.cache.checker.processor.PartitionReconciliationFixPartitionSizesStressRandomizedTest2;
import org.apache.ignite.internal.processors.cache.checker.processor.PartitionReconciliationFixPartitionSizesStressRandomizedTest3;
import org.apache.ignite.internal.processors.cache.checker.processor.PartitionReconciliationFixPartitionSizesStressRandomizedTest4;
import org.apache.ignite.internal.processors.cache.checker.processor.PartitionReconciliationFixPartitionSizesStressRandomizedTest5;
import org.apache.ignite.internal.processors.cache.checker.processor.PartitionReconciliationFixPartitionSizesStressRandomizedTest6;
import org.apache.ignite.internal.processors.cache.checker.processor.PartitionReconciliationFixPartitionSizesStressRandomizedTest7;
import org.apache.ignite.internal.processors.cache.checker.processor.PartitionReconciliationFixPartitionSizesTest;
import org.apache.ignite.internal.processors.cache.checker.processor.PartitionReconciliationFixStressTest;
import org.apache.ignite.internal.processors.cache.checker.processor.PartitionReconciliationFullFixStressTest;
import org.apache.ignite.internal.processors.cache.checker.processor.PartitionReconciliationInterruptionRecheckTest;
import org.apache.ignite.internal.processors.cache.checker.processor.PartitionReconciliationInterruptionRepairTest;
import org.apache.ignite.internal.processors.cache.checker.processor.PartitionReconciliationProcessorTest;
import org.apache.ignite.internal.processors.cache.checker.processor.PartitionReconciliationRecheckAttemptsTest;
import org.apache.ignite.internal.processors.cache.checker.processor.PartitionReconciliationSetDataStructureTest;
import org.apache.ignite.internal.processors.cache.checker.processor.PartitionReconciliationStressTest;
import org.apache.ignite.internal.processors.cache.checker.processor.PartitionReconciliationSystemFastCheckTest;
import org.apache.ignite.internal.processors.cache.checker.processor.ReconciliationResourceLimitedJobTest;
import org.apache.ignite.internal.processors.cache.checker.tasks.CollectPartitionKeysByBatchTaskTest;
import org.apache.ignite.internal.processors.cache.checker.tasks.CollectPartitionKeysByRecheckRequestTaskTest;
import org.apache.ignite.internal.processors.cache.checker.tasks.RepairEntryProcessorTest;
import org.apache.ignite.internal.processors.cache.checker.tasks.RepairRequestTaskTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.DynamicSuite;
import org.junit.runner.RunWith;

/**
 * Test suite.
 */
@RunWith(DynamicSuite.class)
public class PartitionReconciliationTestSuite {
    /**
     * @return IgniteCache test suite.
     */
    public static List<Class<?>> suite() {
        return suite(null);
    }

    /**
     * @param ignoredTests Tests to ignore.
     * @return Test suite.
     */
    public static List<Class<?>> suite(Collection<Class> ignoredTests) {
        List<Class<?>> suite = new ArrayList<>();

//        GridTestUtils.addTestIfNeeded(suite, PartitionReconciliationInterruptionRecheckTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, PartitionReconciliationInterruptionRepairTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, PartitionReconciliationProcessorTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, PartitionReconciliationRecheckAttemptsTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, PartitionReconciliationStressTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, PartitionReconciliationFixStressTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, PartitionReconciliationBinaryObjectsTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, CollectPartitionKeysByBatchTaskTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, PartitionReconciliationFullFixStressTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, CollectPartitionKeysByRecheckRequestTaskTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, ConsistencyCheckUtilsTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, RepairEntryProcessorTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, RepairRequestTaskTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, ReconciliationResourceLimitedJobTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, PartitionReconciliationFastCheckTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, PartitionReconciliationCompactCollectorTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, PartitionReconciliationAtomicLongDataStructureTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, PartitionReconciliationAtomicLongFixStressTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, PartitionReconciliationSetDataStructureTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, PartitionReconciliationAtomicLongStressTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, PartitionReconciliationSystemFastCheckTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, PartitionReconciliationFixPartitionSizesStressParameterizedTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, PartitionReconciliationFixPartitionSizesStressParameterizedTest1.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, PartitionReconciliationFixPartitionSizesStressParameterizedTest2.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, PartitionReconciliationFixPartitionSizesStressParameterizedTest3.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, PartitionReconciliationFixPartitionSizesStressParameterizedTest4.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, PartitionReconciliationFixPartitionSizesStressParameterizedTest5.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, PartitionReconciliationFixPartitionSizesStressParameterizedTest6.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, PartitionReconciliationFixPartitionSizesStressParameterizedTest7.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, PartitionReconciliationFixPartitionSizesStressParameterizedTest8.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, PartitionReconciliationFixPartitionSizesStressParameterizedTest9.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, PartitionReconciliationFixPartitionSizesStressRandomizedTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, PartitionReconciliationFixPartitionSizesStressRandomizedTest1.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, PartitionReconciliationFixPartitionSizesStressRandomizedTest2.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, PartitionReconciliationFixPartitionSizesStressRandomizedTest3.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, PartitionReconciliationFixPartitionSizesStressRandomizedTest4.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, PartitionReconciliationFixPartitionSizesStressRandomizedTest5.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, PartitionReconciliationFixPartitionSizesStressRandomizedTest6.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, PartitionReconciliationFixPartitionSizesStressRandomizedTest7.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, PartitionReconciliationFixPartitionSizesPersistenceStressParameterizedTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, PartitionReconciliationFixPartitionSizesTest.class, ignoredTests);

        return suite;
    }
}
