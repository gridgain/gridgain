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
import org.apache.ignite.internal.processors.cache.checker.processor.PartitionReconciliationBinaryObjectsTest;
import org.apache.ignite.internal.processors.cache.checker.processor.PartitionReconciliationCompactCollectorTest;
import org.apache.ignite.internal.processors.cache.checker.processor.PartitionReconciliationFastCheckTest;
import org.apache.ignite.internal.processors.cache.checker.processor.PartitionReconciliationFixStressTest;
import org.apache.ignite.internal.processors.cache.checker.processor.PartitionReconciliationFullFixStressTest;
import org.apache.ignite.internal.processors.cache.checker.processor.PartitionReconciliationInterruptionRecheckTest;
import org.apache.ignite.internal.processors.cache.checker.processor.PartitionReconciliationInterruptionRepairTest;
import org.apache.ignite.internal.processors.cache.checker.processor.PartitionReconciliationProcessorTest;
import org.apache.ignite.internal.processors.cache.checker.processor.PartitionReconciliationRecheckAttemptsTest;
import org.apache.ignite.internal.processors.cache.checker.processor.PartitionReconciliationStressTest;
import org.apache.ignite.internal.processors.cache.checker.processor.ReconciliationResourceLimitedJobTest;
import org.apache.ignite.internal.processors.cache.checker.tasks.CollectPartitionKeysByBatchTaskTest;
import org.apache.ignite.internal.processors.cache.checker.tasks.CollectPartitionKeysByRecheckRequestTaskTest;
import org.apache.ignite.internal.processors.cache.checker.tasks.RepairEntryProcessorTest;
import org.apache.ignite.internal.processors.cache.checker.tasks.RepairRequestTaskTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.DynamicSuite;
import org.apache.ignite.util.GridCommandHandlerPartitionReconciliationAtomicPersistentTest;
import org.apache.ignite.util.GridCommandHandlerPartitionReconciliationAtomicTest;
import org.apache.ignite.util.GridCommandHandlerPartitionReconciliationCommonTest;
import org.apache.ignite.util.GridCommandHandlerPartitionReconciliationExtendedTest;
import org.apache.ignite.util.GridCommandHandlerPartitionReconciliationReplicatedPersistentTest;
import org.apache.ignite.util.GridCommandHandlerPartitionReconciliationReplicatedTest;
import org.apache.ignite.util.GridCommandHandlerPartitionReconciliationTxPersistentTest;
import org.apache.ignite.util.GridCommandHandlerPartitionReconciliationTxTest;
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

        GridTestUtils.addTestIfNeeded(suite, GridCommandHandlerPartitionReconciliationTxTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCommandHandlerPartitionReconciliationTxPersistentTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCommandHandlerPartitionReconciliationReplicatedTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCommandHandlerPartitionReconciliationReplicatedPersistentTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCommandHandlerPartitionReconciliationExtendedTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCommandHandlerPartitionReconciliationCommonTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCommandHandlerPartitionReconciliationAtomicTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCommandHandlerPartitionReconciliationAtomicPersistentTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, PartitionReconciliationInterruptionRecheckTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, PartitionReconciliationInterruptionRepairTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, PartitionReconciliationProcessorTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, PartitionReconciliationRecheckAttemptsTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, PartitionReconciliationStressTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, PartitionReconciliationFixStressTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, PartitionReconciliationBinaryObjectsTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CollectPartitionKeysByBatchTaskTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, PartitionReconciliationFullFixStressTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CollectPartitionKeysByRecheckRequestTaskTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, ConsistencyCheckUtilsTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, RepairEntryProcessorTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, RepairRequestTaskTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, ReconciliationResourceLimitedJobTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, PartitionReconciliationFastCheckTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, PartitionReconciliationCompactCollectorTest.class, ignoredTests);

        return suite;
    }
}
