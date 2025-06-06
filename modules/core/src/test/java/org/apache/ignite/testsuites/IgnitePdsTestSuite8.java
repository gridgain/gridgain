/*
 * Copyright 2025 GridGain Systems, Inc. and Contributors.
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
import org.apache.ignite.internal.processors.cache.distributed.rebalancing.CacheRebalanceWithRemovedWalSegment;
import org.apache.ignite.internal.processors.cache.distributed.rebalancing.SupplyPartitionHistoricallyWithReorderedUpdates;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsCacheEntriesExpirationTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsConsistencyOnDelayedPartitionOwning;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsDefragmentationEncryptionTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsDefragmentationOldCacheTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsDefragmentationRandomLruEvictionTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsDefragmentationTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsRecoveryAfterFileCorruptionTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsTxRecordLoggingTest;
import org.apache.ignite.internal.processors.cache.persistence.MaintenanceAutoRecoveryTest;
import org.apache.ignite.internal.processors.cache.persistence.MaintenanceClearCacheFolderTest;
import org.apache.ignite.internal.processors.cache.persistence.MaintenancePersistenceTaskTest;
import org.apache.ignite.internal.processors.cache.persistence.PagesPossibleCorruptionDiagnosticTest;
import org.apache.ignite.internal.processors.cache.persistence.PendingTreeCorruptionTest;
import org.apache.ignite.internal.processors.cache.persistence.db.IgnitePdsPageReplacementDuringPartitionClearTest;
import org.apache.ignite.internal.processors.cache.persistence.db.IgnitePdsTransactionsHangTest;
import org.apache.ignite.internal.processors.cache.persistence.defragmentation.DefragmentationMXBeanTest;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageLockTrackerManagerTest;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageLockTrackerResourcesTest;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.SharedPageLockTrackerTest;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.dumpprocessors.ToFileDumpProcessorTest;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.dumpprocessors.ToStringDumpHelperTest;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.log.HeapArrayLockLogTest;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.log.OffHeapLockLogTest;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.stack.HeapArrayLockStackTest;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.stack.OffHeapLockStackTest;
import org.apache.ignite.internal.processors.cache.warmup.LoadAllWarmUpStrategySelfTest;
import org.apache.ignite.internal.processors.cache.warmup.WarmUpSelfTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.DynamicSuite;
import org.junit.runner.RunWith;

/** */
@RunWith(DynamicSuite.class)
public class IgnitePdsTestSuite8 {
    /**
     * @return Suite.
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

        // Page lock tracker tests.
        GridTestUtils.addTestIfNeeded(suite, PageLockTrackerManagerTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, SharedPageLockTrackerTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, ToFileDumpProcessorTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, HeapArrayLockLogTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, HeapArrayLockStackTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, OffHeapLockLogTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, OffHeapLockStackTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, ToStringDumpHelperTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, PageLockTrackerResourcesTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, IgnitePdsCacheEntriesExpirationTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, IgnitePdsConsistencyOnDelayedPartitionOwning.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, SupplyPartitionHistoricallyWithReorderedUpdates.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheRebalanceWithRemovedWalSegment.class, ignoredTests);

        // Warm-up tests.
        GridTestUtils.addTestIfNeeded(suite, WarmUpSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, LoadAllWarmUpStrategySelfTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, PendingTreeCorruptionTest.class, ignoredTests);

        // Defragmentation.
        GridTestUtils.addTestIfNeeded(suite, IgnitePdsDefragmentationTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgnitePdsDefragmentationRandomLruEvictionTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgnitePdsDefragmentationEncryptionTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgnitePdsDefragmentationOldCacheTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, DefragmentationMXBeanTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, PagesPossibleCorruptionDiagnosticTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, MaintenancePersistenceTaskTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, MaintenanceClearCacheFolderTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, MaintenanceAutoRecoveryTest.class, ignoredTests);

        // Tx record logging tests.
        GridTestUtils.addTestIfNeeded(suite, IgnitePdsTxRecordLoggingTest.class, ignoredTests);

        return suite;
    }

    /**
     * Fills {@code suite} with PDS test subset, which operates with real page store, but requires long time to execute.
     *
     * @param suite suite to add tests into.
     * @param ignoredTests Ignored tests.
     */
    private static void addRealPageStoreTestsNotForDirectIo(List<Class<?>> suite, Collection<Class> ignoredTests) {
        GridTestUtils.addTestIfNeeded(suite, IgnitePdsTransactionsHangTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgnitePdsPageReplacementDuringPartitionClearTest.class, ignoredTests);

        // Integrity test.
        GridTestUtils.addTestIfNeeded(suite, IgnitePdsRecoveryAfterFileCorruptionTest.class, ignoredTests);
    }
}
