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

import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.HistoricalRebalanceHeuristicsTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.HistoricalRebalanceTwoPartsInDifferentCheckpointsTest;
import org.apache.ignite.internal.processors.cache.persistence.IgniteDataStorageMetricsSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsCacheStartStopWithFreqCheckpointTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsCorruptedStoreTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsExchangeDuringCheckpointTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsPageSizesTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsPartitionFilesDestroyTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsPartitionsStateRecoveryTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePersistentStoreDataStructuresTest;
import org.apache.ignite.internal.processors.cache.persistence.IgniteRebalanceScheduleResendPartitionsTest;
import org.apache.ignite.internal.processors.cache.persistence.LocalWalModeChangeDuringRebalancingSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.LocalWalModeNoChangeDuringRebalanceOnNonNodeAssignTest;
import org.apache.ignite.internal.processors.cache.persistence.MaintenanceRegistrySimpleTest;
import org.apache.ignite.internal.processors.cache.persistence.WALPreloadingWithCompactionTest;
import org.apache.ignite.internal.processors.cache.persistence.WalPreloadingConcurrentTest;
import org.apache.ignite.internal.processors.cache.persistence.baseline.ClientAffinityAssignmentWithBaselineTest;
import org.apache.ignite.internal.processors.cache.persistence.baseline.ClusterActivationEventTest;
import org.apache.ignite.internal.processors.cache.persistence.baseline.ClusterActivationEventWithPersistenceTest;
import org.apache.ignite.internal.processors.cache.persistence.baseline.IgniteAbsentEvictionNodeOutOfBaselineTest;
import org.apache.ignite.internal.processors.cache.persistence.baseline.IgniteAllBaselineNodesOnlineFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.baseline.IgniteBaselineRestartClusterTest;
import org.apache.ignite.internal.processors.cache.persistence.baseline.IgniteOfflineBaselineNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.baseline.IgniteOnlineNodeOutOfBaselineFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.db.FullHistRebalanceOnClientStopTest;
import org.apache.ignite.internal.processors.cache.persistence.db.IgnitePdsRebalancingOnNotStableTopologyTest;
import org.apache.ignite.internal.processors.cache.persistence.db.IgnitePdsReserveWalSegmentsTest;
import org.apache.ignite.internal.processors.cache.persistence.db.IgnitePdsReserveWalSegmentsWithCompactionTest;
import org.apache.ignite.internal.processors.cache.persistence.db.IgnitePdsWholeClusterRestartTest;
import org.apache.ignite.internal.processors.cache.persistence.db.IgniteShutdownOnSupplyMessageFailureTest;
import org.apache.ignite.internal.processors.cache.persistence.db.SlowHistoricalRebalanceSmallHistoryTest;
import org.apache.ignite.internal.processors.cache.persistence.db.checkpoint.CheckpointFailBeforeWriteMarkTest;
import org.apache.ignite.internal.processors.cache.persistence.db.checkpoint.CheckpointFreeListTest;
import org.apache.ignite.internal.processors.cache.persistence.db.checkpoint.CheckpointListenerForRegionTest;
import org.apache.ignite.internal.processors.cache.persistence.db.checkpoint.CheckpointMarkerReadingErrorOnStartTest;
import org.apache.ignite.internal.processors.cache.persistence.db.checkpoint.CheckpointTempFilesCleanupOnStartupTest;
import org.apache.ignite.internal.processors.cache.persistence.db.checkpoint.IgniteCheckpointDirtyPagesForLowLoadTest;
import org.apache.ignite.internal.processors.cache.persistence.db.checkpoint.LightweightCheckpointTest;
import org.apache.ignite.internal.processors.cache.persistence.db.filename.IgniteUidAsConsistentIdMigrationTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.CorruptedCheckpointReservationTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.FsyncWalRolloverDoesNotBlockTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteLocalWalSizeTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteNodeStoppedDuringDisableWALTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWALTailIsReachedDuringIterationOverArchiveTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalFlushBackgroundSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalFlushBackgroundWithMmapBufferSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalFlushFailoverTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalFlushFsyncSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalFlushFsyncWithDedicatedWorkerSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalFlushFsyncWithMmapBufferSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalFlushLogOnlySelfTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalFlushLogOnlyWithMmapBufferSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalFormatFileFailoverTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalHistoryReservationsTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalIteratorExceptionDuringReadTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalIteratorSwitchSegmentTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalRebalanceLoggingTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalReplayingAfterRestartTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalSerializerVersionTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.WalArchiveSizeConfigurationTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.WalCompactionNoArchiverTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.WalCompactionSwitchOnTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.WalCompactionTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.WalDeletionArchiveFsyncTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.WalDeletionArchiveLogOnlyTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.WalRolloverTypesTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.WriteAheadLogManagerSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.crc.IgniteDataIntegrityTests;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.crc.IgniteFsyncReplayWalIteratorInvalidCrcTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.crc.IgnitePureJavaCrcCompatibility;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.crc.IgniteReplayWalIteratorInvalidCrcTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.crc.IgniteStandaloneWalIteratorInvalidCrcTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.crc.IgniteWithoutArchiverWalIteratorInvalidCrcTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.reader.IgniteWalReaderTest;
import org.apache.ignite.internal.processors.cache.persistence.freelist.FreeListCachingTest;
import org.apache.ignite.internal.processors.cache.persistence.io.GGPageIoTest;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.FilteredWalIteratorTest;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.StandaloneWalRecordsIteratorTest;
import org.apache.ignite.internal.processors.cache.persistence.wal.scanner.WalScannerTest;
import org.apache.ignite.internal.processors.cluster.ClusterStateChangeEventTest;
import org.apache.ignite.internal.processors.cluster.ClusterStateChangeEventWithPersistenceTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.DynamicSuite;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/** */
@RunWith(DynamicSuite.class)
public class IgnitePdsTestSuite6 {
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

        // Integrity test.
        GridTestUtils.addTestIfNeeded(suite, IgniteDataIntegrityTests.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteStandaloneWalIteratorInvalidCrcTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteReplayWalIteratorInvalidCrcTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteFsyncReplayWalIteratorInvalidCrcTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgnitePureJavaCrcCompatibility.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteWithoutArchiverWalIteratorInvalidCrcTest.class, ignoredTests);

        addRealPageStoreTestsNotForDirectIo(suite, ignoredTests);

        // BaselineTopology tests
        GridTestUtils.addTestIfNeeded(suite, IgniteAllBaselineNodesOnlineFullApiSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteOfflineBaselineNodeFullApiSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteOnlineNodeOutOfBaselineFullApiSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, ClientAffinityAssignmentWithBaselineTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteAbsentEvictionNodeOutOfBaselineTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, ClusterActivationEventTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, ClusterActivationEventWithPersistenceTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, ClusterStateChangeEventTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, ClusterStateChangeEventWithPersistenceTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteBaselineRestartClusterTest.class, ignoredTests);

        // Maintenance tests
        GridTestUtils.addTestIfNeeded(suite, MaintenanceRegistrySimpleTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, WalArchiveSizeConfigurationTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, CheckpointMarkerReadingErrorOnStartTest.class, ignoredTests);

        return suite;
    }

    /**
     * Fills {@code suite} with PDS test subset, which operates with real page store, but requires long time to
     * execute.
     *
     * @param suite suite to add tests into.
     * @param ignoredTests Ignored tests.
     */
    private static void addRealPageStoreTestsNotForDirectIo(List<Class<?>> suite, Collection<Class> ignoredTests) {
        GridTestUtils.addTestIfNeeded(suite, IgnitePdsPartitionFilesDestroyTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, LocalWalModeChangeDuringRebalancingSelfTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, LocalWalModeNoChangeDuringRebalanceOnNonNodeAssignTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, IgniteWalFlushFsyncSelfTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, IgniteWalFlushFsyncWithDedicatedWorkerSelfTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, IgniteWalFlushFsyncWithMmapBufferSelfTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, IgnitePdsCacheStartStopWithFreqCheckpointTest.class, ignoredTests);
    }
}
