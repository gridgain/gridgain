/*
 * Copyright 2022 GridGain Systems, Inc. and Contributors.
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
import org.apache.ignite.cache.LocalCacheWarningsTest;
import org.apache.ignite.internal.metric.CacheMetricsAddRemoveTest;
import org.apache.ignite.internal.metric.IoStatisticsCachePersistenceSelfTest;
import org.apache.ignite.internal.metric.IoStatisticsCacheSelfTest;
import org.apache.ignite.internal.metric.IoStatisticsMetricsLocalMXBeanImplSelfTest;
import org.apache.ignite.internal.metric.IoStatisticsMetricsLocalMxBeanCacheGroupsTest;
import org.apache.ignite.internal.metric.IoStatisticsSelfTest;
import org.apache.ignite.internal.metric.JmxExporterSpiTest;
import org.apache.ignite.internal.metric.LogExporterSpiTest;
import org.apache.ignite.internal.metric.MetricsConfigurationTest;
import org.apache.ignite.internal.metric.MetricsSelfTest;
import org.apache.ignite.internal.metric.ReadMetricsOnNodeStartupTest;
import org.apache.ignite.internal.metric.SystemMetricsTest;
import org.apache.ignite.internal.metric.SystemViewClusterActivationTest;
import org.apache.ignite.internal.metric.SystemViewComputeJobTest;
import org.apache.ignite.internal.metric.SystemViewSelfTest;
import org.apache.ignite.internal.processors.cache.CacheClearAsyncDeadlockTest;
import org.apache.ignite.internal.processors.cache.GridCacheDataTypesCoverageTest;
import org.apache.ignite.internal.processors.cache.GridCacheLongRunningTransactionDiagnosticsTest;
import org.apache.ignite.internal.processors.cache.GridCacheVersionGenerationWithCacheStorageTest;
import org.apache.ignite.internal.processors.cache.IgniteOOMWithoutNodeFailureTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheRemoveWithTombstonesBasicTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheRemoveWithTombstonesFailoverTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheRemoveWithTombstonesPreloadingTest;
import org.apache.ignite.internal.processors.cache.distributed.FailBackupOnAtomicOperationTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.HistoricalRebalanceRemovesConsistencyTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.ConcurrentTombstonesCleanupTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.TombstoneClearingCountersTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.TombstonesManagementTest;
import org.apache.ignite.internal.processors.cache.transactions.TransactionCommitTest;
import org.apache.ignite.internal.processors.cache.transactions.TxRecoveryOnCoordniatorFailTest;
import org.apache.ignite.internal.processors.cluster.ClusterNameBeforeActivation;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.DynamicSuite;
import org.junit.runner.RunWith;

/**
 * Test suite.
 */
@RunWith(DynamicSuite.class)
public class IgniteCacheTestSuite13 {
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

        // IO statistics.
        GridTestUtils.addTestIfNeeded(suite, IoStatisticsCachePersistenceSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IoStatisticsCacheSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IoStatisticsSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IoStatisticsMetricsLocalMXBeanImplSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IoStatisticsMetricsLocalMxBeanCacheGroupsTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, MetricsSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, SystemMetricsTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, MetricsConfigurationTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, SystemViewSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, SystemViewClusterActivationTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, SystemViewComputeJobTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheMetricsAddRemoveTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, JmxExporterSpiTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, LogExporterSpiTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, ReadMetricsOnNodeStartupTest.class, ignoredTests);

        // Compatibility statistics tests.
        GridTestUtils.addTestIfNeeded(suite, org.apache.ignite.internal.stat.IoStatisticsManagerSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, org.apache.ignite.internal.stat.IoStatisticsMetricsLocalMXBeanImplSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, org.apache.ignite.internal.stat.IoStatisticsMetricsLocalMxBeanImplIllegalArgumentsTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, org.apache.ignite.internal.stat.IoStatisticsMetricsLocalMxBeanCacheGroupsTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, org.apache.ignite.internal.stat.IoStatisticsMetricsLocalMXBeanCacheSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, org.apache.ignite.internal.stat.IoStatisticsMetricsLocalMXBeanCachePersistenceSelfTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, GridCacheLongRunningTransactionDiagnosticsTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, FailBackupOnAtomicOperationTest.class, ignoredTests);

        // Data Types coverage
        GridTestUtils.addTestIfNeeded(suite, GridCacheDataTypesCoverageTest.class, ignoredTests);

        // Grid Cache Version generation coverage.
        GridTestUtils.addTestIfNeeded(suite, GridCacheVersionGenerationWithCacheStorageTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, HistoricalRebalanceRemovesConsistencyTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, IgniteOOMWithoutNodeFailureTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, CacheRemoveWithTombstonesBasicTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheRemoveWithTombstonesFailoverTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheRemoveWithTombstonesPreloadingTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, TombstoneClearingCountersTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, TombstonesManagementTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, ConcurrentTombstonesCleanupTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, TxRecoveryOnCoordniatorFailTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, ClusterNameBeforeActivation.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, CacheClearAsyncDeadlockTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, TransactionCommitTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, LocalCacheWarningsTest.class, ignoredTests);

        return suite;
    }
}
