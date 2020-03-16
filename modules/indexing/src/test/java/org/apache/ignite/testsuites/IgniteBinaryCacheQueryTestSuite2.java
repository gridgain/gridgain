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

import org.apache.ignite.internal.metric.SqlStatisticOffloadingTest;
import org.apache.ignite.internal.metric.SqlStatisticsMemoryQuotaTest;
import org.apache.ignite.internal.metric.SqlStatisticsUserQueriesFastTest;
import org.apache.ignite.internal.metric.SqlStatisticsUserQueriesLongTest;
import org.apache.ignite.internal.processors.cache.CacheScanPartitionQueryFallbackSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheCrossCacheJoinRandomTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheObjectKeyIndexingSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCachePartitionedQueryMultiThreadedSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheQueryEvictsMultiThreadedSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheQueryMultiThreadedSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheSqlQueryMultiThreadedSelfTest;
import org.apache.ignite.internal.processors.cache.QueryJoinWithDifferentNodeFiltersTest;
import org.apache.ignite.internal.processors.cache.SqlCacheStartStopTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedTxMultiNodeSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheClientQueryReplicatedNodeRestartSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheDistributedQueryStopOnCancelOrTimeoutSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheQueryNodeFailTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheQueryNodeRestartDistributedJoinSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheQueryNodeRestartSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheQueryNodeRestartSelfTest2;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheQueryNodeRestartTxSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheQueryStopOnCancelOrTimeoutDistributedJoinSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteSqlQueryWithBaselineTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedTxMultiNodeBasicTest;
import org.apache.ignite.internal.processors.cache.index.ChooseIndexTest;
import org.apache.ignite.internal.processors.cache.index.DynamicColumnsConcurrentAtomicPartitionedSelfTest;
import org.apache.ignite.internal.processors.cache.index.DynamicColumnsConcurrentAtomicReplicatedSelfTest;
import org.apache.ignite.internal.processors.cache.index.DynamicColumnsConcurrentTransactionalPartitionedSelfTest;
import org.apache.ignite.internal.processors.cache.index.DynamicColumnsConcurrentTransactionalReplicatedSelfTest;
import org.apache.ignite.internal.processors.cache.index.DynamicIndexPartitionedAtomicConcurrentSelfTest;
import org.apache.ignite.internal.processors.cache.index.DynamicIndexPartitionedTransactionalConcurrentSelfTest;
import org.apache.ignite.internal.processors.cache.index.DynamicIndexReplicatedAtomicConcurrentSelfTest;
import org.apache.ignite.internal.processors.cache.index.DynamicIndexReplicatedTransactionalConcurrentSelfTest;
import org.apache.ignite.internal.processors.cache.query.ScanQueryOffheapExpiryPolicySelfTest;
import org.apache.ignite.internal.processors.database.baseline.IgniteChangingBaselineCacheQueryNodeRestartSelfTest;
import org.apache.ignite.internal.processors.database.baseline.IgniteStableBaselineCacheQueryNodeRestartsSelfTest;
import org.apache.ignite.internal.processors.query.DmlBatchSizeDeadlockTest;
import org.apache.ignite.internal.processors.query.IgniteCacheGroupsCompareQueryTest;
import org.apache.ignite.internal.processors.query.IgniteCacheGroupsSqlDistributedJoinSelfTest;
import org.apache.ignite.internal.processors.query.IgniteCacheGroupsSqlSegmentedIndexMultiNodeSelfTest;
import org.apache.ignite.internal.processors.query.IgniteCacheGroupsSqlSegmentedIndexSelfTest;
import org.apache.ignite.internal.processors.query.IgniteSqlCreateTableTemplateTest;
import org.apache.ignite.internal.processors.query.LazyOnDmlTest;
import org.apache.ignite.internal.processors.query.LocalQueryLazyTest;
import org.apache.ignite.internal.processors.query.LongRunningQueryTest;
import org.apache.ignite.internal.processors.query.SqlIndexConsistencyAfterInterruptAtomicCacheOperationTest;
import org.apache.ignite.internal.processors.query.SqlIndexConsistencyAfterInterruptTxCacheOperationTest;
import org.apache.ignite.internal.processors.query.SqlInsertMergeImplicitColumnsTest;
import org.apache.ignite.internal.processors.query.SqlLocalQueryConnectionAndStatementTest;
import org.apache.ignite.internal.processors.query.SqlMergeOnClientNodeTest;
import org.apache.ignite.internal.processors.query.SqlMergeTest;
import org.apache.ignite.internal.processors.query.SqlPartOfComplexPkLookupTest;
import org.apache.ignite.internal.processors.query.SqlQueriesTopologyMappingTest;
import org.apache.ignite.internal.processors.query.SqlTwoCachesInGroupWithSameEntryTest;
import org.apache.ignite.internal.processors.query.h2.CacheQueryEntityWithDateTimeApiFieldsTest;
import org.apache.ignite.internal.processors.query.h2.DmlStatementsProcessorTest;
import org.apache.ignite.internal.processors.query.h2.StatementCacheTest;
import org.apache.ignite.internal.processors.query.h2.twostep.CacheQueryMemoryLeakTest;
import org.apache.ignite.internal.processors.query.h2.twostep.CreateTableWithDateKeySelfTest;
import org.apache.ignite.internal.processors.query.h2.twostep.DisappearedCacheCauseRetryMessageSelfTest;
import org.apache.ignite.internal.processors.query.h2.twostep.DisappearedCacheWasNotFoundMessageSelfTest;
import org.apache.ignite.internal.processors.query.h2.twostep.NonCollocatedRetryMessageSelfTest;
import org.apache.ignite.internal.processors.query.h2.twostep.NoneOrSinglePartitionsQueryOptimizationsTest;
import org.apache.ignite.internal.processors.query.h2.twostep.RetryCauseMessageSelfTest;
import org.apache.ignite.internal.processors.query.h2.twostep.TableViewSubquerySelfTest;
import org.apache.ignite.internal.processors.query.oom.ClientQueryQuotaTest;
import org.apache.ignite.internal.processors.query.oom.DiskSpillingBasicTest;
import org.apache.ignite.internal.processors.query.oom.DiskSpillingDmlTest;
import org.apache.ignite.internal.processors.query.oom.DiskSpillingGlobalQuotaTest;
import org.apache.ignite.internal.processors.query.oom.DiskSpillingIoErrorTest;
import org.apache.ignite.internal.processors.query.oom.DiskSpillingMemoryTrackerTest;
import org.apache.ignite.internal.processors.query.oom.DiskSpillingLoggingTest;
import org.apache.ignite.internal.processors.query.oom.DiskSpillingMultipleIndexesTest;
import org.apache.ignite.internal.processors.query.oom.DiskSpillingMultipleNodesTest;
import org.apache.ignite.internal.processors.query.oom.DiskSpillingPersistenceTest;
import org.apache.ignite.internal.processors.query.oom.DiskSpillingQueriesTest;
import org.apache.ignite.internal.processors.query.oom.DiskSpillingQueryParallelismTest;
import org.apache.ignite.internal.processors.query.oom.DiskSpillingWithBaselineTest;
import org.apache.ignite.internal.processors.query.oom.GridQueryMemoryMetricProviderSelfTest;
import org.apache.ignite.internal.processors.query.oom.LocalQueryMemoryTrackerSelfTest;
import org.apache.ignite.internal.processors.query.oom.LocalQueryMemoryTrackerWithQueryParallelismSelfTest;
import org.apache.ignite.internal.processors.query.oom.MemoryQuotaDynamicConfigurationTest;
import org.apache.ignite.internal.processors.query.oom.MemoryQuotaStaticAndDynamicConfigurationTest;
import org.apache.ignite.internal.processors.query.oom.MemoryQuotaStaticConfigurationTest;
import org.apache.ignite.internal.processors.query.oom.QueryMemoryManagerConfigurationSelfTest;
import org.apache.ignite.internal.processors.query.oom.QueryMemoryTrackerSelfTest;
import org.apache.ignite.sqltests.SqlDataTypesCoverageTests;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Test suite for cache queries.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    StatementCacheTest.class,
    ChooseIndexTest.class,
    LazyOnDmlTest.class,

    SqlInsertMergeImplicitColumnsTest.class,
    SqlMergeTest.class,
    SqlMergeOnClientNodeTest.class,

    SqlCacheStartStopTest.class,

    SqlIndexConsistencyAfterInterruptAtomicCacheOperationTest.class,
    SqlIndexConsistencyAfterInterruptTxCacheOperationTest.class,
    SqlTwoCachesInGroupWithSameEntryTest.class,

    // Dynamic index create/drop tests.
    DynamicIndexPartitionedAtomicConcurrentSelfTest.class,
    DynamicIndexPartitionedTransactionalConcurrentSelfTest.class,
    DynamicIndexReplicatedAtomicConcurrentSelfTest.class,
    DynamicIndexReplicatedTransactionalConcurrentSelfTest.class,

    DynamicColumnsConcurrentAtomicPartitionedSelfTest.class,
    DynamicColumnsConcurrentTransactionalPartitionedSelfTest.class,
    DynamicColumnsConcurrentAtomicReplicatedSelfTest.class,
    DynamicColumnsConcurrentTransactionalReplicatedSelfTest.class,

    // Distributed joins.
    IgniteCacheQueryNodeRestartDistributedJoinSelfTest.class,
    IgniteCacheQueryStopOnCancelOrTimeoutDistributedJoinSelfTest.class,

    // Other tests.
    IgniteCacheQueryMultiThreadedSelfTest.class,

    IgniteCacheQueryEvictsMultiThreadedSelfTest.class,

    ScanQueryOffheapExpiryPolicySelfTest.class,

    IgniteCacheCrossCacheJoinRandomTest.class,
    IgniteCacheClientQueryReplicatedNodeRestartSelfTest.class,
    IgniteCacheQueryNodeFailTest.class,
    IgniteCacheQueryNodeRestartSelfTest.class,
    IgniteSqlQueryWithBaselineTest.class,
    IgniteChangingBaselineCacheQueryNodeRestartSelfTest.class,
    IgniteStableBaselineCacheQueryNodeRestartsSelfTest.class,
    IgniteCacheQueryNodeRestartSelfTest2.class,
    IgniteCacheQueryNodeRestartTxSelfTest.class,
    IgniteCacheSqlQueryMultiThreadedSelfTest.class,
    IgniteCachePartitionedQueryMultiThreadedSelfTest.class,
    CacheScanPartitionQueryFallbackSelfTest.class,
    IgniteCacheDistributedQueryStopOnCancelOrTimeoutSelfTest.class,
    IgniteCacheObjectKeyIndexingSelfTest.class,

    IgniteCacheGroupsCompareQueryTest.class,
    IgniteCacheGroupsSqlSegmentedIndexSelfTest.class,
    IgniteCacheGroupsSqlSegmentedIndexMultiNodeSelfTest.class,
    IgniteCacheGroupsSqlDistributedJoinSelfTest.class,

    QueryJoinWithDifferentNodeFiltersTest.class,

    CacheQueryMemoryLeakTest.class,

    CreateTableWithDateKeySelfTest.class,

    CacheQueryEntityWithDateTimeApiFieldsTest.class,

    DmlStatementsProcessorTest.class,

    NonCollocatedRetryMessageSelfTest.class,
    RetryCauseMessageSelfTest.class,
    DisappearedCacheCauseRetryMessageSelfTest.class,
    DisappearedCacheWasNotFoundMessageSelfTest.class,

    TableViewSubquerySelfTest.class,

    SqlLocalQueryConnectionAndStatementTest.class,

    NoneOrSinglePartitionsQueryOptimizationsTest.class,

    IgniteSqlCreateTableTemplateTest.class,

    LocalQueryLazyTest.class,

    LongRunningQueryTest.class,

    SqlStatisticsUserQueriesFastTest.class,
    SqlStatisticsUserQueriesLongTest.class,

    // Memory quota tests.
    SqlStatisticsMemoryQuotaTest.class,
    LocalQueryMemoryTrackerSelfTest.class,
    LocalQueryMemoryTrackerWithQueryParallelismSelfTest.class,
    QueryMemoryTrackerSelfTest.class,
    MemoryQuotaDynamicConfigurationTest.class,
    MemoryQuotaStaticConfigurationTest.class,
    MemoryQuotaStaticAndDynamicConfigurationTest.class,
    QueryMemoryManagerConfigurationSelfTest.class,
    ClientQueryQuotaTest.class,

    // Offloading tests.
    DiskSpillingBasicTest.class,
    DiskSpillingGlobalQuotaTest.class,
    DiskSpillingQueriesTest.class,
    DiskSpillingMultipleNodesTest.class,
    DiskSpillingPersistenceTest.class,
    DiskSpillingQueryParallelismTest.class,
    DiskSpillingMultipleIndexesTest.class,
    DiskSpillingWithBaselineTest.class,
    DiskSpillingIoErrorTest.class,
    DiskSpillingDmlTest.class,
    SqlStatisticOffloadingTest.class,
    DiskSpillingLoggingTest.class,
    DiskSpillingMemoryTrackerTest.class,

    GridCachePartitionedTxMultiNodeSelfTest.class,
    GridCacheReplicatedTxMultiNodeBasicTest.class,

    GridQueryMemoryMetricProviderSelfTest.class,

    SqlPartOfComplexPkLookupTest.class,

    SqlDataTypesCoverageTests.class,

    SqlQueriesTopologyMappingTest.class,

    DmlBatchSizeDeadlockTest.class,
})
public class IgniteBinaryCacheQueryTestSuite2 {
}
