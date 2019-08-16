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

import org.apache.ignite.internal.processors.query.oom.DiskSpillingBasicTest;
import org.apache.ignite.internal.processors.query.oom.DiskSpillingGlobalQuotaTest;
import org.apache.ignite.internal.processors.query.oom.DiskSpillingMultipleIndexesTest;
import org.apache.ignite.internal.processors.query.oom.DiskSpillingMultipleNodesTest;
import org.apache.ignite.internal.processors.query.oom.DiskSpillingPersistenceTest;
import org.apache.ignite.internal.processors.query.oom.DiskSpillingQueriesTest;
import org.apache.ignite.internal.processors.query.oom.DiskSpillingQueryParallelismTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Test suite for cache queries.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    // Dynamic index create/drop tests.
//    DynamicIndexPartitionedAtomicConcurrentSelfTest.class,
//    DynamicIndexPartitionedTransactionalConcurrentSelfTest.class,
//    DynamicIndexReplicatedAtomicConcurrentSelfTest.class,
//    DynamicIndexReplicatedTransactionalConcurrentSelfTest.class,
//
//    DynamicColumnsConcurrentAtomicPartitionedSelfTest.class,
//    DynamicColumnsConcurrentTransactionalPartitionedSelfTest.class,
//    DynamicColumnsConcurrentAtomicReplicatedSelfTest.class,
//    DynamicColumnsConcurrentTransactionalReplicatedSelfTest.class,
//
//    // Distributed joins.
//    IgniteCacheQueryNodeRestartDistributedJoinSelfTest.class,
//    IgniteCacheQueryStopOnCancelOrTimeoutDistributedJoinSelfTest.class,
//
//    // Other tests.
//    IgniteCacheQueryMultiThreadedSelfTest.class,
//
//    IgniteCacheQueryEvictsMultiThreadedSelfTest.class,
//
//    ScanQueryOffheapExpiryPolicySelfTest.class,
//
//    IgniteCacheCrossCacheJoinRandomTest.class,
//    IgniteCacheClientQueryReplicatedNodeRestartSelfTest.class,
//    IgniteCacheQueryNodeFailTest.class,
//    IgniteCacheQueryNodeRestartSelfTest.class,
//    IgniteSqlQueryWithBaselineTest.class,
//    IgniteChangingBaselineCacheQueryNodeRestartSelfTest.class,
//    IgniteStableBaselineCacheQueryNodeRestartsSelfTest.class,
//    IgniteCacheQueryNodeRestartSelfTest2.class,
//    IgniteCacheQueryNodeRestartTxSelfTest.class,
//    IgniteCacheSqlQueryMultiThreadedSelfTest.class,
//    IgniteCachePartitionedQueryMultiThreadedSelfTest.class,
//    CacheScanPartitionQueryFallbackSelfTest.class,
//    IgniteCacheDistributedQueryStopOnCancelOrTimeoutSelfTest.class,
//    IgniteCacheObjectKeyIndexingSelfTest.class,
//
//    IgniteCacheGroupsCompareQueryTest.class,
//    IgniteCacheGroupsSqlSegmentedIndexSelfTest.class,
//    IgniteCacheGroupsSqlSegmentedIndexMultiNodeSelfTest.class,
//    IgniteCacheGroupsSqlDistributedJoinSelfTest.class,
//
//    QueryJoinWithDifferentNodeFiltersTest.class,
//
//    CacheQueryMemoryLeakTest.class,
//
//    CreateTableWithDateKeySelfTest.class,
//
//    CacheQueryEntityWithDateTimeApiFieldsTest.class,
//
//    DmlStatementsProcessorTest.class,
//
//    NonCollocatedRetryMessageSelfTest.class,
//    RetryCauseMessageSelfTest.class,
//    DisappearedCacheCauseRetryMessageSelfTest.class,
//    DisappearedCacheWasNotFoundMessageSelfTest.class,
//
//    TableViewSubquerySelfTest.class,
//
//    SqlLocalQueryConnectionAndStatementTest.class,
//
//    NoneOrSinglePartitionsQueryOptimizationsTest.class,
//
//    IgniteSqlCreateTableTemplateTest.class,
//
//    LocalQueryLazyTest.class,
//
//    LongRunningQueryTest.class,
//
//    LocalQueryMemoryTrackerSelfTest.class,
//    LocalQueryMemoryTrackerWithQueryParallelismSelfTest.class,
//
//    SqlStatisticsMemoryQuotaTest.class,
//
//    QueryMemoryTrackerSelfTest.class,
//
//    DmlBatchSizeDeadlockTest.class,

    DiskSpillingBasicTest.class,
    DiskSpillingGlobalQuotaTest.class,
    DiskSpillingQueriesTest.class,
    DiskSpillingMultipleNodesTest.class,
    DiskSpillingPersistenceTest.class,
    DiskSpillingQueryParallelismTest.class,
    DiskSpillingMultipleIndexesTest.class,
//
//    GridCachePartitionedTxMultiNodeSelfTest.class,
//    GridCacheReplicatedTxMultiNodeBasicTest.class
})
public class IgniteBinaryCacheQueryTestSuite2 {
}
