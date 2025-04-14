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

import org.apache.ignite.internal.metric.SqlStatisticOffloadingTest;
import org.apache.ignite.internal.metric.SqlStatisticsMemoryQuotaTest;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.metric.SqlStatisticsUserQueriesFastTest;
import org.apache.ignite.internal.metric.SqlStatisticsUserQueriesLongTest;
import org.apache.ignite.internal.processors.cache.SqlPageLocksDumpTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedTxMultiNodeSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheQueryReservationOnUnstableTopologyTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedTxMultiNodeBasicTest;
import org.apache.ignite.internal.processors.query.DmlBatchSizeDeadlockTest;
import org.apache.ignite.internal.processors.query.IgniteInsertNullableDuplicatesSqlTest;
import org.apache.ignite.internal.processors.query.IgniteSqlCreateTableTemplateTest;
import org.apache.ignite.internal.processors.query.LocalQueryLazyTest;
import org.apache.ignite.internal.processors.query.LongRunningQueryTest;
import org.apache.ignite.internal.processors.query.LongRunningQueryThrottlingTest;
import org.apache.ignite.internal.processors.query.SqlLocalQueryConnectionAndStatementTest;
import org.apache.ignite.internal.processors.query.SqlPartOfComplexPkLookupTest;
import org.apache.ignite.internal.processors.query.SqlQueriesTopologyMappingTest;
import org.apache.ignite.internal.processors.query.h2.CacheQueryEntityWithDateTimeApiFieldsTest;
import org.apache.ignite.internal.processors.query.h2.DmlStatementsProcessorTest;
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
import org.apache.ignite.internal.processors.query.oom.DiskSpillingLoggingTest;
import org.apache.ignite.internal.processors.query.oom.DiskSpillingMemoryTrackerTest;
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
import org.apache.ignite.internal.processors.query.oom.MemoryTrackerOnReducerTest;
import org.apache.ignite.internal.processors.query.oom.OOMLeadsTest;
import org.apache.ignite.internal.processors.query.oom.QueryMemoryManagerConfigurationSelfTest;
import org.apache.ignite.internal.processors.query.oom.QueryMemoryManagerSelfTest;
import org.apache.ignite.internal.processors.query.oom.QueryMemoryTrackerSelfTest;
import org.apache.ignite.sqltests.SqlDataTypesCoverageTests;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Test suite for cache queries.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
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
    LongRunningQueryThrottlingTest.class,

    SqlStatisticsUserQueriesFastTest.class,
    SqlStatisticsUserQueriesLongTest.class,

    IgniteInsertNullableDuplicatesSqlTest.class,

    // Memory quota tests.
    SqlStatisticsMemoryQuotaTest.class,
    LocalQueryMemoryTrackerSelfTest.class,
    LocalQueryMemoryTrackerWithQueryParallelismSelfTest.class,
    QueryMemoryTrackerSelfTest.class,
    QueryMemoryManagerSelfTest.class,
    MemoryQuotaDynamicConfigurationTest.class,
    MemoryQuotaStaticConfigurationTest.class,
    MemoryQuotaStaticAndDynamicConfigurationTest.class,
    QueryMemoryManagerConfigurationSelfTest.class,
    ClientQueryQuotaTest.class,
    MemoryTrackerOnReducerTest.class,

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

    GridQueryMemoryMetricProviderSelfTest.class,

    SqlPartOfComplexPkLookupTest.class,

    SqlQueriesTopologyMappingTest.class,

    SqlDataTypesCoverageTests.class,

    DmlBatchSizeDeadlockTest.class,

    GridCachePartitionedTxMultiNodeSelfTest.class,
    GridCacheReplicatedTxMultiNodeBasicTest.class,

    IgniteCacheQueryReservationOnUnstableTopologyTest.class,

    ScriptTestSuite.class,
    OOMLeadsTest.class,

    SqlPageLocksDumpTest.class
})
public class IgniteBinaryCacheQueryTestSuite4 {
    /** Setup lazy mode default. */
    @BeforeClass
    public static void setupLazy() {
        GridTestUtils.setFieldValue(SqlFieldsQuery.class, "DFLT_LAZY", false);
    }
}
