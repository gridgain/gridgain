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

import org.apache.ignite.internal.metric.IndexPagesMetricsInMemoryTest;
import org.apache.ignite.internal.processors.cache.BinaryTypeMismatchLoggingTest;
import org.apache.ignite.internal.processors.cache.BinaryTypeRegistrationTest;
import org.apache.ignite.internal.processors.cache.CacheBinaryKeyConcurrentQueryTest;
import org.apache.ignite.internal.processors.cache.CacheConfigurationP2PTest;
import org.apache.ignite.internal.processors.cache.CacheGroupMetricsWithIndexBuildFailTest;
import org.apache.ignite.internal.processors.cache.CacheGroupMetricsWithIndexTest;
import org.apache.ignite.internal.processors.cache.CacheIndexStreamerTest;
import org.apache.ignite.internal.processors.cache.CacheOperationsWithExpirationTest;
import org.apache.ignite.internal.processors.cache.CacheQueryAfterDynamicCacheStartFailureTest;
import org.apache.ignite.internal.processors.cache.CacheQueryFilterExpiredTest;
import org.apache.ignite.internal.processors.cache.CacheRandomOperationsMultithreadedTest;
import org.apache.ignite.internal.processors.cache.CacheRegisterMetadataLocallyTest;
import org.apache.ignite.internal.processors.cache.ClientReconnectAfterClusterRestartTest;
import org.apache.ignite.internal.processors.cache.ClusterReadOnlyModeDoesNotBreakSqlSelectTest;
import org.apache.ignite.internal.processors.cache.ClusterReadOnlyModeSqlTest;
import org.apache.ignite.internal.processors.cache.EnumClassImplementingIndexedInterfaceTest;
import org.apache.ignite.internal.processors.cache.GridCacheOffHeapSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheOffheapIndexEntryEvictTest;
import org.apache.ignite.internal.processors.cache.GridCacheOffheapIndexGetSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheSqlDdlClusterReadOnlyModeTest;
import org.apache.ignite.internal.processors.cache.GridIndexingWithNoopSwapSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheConfigurationPrimitiveTypesSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheGroupsSqlTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheStarvationOnRebalanceTest;
import org.apache.ignite.internal.processors.cache.IgniteClientReconnectQueriesTest;
import org.apache.ignite.internal.processors.cache.WrongIndexedTypesTest;
import org.apache.ignite.internal.processors.cache.checker.processor.PartitionReconciliationTombstonesWithIndicesTest;
import org.apache.ignite.internal.processors.cache.index.H2TreeCorruptedTreeExceptionTest;
import org.apache.ignite.internal.processors.cache.index.IndexCorruptionRebuildTest;
import org.apache.ignite.internal.processors.cache.persistence.RebuildIndexLogMessageTest;
import org.apache.ignite.internal.processors.cache.ttl.CacheSizeTtlTest;
import org.apache.ignite.internal.processors.cache.ttl.CacheTtlAtomicLocalSelfTest;
import org.apache.ignite.internal.processors.cache.ttl.CacheTtlAtomicPartitionedSelfTest;
import org.apache.ignite.internal.processors.cache.ttl.CacheTtlReadOnlyModeSelfTest;
import org.apache.ignite.internal.processors.cache.ttl.CacheTtlTransactionalLocalSelfTest;
import org.apache.ignite.internal.processors.cache.ttl.CacheTtlTransactionalPartitionedSelfTest;
import org.apache.ignite.internal.processors.client.IgniteDataStreamerTest;
import org.apache.ignite.internal.processors.query.h2.database.H2ComputeInlineSizeTest;
import org.apache.ignite.internal.processors.query.h2.database.inlinecolumn.InlineIndexColumnTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Cache tests using indexing.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    InlineIndexColumnTest.class,
    H2ComputeInlineSizeTest.class,

    GridIndexingWithNoopSwapSelfTest.class,
    GridCacheOffHeapSelfTest.class,

    CacheTtlTransactionalLocalSelfTest.class,
    CacheTtlTransactionalPartitionedSelfTest.class,
    CacheTtlAtomicLocalSelfTest.class,
    CacheTtlAtomicPartitionedSelfTest.class,
    CacheTtlReadOnlyModeSelfTest.class,
    CacheSizeTtlTest.class,

    GridCacheOffheapIndexGetSelfTest.class,
    GridCacheOffheapIndexEntryEvictTest.class,
    CacheIndexStreamerTest.class,

    CacheConfigurationP2PTest.class,

    IgniteCacheConfigurationPrimitiveTypesSelfTest.class,
    IgniteClientReconnectQueriesTest.class,
    CacheRandomOperationsMultithreadedTest.class,
    IgniteCacheStarvationOnRebalanceTest.class,
    CacheOperationsWithExpirationTest.class,
    CacheBinaryKeyConcurrentQueryTest.class,
    CacheQueryFilterExpiredTest.class,

    ClientReconnectAfterClusterRestartTest.class,

    CacheQueryAfterDynamicCacheStartFailureTest.class,

    CacheRegisterMetadataLocallyTest.class,

    IgniteCacheGroupsSqlTest.class,

    IgniteDataStreamerTest.class,

    BinaryTypeMismatchLoggingTest.class,

    BinaryTypeRegistrationTest.class,

    ClusterReadOnlyModeSqlTest.class,
    GridCacheSqlDdlClusterReadOnlyModeTest.class,

    ClusterReadOnlyModeDoesNotBreakSqlSelectTest.class,

    CacheGroupMetricsWithIndexTest.class,
    CacheGroupMetricsWithIndexBuildFailTest.class,

    RebuildIndexLogMessageTest.class,

    H2TreeCorruptedTreeExceptionTest.class,

    WrongIndexedTypesTest.class,

    IndexPagesMetricsInMemoryTest.class,

    EnumClassImplementingIndexedInterfaceTest.class,

    IndexCorruptionRebuildTest.class,

    PartitionReconciliationTombstonesWithIndicesTest.class
})
public class IgniteCacheWithIndexingTestSuite {
}
