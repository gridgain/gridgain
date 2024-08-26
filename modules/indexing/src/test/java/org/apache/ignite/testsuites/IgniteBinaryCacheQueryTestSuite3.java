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

import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.metric.SystemViewSelfTest;
import org.apache.ignite.internal.processors.cache.BigEntryQueryTest;
import org.apache.ignite.internal.processors.cache.BinaryMetadataConcurrentUpdateWithIndexesTest;
import org.apache.ignite.internal.processors.cache.BinarySerializationQuerySelfTest;
import org.apache.ignite.internal.processors.cache.BinarySerializationQueryWithReflectiveSerializerSelfTest;
import org.apache.ignite.internal.processors.cache.CacheIteratorScanQueryTest;
import org.apache.ignite.internal.processors.cache.CacheLocalQueryDetailMetricsSelfTest;
import org.apache.ignite.internal.processors.cache.CacheLocalQueryMetricsSelfTest;
import org.apache.ignite.internal.processors.cache.CacheOffheapBatchIndexingSingleTypeTest;
import org.apache.ignite.internal.processors.cache.CachePartitionedQueryDetailMetricsDistributedSelfTest;
import org.apache.ignite.internal.processors.cache.CachePartitionedQueryDetailMetricsLocalSelfTest;
import org.apache.ignite.internal.processors.cache.CachePartitionedQueryMetricsDistributedSelfTest;
import org.apache.ignite.internal.processors.cache.CachePartitionedQueryMetricsLocalSelfTest;
import org.apache.ignite.internal.processors.cache.CacheQueryEvictDataLostTest;
import org.apache.ignite.internal.processors.cache.CacheQueryNewClientSelfTest;
import org.apache.ignite.internal.processors.cache.CacheReplicatedQueryDetailMetricsDistributedSelfTest;
import org.apache.ignite.internal.processors.cache.CacheReplicatedQueryDetailMetricsLocalSelfTest;
import org.apache.ignite.internal.processors.cache.CacheReplicatedQueryMetricsDistributedSelfTest;
import org.apache.ignite.internal.processors.cache.CacheReplicatedQueryMetricsLocalSelfTest;
import org.apache.ignite.internal.processors.cache.CacheSqlQueryValueCopySelfTest;
import org.apache.ignite.internal.processors.cache.CheckIndexesInlineSizeOnNodeJoinMultiJvmTest;
import org.apache.ignite.internal.processors.cache.GridCacheDynamicLoadOnClientPersistentTest;
import org.apache.ignite.internal.processors.cache.GridCacheDynamicLoadOnClientTest;
import org.apache.ignite.internal.processors.cache.GridCacheFullTextQueryMultithreadedSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheFullTextQuerySelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheQueryIndexingDisabledSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheQuerySqlFieldInlineSizeSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheBinaryObjectsScanSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheBinaryObjectsScanWithEventsSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheDistributedJoinCollocatedAndNotTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheDistributedJoinCustomAffinityMapper;
import org.apache.ignite.internal.processors.cache.IgniteCacheDistributedJoinNoIndexTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheDistributedJoinPartitionedAndReplicatedTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheDistributedJoinQueryConditionsTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheDistributedJoinTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheFieldsQueryNoDataSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheFullTextQueryNodeJoiningSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheNoClassQuerySelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheP2pUnmarshallingQueryErrorTest;
import org.apache.ignite.internal.processors.cache.IgniteCheckClusterStateBeforeExecuteQueryTest;
import org.apache.ignite.internal.processors.cache.IndexingCachePartitionLossPolicySelfTest;
import org.apache.ignite.internal.processors.cache.QueryEntityCaseMismatchTest;
import org.apache.ignite.internal.processors.cache.ReservationsOnDoneAfterTopologyUnlockFailTest;
import org.apache.ignite.internal.processors.cache.SqlFieldsQuerySelfTest;
import org.apache.ignite.internal.processors.cache.authentication.SqlUserCommandSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheAtomicFieldsQuerySelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheAtomicNearEnabledFieldsQuerySelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheDistributedPartitionQueryConfigurationSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheDistributedPartitionQueryNodeRestartsSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheDistributedPartitionQuerySelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheDistributedQueryCancelSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCachePartitionedFieldsQueryP2PEnabledSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCachePartitionedFieldsQuerySelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.IgniteCacheReplicatedFieldsQueryJoinNoPrimaryPartitionsSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.IgniteCacheReplicatedFieldsQueryP2PEnabledSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.IgniteCacheReplicatedFieldsQueryROSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.IgniteCacheReplicatedFieldsQuerySelfTest;
import org.apache.ignite.internal.processors.cache.encryption.EncryptedSqlTableTest;
import org.apache.ignite.internal.processors.cache.encryption.EncryptedSqlTemplateTableTest;
import org.apache.ignite.internal.processors.cache.index.BasicJavaTypesIndexTest;
import org.apache.ignite.internal.processors.cache.index.BasicSqlTypesIndexTest;
import org.apache.ignite.internal.processors.cache.index.H2ConnectionLeaksSelfTest;
import org.apache.ignite.internal.processors.cache.index.H2RowCachePageEvictionTest;
import org.apache.ignite.internal.processors.cache.index.H2RowCacheSelfTest;
import org.apache.ignite.internal.processors.cache.index.H2RowExpireTimeIndexSelfTest;
import org.apache.ignite.internal.processors.cache.index.IgniteDecimalSelfTest;
import org.apache.ignite.internal.processors.cache.index.IndexColumnTypeMismatchTest;
import org.apache.ignite.internal.processors.cache.index.LongIndexNameTest;
import org.apache.ignite.internal.processors.cache.index.OptimizedMarshallerIndexNameTest;
import org.apache.ignite.internal.processors.cache.index.PojoIndexLocalQueryTest;
import org.apache.ignite.internal.processors.cache.index.SqlPartitionEvictionTest;
import org.apache.ignite.internal.processors.cache.index.SqlTransactionCommandsWithMvccDisabledSelfTest;
import org.apache.ignite.internal.processors.cache.local.IgniteCacheLocalFieldsQuerySelfTest;
import org.apache.ignite.internal.processors.cache.local.IgniteCacheLocalQueryCancelOrTimeoutSelfTest;
import org.apache.ignite.internal.processors.cache.metric.SqlViewExporterSpiTest;
import org.apache.ignite.internal.processors.cache.query.IgniteCacheQueryCacheDestroySelfTest;
import org.apache.ignite.internal.processors.cache.query.ScanQueryConcurrentSqlUpdatesTest;
import org.apache.ignite.internal.processors.cache.query.ScanQueryConcurrentUpdatesTest;
import org.apache.ignite.internal.processors.query.IgniteQueryConvertibleTypesValidationTest;
import org.apache.ignite.internal.processors.query.IgniteQueryDedicatedPoolTest;
import org.apache.ignite.internal.processors.query.IgniteSqlCustomAggregationTest;
import org.apache.ignite.internal.processors.query.IgniteSqlCustomFunctionTest;
import org.apache.ignite.internal.processors.query.IgniteSqlCustomSchemaTest;
import org.apache.ignite.internal.processors.query.IgniteSqlCustomSchemaWithPdsEnabled;
import org.apache.ignite.internal.processors.query.IgniteSqlDefaultSchemaTest;
import org.apache.ignite.internal.processors.query.IgniteSqlDefaultValueTest;
import org.apache.ignite.internal.processors.query.IgniteSqlDistributedJoin2SelfTest;
import org.apache.ignite.internal.processors.query.IgniteSqlDistributedJoinSelfTest;
import org.apache.ignite.internal.processors.query.IgniteSqlEntryCacheModeAgnosticTest;
import org.apache.ignite.internal.processors.query.IgniteSqlGroupConcatCollocatedTest;
import org.apache.ignite.internal.processors.query.IgniteSqlGroupConcatNotCollocatedTest;
import org.apache.ignite.internal.processors.query.IgniteSqlKeyValueFieldsTest;
import org.apache.ignite.internal.processors.query.IgniteSqlNotNullConstraintTest;
import org.apache.ignite.internal.processors.query.IgniteSqlParameterizedQueryTest;
import org.apache.ignite.internal.processors.query.IgniteSqlQueryParallelismTest;
import org.apache.ignite.internal.processors.query.IgniteSqlRoutingTest;
import org.apache.ignite.internal.processors.query.IgniteSqlSchemaNameValidationTest;
import org.apache.ignite.internal.processors.query.IgniteSqlSchemasDiffConfigurationsTest;
import org.apache.ignite.internal.processors.query.JdbcSqlCustomSchemaTest;
import org.apache.ignite.internal.processors.query.JdbcSqlDefaultSchemaTest;
import org.apache.ignite.internal.processors.query.KillQueryErrorOnCancelTest;
import org.apache.ignite.internal.processors.query.KillQueryFromClientTest;
import org.apache.ignite.internal.processors.query.KillQueryFromNeighbourTest;
import org.apache.ignite.internal.processors.query.KillQueryOnClientDisconnectTest;
import org.apache.ignite.internal.processors.query.KillQueryTest;
import org.apache.ignite.internal.processors.query.QueryJmxMetricsTest;
import org.apache.ignite.internal.processors.query.SqlIncompatibleDataTypeExceptionTest;
import org.apache.ignite.internal.processors.query.SqlIndexesSystemViewStaticCfgTest;
import org.apache.ignite.internal.processors.query.SqlIndexesSystemViewTest;
import org.apache.ignite.internal.processors.query.SqlMetricsOnWebConsoleSelfTest;
import org.apache.ignite.internal.processors.query.SqlQueryHistoryFromClientSelfTest;
import org.apache.ignite.internal.processors.query.SqlQueryHistorySelfTest;
import org.apache.ignite.internal.processors.query.SqlQuerySystemViewsIntegrationTest;
import org.apache.ignite.internal.processors.query.SqlQuerySystemViewsSelfTest;
import org.apache.ignite.internal.processors.query.SqlSystemViewsSelfTest;
import org.apache.ignite.internal.processors.query.WrongQueryEntityFieldTypeTest;
import org.apache.ignite.internal.processors.query.h2.GridIndexRebuildSelfTest;
import org.apache.ignite.internal.processors.query.h2.GridIndexRebuildTest;
import org.apache.ignite.internal.processors.query.h2.H2ColumnTypeConversionCheckSelfTest;
import org.apache.ignite.internal.processors.query.h2.QueryParserMetricsHolderSelfTest;
import org.apache.ignite.internal.processors.query.h2.sql.BaseH2CompareQueryTest;
import org.apache.ignite.internal.processors.query.h2.sql.H2CompareBigQueryDistributedJoinsTest;
import org.apache.ignite.internal.processors.query.h2.sql.H2CompareBigQueryTest;
import org.apache.ignite.internal.processors.query.h2.sql.ParameterTypeInferenceTest;
import org.apache.ignite.internal.processors.query.h2.twostep.AndOperationExtractPartitionSelfTest;
import org.apache.ignite.internal.processors.query.h2.twostep.BetweenOperationExtractPartitionSelfTest;
import org.apache.ignite.internal.processors.query.h2.twostep.DmlSelectPartitionPruningSelfTest;
import org.apache.ignite.internal.processors.query.h2.twostep.InOperationExtractPartitionSelfTest;
import org.apache.ignite.internal.processors.query.h2.twostep.JoinPartitionPruningSelfTest;
import org.apache.ignite.internal.processors.query.h2.twostep.MvccDmlPartitionPruningSelfTest;
import org.apache.ignite.internal.processors.query.h2.twostep.SqlDataTypeConversionTest;
import org.apache.ignite.internal.processors.sql.IgniteCachePartitionedAtomicColumnConstraintsTest;
import org.apache.ignite.internal.processors.sql.IgniteCachePartitionedTransactionalColumnConstraintsTest;
import org.apache.ignite.internal.processors.sql.IgniteCachePartitionedTransactionalSnapshotColumnConstraintTest;
import org.apache.ignite.internal.processors.sql.IgniteCacheReplicatedAtomicColumnConstraintsTest;
import org.apache.ignite.internal.processors.sql.IgniteCacheReplicatedTransactionalColumnConstraintsTest;
import org.apache.ignite.internal.processors.sql.IgniteCacheReplicatedTransactionalSnapshotColumnConstraintTest;
import org.apache.ignite.internal.processors.sql.IgniteSQLColumnConstraintsTest;
import org.apache.ignite.internal.processors.sql.IgniteTransactionSQLColumnConstraintTest;
import org.apache.ignite.internal.sql.SqlParserUserSelfTest;
import org.apache.ignite.spi.communication.tcp.GridOrderedMessageCancelSelfTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Test suite for cache queries.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    // Fields queries.
    SqlFieldsQuerySelfTest.class,
    IgniteCacheLocalFieldsQuerySelfTest.class,
    IgniteCacheReplicatedFieldsQuerySelfTest.class,
    IgniteCacheReplicatedFieldsQueryROSelfTest.class,
    IgniteCacheReplicatedFieldsQueryP2PEnabledSelfTest.class,
    IgniteCacheReplicatedFieldsQueryJoinNoPrimaryPartitionsSelfTest.class,
    IgniteCachePartitionedFieldsQuerySelfTest.class,
    IgniteCacheAtomicFieldsQuerySelfTest.class,
    IgniteCacheAtomicNearEnabledFieldsQuerySelfTest.class,
    IgniteCachePartitionedFieldsQueryP2PEnabledSelfTest.class,
    IgniteCacheFieldsQueryNoDataSelfTest.class,
    GridCacheQueryIndexingDisabledSelfTest.class,
    GridOrderedMessageCancelSelfTest.class,
    CacheQueryEvictDataLostTest.class,

    // Full text queries.
    GridCacheFullTextQuerySelfTest.class,
    GridCacheFullTextQueryMultithreadedSelfTest.class,
    IgniteCacheFullTextQueryNodeJoiningSelfTest.class,

    // Ignite cache and H2 comparison.
    BaseH2CompareQueryTest.class,
    H2CompareBigQueryTest.class,
    H2CompareBigQueryDistributedJoinsTest.class,
    IndexColumnTypeMismatchTest.class,

    // Cache query metrics.
    CacheLocalQueryMetricsSelfTest.class,
    CachePartitionedQueryMetricsDistributedSelfTest.class,
    CachePartitionedQueryMetricsLocalSelfTest.class,
    CacheReplicatedQueryMetricsDistributedSelfTest.class,
    CacheReplicatedQueryMetricsLocalSelfTest.class,
    QueryParserMetricsHolderSelfTest.class,

    // Cache query metrics.
    CacheLocalQueryDetailMetricsSelfTest.class,
    CachePartitionedQueryDetailMetricsDistributedSelfTest.class,
    CachePartitionedQueryDetailMetricsLocalSelfTest.class,
    CacheReplicatedQueryDetailMetricsDistributedSelfTest.class,
    CacheReplicatedQueryDetailMetricsLocalSelfTest.class,
    QueryJmxMetricsTest.class,

    // Unmarshalling query test.
    IgniteCacheP2pUnmarshallingQueryErrorTest.class,
    IgniteCacheNoClassQuerySelfTest.class,

    // Cancellation.
    IgniteCacheDistributedQueryCancelSelfTest.class,
    IgniteCacheLocalQueryCancelOrTimeoutSelfTest.class,

    // Distributed joins.
    H2CompareBigQueryDistributedJoinsTest.class,
    IgniteCacheDistributedJoinCollocatedAndNotTest.class,
    IgniteCacheDistributedJoinCustomAffinityMapper.class,
    IgniteCacheDistributedJoinNoIndexTest.class,
    IgniteCacheDistributedJoinPartitionedAndReplicatedTest.class,
    IgniteCacheDistributedJoinQueryConditionsTest.class,
    IgniteCacheDistributedJoinTest.class,
    IgniteSqlDistributedJoinSelfTest.class,
    IgniteSqlDistributedJoin2SelfTest.class,
    IgniteSqlQueryParallelismTest.class,

    // Other.
    CacheIteratorScanQueryTest.class,
    CacheQueryNewClientSelfTest.class,
    CacheOffheapBatchIndexingSingleTypeTest.class,
    CacheSqlQueryValueCopySelfTest.class,
    IgniteCacheQueryCacheDestroySelfTest.class,
    IgniteQueryDedicatedPoolTest.class,
    IgniteSqlEntryCacheModeAgnosticTest.class,
    QueryEntityCaseMismatchTest.class,
    IgniteCacheDistributedPartitionQuerySelfTest.class,
    IgniteCacheDistributedPartitionQueryNodeRestartsSelfTest.class,
    IgniteCacheDistributedPartitionQueryConfigurationSelfTest.class,
    IgniteSqlKeyValueFieldsTest.class,
    IgniteSqlRoutingTest.class,
    IgniteSqlNotNullConstraintTest.class,
    LongIndexNameTest.class,
    GridCacheQuerySqlFieldInlineSizeSelfTest.class,
    IgniteSqlParameterizedQueryTest.class,
    H2ConnectionLeaksSelfTest.class,
    IgniteCheckClusterStateBeforeExecuteQueryTest.class,
    OptimizedMarshallerIndexNameTest.class,
    SqlSystemViewsSelfTest.class,
    SqlQuerySystemViewsSelfTest.class,
    SqlQuerySystemViewsIntegrationTest.class,
    SqlIndexesSystemViewTest.class,
    SqlIndexesSystemViewStaticCfgTest.class,
    SqlMetricsOnWebConsoleSelfTest.class,
    ScanQueryConcurrentUpdatesTest.class,
    ScanQueryConcurrentSqlUpdatesTest.class,
    ReservationsOnDoneAfterTopologyUnlockFailTest.class,

    GridIndexRebuildSelfTest.class,
    GridIndexRebuildTest.class,
    CheckIndexesInlineSizeOnNodeJoinMultiJvmTest.class,

    SqlTransactionCommandsWithMvccDisabledSelfTest.class,

    IgniteSqlDefaultValueTest.class,
    IgniteDecimalSelfTest.class,
    IgniteSQLColumnConstraintsTest.class,
    IgniteTransactionSQLColumnConstraintTest.class,

    IgniteSqlDefaultSchemaTest.class,
    IgniteSqlCustomSchemaTest.class,
    JdbcSqlDefaultSchemaTest.class,
    JdbcSqlCustomSchemaTest.class,
    IgniteSqlSchemaNameValidationTest.class,
    IgniteSqlCustomSchemaWithPdsEnabled.class,
    IgniteSqlSchemasDiffConfigurationsTest.class,

    IgniteCachePartitionedAtomicColumnConstraintsTest.class,
    IgniteCachePartitionedTransactionalColumnConstraintsTest.class,
    IgniteCachePartitionedTransactionalSnapshotColumnConstraintTest.class,
    IgniteCacheReplicatedAtomicColumnConstraintsTest.class,
    IgniteCacheReplicatedTransactionalColumnConstraintsTest.class,
    IgniteCacheReplicatedTransactionalSnapshotColumnConstraintTest.class,

    WrongQueryEntityFieldTypeTest.class,

    // H2 Rows on-heap cache
    H2RowCacheSelfTest.class,
    H2RowCachePageEvictionTest.class,
    H2RowExpireTimeIndexSelfTest.class,

    // User operation SQL
    SqlParserUserSelfTest.class,
    SqlUserCommandSelfTest.class,
    EncryptedSqlTableTest.class,
    EncryptedSqlTemplateTableTest.class,

    // Partition loss.
    IndexingCachePartitionLossPolicySelfTest.class,

    // Partitions eviction
    SqlPartitionEvictionTest.class,

    // GROUP_CONCAT
    IgniteSqlGroupConcatCollocatedTest.class,
    IgniteSqlGroupConcatNotCollocatedTest.class,

    // Custom aggregations functions
    IgniteSqlCustomAggregationTest.class,

    // Custom sql functions
    IgniteSqlCustomFunctionTest.class,

    // Binary
    BinarySerializationQuerySelfTest.class,
    BinarySerializationQueryWithReflectiveSerializerSelfTest.class,
    IgniteCacheBinaryObjectsScanSelfTest.class,
    IgniteCacheBinaryObjectsScanWithEventsSelfTest.class,
    BigEntryQueryTest.class,
    BinaryMetadataConcurrentUpdateWithIndexesTest.class,

    // Partition pruning.
    InOperationExtractPartitionSelfTest.class,
    AndOperationExtractPartitionSelfTest.class,
    BetweenOperationExtractPartitionSelfTest.class,
    JoinPartitionPruningSelfTest.class,
    DmlSelectPartitionPruningSelfTest.class,
    MvccDmlPartitionPruningSelfTest.class,

    GridCacheDynamicLoadOnClientTest.class,
    GridCacheDynamicLoadOnClientPersistentTest.class,

    SqlDataTypeConversionTest.class,
    ParameterTypeInferenceTest.class,

    //Query history.
    SqlQueryHistorySelfTest.class,
    SqlQueryHistoryFromClientSelfTest.class,

    SqlIncompatibleDataTypeExceptionTest.class,

    BasicSqlTypesIndexTest.class,
    BasicJavaTypesIndexTest.class,
    PojoIndexLocalQueryTest.class,

    //Cancellation of queries.
    KillQueryTest.class,
    KillQueryFromNeighbourTest.class,
    KillQueryFromClientTest.class,
    KillQueryOnClientDisconnectTest.class,
    KillQueryErrorOnCancelTest.class,

    IgniteQueryConvertibleTypesValidationTest.class,
    H2ColumnTypeConversionCheckSelfTest.class,

    IgniteStatisticsTestSuite.class,

    SqlViewExporterSpiTest.class,
    SystemViewSelfTest.class,
})
public class IgniteBinaryCacheQueryTestSuite3 {
    /** Setup lazy mode default. */
    @BeforeClass
    public static void setupLazy() {
        GridTestUtils.setFieldValue(SqlFieldsQuery.class, "DFLT_LAZY", false);
    }
}
