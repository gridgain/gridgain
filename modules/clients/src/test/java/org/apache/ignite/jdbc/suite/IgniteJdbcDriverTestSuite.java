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

package org.apache.ignite.jdbc.suite;

import java.security.Security;
import org.apache.ignite.jdbc.thin.JdbcThinConnectionSSLTest;
import org.apache.ignite.jdbc.thin.JdbcThinConnectionTimeoutSelfTest;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * JDBC driver test suite.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
//    QaJdbcTestSuite.class,
//
//    JdbcThinStatementTimeoutInternalsTest.class,
//
//    RunningQueryInfoCheckInitiatorTest.class,
//
//    JdbcConnectionSelfTest.class,
//    JdbcStatementSelfTest.class,
//    JdbcPreparedStatementSelfTest.class,
//    JdbcResultSetSelfTest.class,
//    JdbcComplexQuerySelfTest.class,
//    JdbcMetadataSelfTest.class,
//    JdbcEmptyCacheSelfTest.class,
//    JdbcLocalCachesSelfTest.class,
//    JdbcNoDefaultCacheTest.class,
//    JdbcDefaultNoOpCacheTest.class,
//    JdbcPojoQuerySelfTest.class,
//    JdbcPojoLegacyQuerySelfTest.class,
//    JdbcConnectionReopenTest.class,
//
//    // Ignite client node based driver tests
//    org.apache.ignite.internal.jdbc2.JdbcCursorLeaksTest.class,
//    org.apache.ignite.internal.jdbc2.JdbcConnectionSelfTest.class,
//    org.apache.ignite.internal.jdbc2.JdbcSpringSelfTest.class,
//    org.apache.ignite.internal.jdbc2.JdbcStatementSelfTest.class,
//    org.apache.ignite.internal.jdbc2.JdbcPreparedStatementSelfTest.class,
//    org.apache.ignite.internal.jdbc2.JdbcResultSetSelfTest.class,
//    org.apache.ignite.internal.jdbc2.JdbcComplexQuerySelfTest.class,
//    JdbcDistributedJoinsQueryTest.class,
//    org.apache.ignite.internal.jdbc2.JdbcMetadataSelfTest.class,
//    org.apache.ignite.internal.jdbc2.JdbcEmptyCacheSelfTest.class,
//    org.apache.ignite.internal.jdbc2.JdbcLocalCachesSelfTest.class,
//    org.apache.ignite.internal.jdbc2.JdbcConnectionWithoutCacheNameTest.class,
//    org.apache.ignite.internal.jdbc2.JdbcMergeStatementSelfTest.class,
//    org.apache.ignite.internal.jdbc2.JdbcBinaryMarshallerMergeStatementSelfTest.class,
//    org.apache.ignite.internal.jdbc2.JdbcUpdateStatementSelfTest.class,
//    org.apache.ignite.internal.jdbc2.JdbcInsertStatementSelfTest.class,
//    org.apache.ignite.internal.jdbc2.JdbcBinaryMarshallerInsertStatementSelfTest.class,
//    org.apache.ignite.internal.jdbc2.JdbcDeleteStatementSelfTest.class,
//    org.apache.ignite.internal.jdbc2.JdbcStatementBatchingSelfTest.class,
//    org.apache.ignite.internal.jdbc2.JdbcErrorsSelfTest.class,
//    org.apache.ignite.internal.jdbc2.JdbcStreamingToPublicCacheTest.class,
//    org.apache.ignite.internal.jdbc2.JdbcNoCacheStreamingSelfTest.class,
//    JdbcBulkLoadSelfTest.class,
//    JdbcSchemaCaseSelfTest.class,
//
//    JdbcBlobTest.class,
//    org.apache.ignite.internal.jdbc2.JdbcStreamingSelfTest.class,
//    JdbcThinStreamingNotOrderedSelfTest.class,
//    JdbcThinStreamingOrderedSelfTest.class,
//    JdbcThinDataPageScanPropertySelfTest.class,
//    JdbcThinStreamingResetStreamTest.class,
//    JdbcQueryMemoryTrackerSelfTest.class,
//    JdbcQueryQuotaTest.class,
//
//    // DDL tests.
//    org.apache.ignite.internal.jdbc2.JdbcDynamicIndexAtomicPartitionedNearSelfTest.class,
//    org.apache.ignite.internal.jdbc2.JdbcDynamicIndexAtomicPartitionedSelfTest.class,
//    org.apache.ignite.internal.jdbc2.JdbcDynamicIndexAtomicReplicatedSelfTest.class,
//    org.apache.ignite.internal.jdbc2.JdbcDynamicIndexTransactionalPartitionedNearSelfTest.class,
//    org.apache.ignite.internal.jdbc2.JdbcDynamicIndexTransactionalPartitionedSelfTest.class,
//    org.apache.ignite.internal.jdbc2.JdbcDynamicIndexTransactionalReplicatedSelfTest.class,
//
//    // New thin JDBC
//    JdbcThinConnectionSelfTest.class,
//    JdbcThinConnectionMvccEnabledSelfTest.class,
//    JdbcThinConnectionMultipleAddressesTest.class,
//    JdbcThinTcpIoTest.class,
//    JdbcThinConnectionAdditionalSecurityTest.class,
//    JdbcThinConnectionSSLTest.class,
//    JdbcThinDataSourceSelfTest.class,
//    JdbcThinPreparedStatementSelfTest.class,
//    JdbcThinResultSetSelfTest.class,
//    JdbcThinConnectionPropertiesTest.class,
//
//    JdbcThinStatementSelfTest.class,
//    JdbcThinComplexQuerySelfTest.class,
//    JdbcThinDistributedJoinsSelfTest.class,
//    JdbcThinNoDefaultSchemaTest.class,
//    JdbcThinSchemaCaseSelfTest.class,
//    JdbcThinEmptyCacheSelfTest.class,
//    JdbcThinMetadataSelfTest.class,
//    JdbcThinMetadataPrimaryKeysSelfTest.class,
//    JdbcThinMetadataSqlMatchTest.class,
//    JdbcThinErrorsSelfTest.class,
//    JdbcThinStatementCancelSelfTest.class,
//    JdbcThinStatementTimeoutSelfTest.class,
    JdbcThinConnectionTimeoutSelfTest.class,
    JdbcThinConnectionTimeoutSelfTest.class,
    JdbcThinConnectionTimeoutSelfTest.class,
    JdbcThinConnectionTimeoutSelfTest.class,
    JdbcThinConnectionTimeoutSelfTest.class,
    JdbcThinConnectionTimeoutSelfTest.class,
    JdbcThinConnectionTimeoutSelfTest.class,
    JdbcThinConnectionTimeoutSelfTest.class,
    JdbcThinConnectionTimeoutSelfTest.class,
    JdbcThinConnectionTimeoutSelfTest.class,

    JdbcThinConnectionTimeoutSelfTest.class,
    JdbcThinConnectionTimeoutSelfTest.class,
    JdbcThinConnectionTimeoutSelfTest.class,
    JdbcThinConnectionTimeoutSelfTest.class,
    JdbcThinConnectionTimeoutSelfTest.class,
    JdbcThinConnectionTimeoutSelfTest.class,
    JdbcThinConnectionTimeoutSelfTest.class,
    JdbcThinConnectionTimeoutSelfTest.class,
    JdbcThinConnectionTimeoutSelfTest.class,
    JdbcThinConnectionTimeoutSelfTest.class,

    JdbcThinConnectionTimeoutSelfTest.class,
    JdbcThinConnectionTimeoutSelfTest.class,
    JdbcThinConnectionTimeoutSelfTest.class,
    JdbcThinConnectionTimeoutSelfTest.class,
    JdbcThinConnectionTimeoutSelfTest.class,
    JdbcThinConnectionTimeoutSelfTest.class,
    JdbcThinConnectionTimeoutSelfTest.class,
    JdbcThinConnectionTimeoutSelfTest.class,
    JdbcThinConnectionTimeoutSelfTest.class,
    JdbcThinConnectionTimeoutSelfTest.class,
//    JdbcThinDefaultTimeoutTest.class,
//
//    JdbcThinSystemSchemaDurabilityTest.class,
//    JdbcThinSystemSchemaImplicitSchemaNamePropTest.class,
//    JdbcThinSystemSchemaExplicitSchemaNamePropTrueTest.class,
//    JdbcThinSystemSchemaExplicitSchemaNamePropFalseTest.class,
//
//    JdbcThinInsertStatementSelfTest.class,
//    JdbcThinUpdateStatementSelfTest.class,
//    JdbcThinMergeStatementSelfTest.class,
//    JdbcThinDeleteStatementSelfTest.class,
//    JdbcThinAutoCloseServerCursorTest.class,
//    JdbcThinBatchSelfTest.class,
//    JdbcThinMissingLongArrayResultsTest.class,
//
//    // New thin JDBC driver, DDL tests
//    JdbcThinDynamicIndexAtomicPartitionedNearSelfTest.class,
//    JdbcThinDynamicIndexAtomicPartitionedSelfTest.class,
//    JdbcThinDynamicIndexAtomicReplicatedSelfTest.class,
//    JdbcThinDynamicIndexTransactionalPartitionedNearSelfTest.class,
//    JdbcThinDynamicIndexTransactionalPartitionedSelfTest.class,
//    JdbcThinDynamicIndexTransactionalReplicatedSelfTest.class,
//    JdbcThinMultiStatementSelfTest.class,
//
//    // New thin JDBC driver, DML tests
//    JdbcThinBulkLoadSelfTest.class,
//
//    // New thin JDBC driver, full SQL tests
//    JdbcThinComplexDmlDdlSelfTest.class,
//
//    JdbcThinSelectAfterAlterTable.class,
//
//    // Update on server
//    JdbcThinInsertStatementSkipReducerOnUpdateSelfTest.class,
//    JdbcThinUpdateStatementSkipReducerOnUpdateSelfTest.class,
//    JdbcThinMergeStatementSkipReducerOnUpdateSelfTest.class,
//    JdbcThinComplexDmlDdlSkipReducerOnUpdateSelfTest.class,
//    JdbcThinComplexDmlDdlCustomSchemaSelfTest.class,
//
//    // Transactions
//    JdbcThinTransactionsSelfTest.class,
//    JdbcThinTransactionsClientAutoCommitComplexSelfTest.class,
//    JdbcThinTransactionsServerAutoCommitComplexSelfTest.class,
//    JdbcThinTransactionsClientNoAutoCommitComplexSelfTest.class,
//    JdbcThinTransactionsServerNoAutoCommitComplexSelfTest.class,
//
//    JdbcThinLocalQueriesSelfTest.class,
//    // FH tests.
//    JdbcThinDatabaseFailureTest.class,
//
//    // Various commands.
//    JdbcThinWalModeChangeSelfTest.class,
//    JdbcThinAuthenticateConnectionSelfTest.class,
//
//    JdbcThinPreparedStatementLeakTest.class,
//    JdbcThinTransactionsLeaksMvccTest.class,
//
//    JdbcThinQueryMemoryTrackerSelfTest.class,
//    JdbcThinDiskSpillingClientsTest.class,
//    JdbcDiskSpillingClientsTest.class,
//
//    // Data types coverage.
//    JdbcThinCacheToJdbcDataTypesCoverageTest.class,
//    JdbcThinJdbcToCacheDataTypesCoverageTest.class
})
public class IgniteJdbcDriverTestSuite {
    /**
     * Enable NULL algorithm and keep 3DES_EDE_CBC disabled.
     * See {@link JdbcThinConnectionSSLTest#testDisabledCustomCipher()} for details.
     */
    @BeforeClass
    public static void init() {
        Security.setProperty("jdk.tls.disabledAlgorithms", "3DES_EDE_CBC");
    }
}
