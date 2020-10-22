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

import org.apache.ignite.events.IgniteControlUtilityBaselineEventsLocalTest;
import org.apache.ignite.events.IgniteControlUtilityBaselineEventsRemoteTest;
import org.apache.ignite.internal.commandline.CommandHandlerParsingTest;
import org.apache.ignite.internal.commandline.indexreader.IgniteIndexReaderTest;
import org.apache.ignite.internal.commandline.walconverter.IgniteWalConverterArgumentsTest;
import org.apache.ignite.internal.commandline.walconverter.IgniteWalConverterSensitiveDataTest;
import org.apache.ignite.internal.commandline.walconverter.IgniteWalConverterTest;
import org.apache.ignite.internal.processors.security.GridCommandHandlerSslWithSecurityTest;
import org.apache.ignite.util.GridCommandHandlerBrokenIndexTest;
import org.apache.ignite.util.GridCommandHandlerCheckIndexesInlineSizeTest;
import org.apache.ignite.util.GridCommandHandlerClusterByClassTest;
import org.apache.ignite.util.GridCommandHandlerClusterByClassWithSSLTest;
import org.apache.ignite.util.GridCommandHandlerGetCacheSizeTest;
import org.apache.ignite.util.GridCommandHandlerIndexForceRebuildTest;
import org.apache.ignite.util.GridCommandHandlerIndexListTest;
import org.apache.ignite.util.GridCommandHandlerIndexRebuildStatusTest;
import org.apache.ignite.util.GridCommandHandlerIndexingCheckSizeTest;
import org.apache.ignite.util.GridCommandHandlerIndexingClusterByClassTest;
import org.apache.ignite.util.GridCommandHandlerIndexingClusterByClassWithSSLTest;
import org.apache.ignite.util.GridCommandHandlerIndexingTest;
import org.apache.ignite.util.GridCommandHandlerIndexingWithSSLTest;
import org.apache.ignite.util.GridCommandHandlerInterruptCommandTest;
import org.apache.ignite.util.GridCommandHandlerMetadataTest;
import org.apache.ignite.util.GridCommandHandlerPartitionReconciliationAtomicPersistentTest;
import org.apache.ignite.util.GridCommandHandlerPartitionReconciliationAtomicTest;
import org.apache.ignite.util.GridCommandHandlerPartitionReconciliationCommonTest;
import org.apache.ignite.util.GridCommandHandlerPartitionReconciliationExtendedTest;
import org.apache.ignite.util.GridCommandHandlerPartitionReconciliationReplicatedPersistentTest;
import org.apache.ignite.util.GridCommandHandlerPartitionReconciliationReplicatedTest;
import org.apache.ignite.util.GridCommandHandlerPartitionReconciliationTxPersistentTest;
import org.apache.ignite.util.GridCommandHandlerPartitionReconciliationTxTest;
import org.apache.ignite.util.GridCommandHandlerPropertiesTest;
import org.apache.ignite.util.GridCommandHandlerRUTest;
import org.apache.ignite.util.GridCommandHandlerSslTest;
import org.apache.ignite.util.GridCommandHandlerTest;
import org.apache.ignite.util.GridCommandHandlerTracingConfigurationTest;
import org.apache.ignite.util.GridCommandHandlerWithSSLTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Test suite for control utility.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    CommandHandlerParsingTest.class,

    // Tests for tracing configuration
    GridCommandHandlerTracingConfigurationTest.class,

    GridCommandHandlerTest.class,
    GridCommandHandlerWithSSLTest.class,
    GridCommandHandlerClusterByClassTest.class,
    GridCommandHandlerClusterByClassWithSSLTest.class,
    GridCommandHandlerSslTest.class,
    GridCommandHandlerRUTest.class,

    GridCommandHandlerPartitionReconciliationAtomicPersistentTest.class,
    GridCommandHandlerPartitionReconciliationAtomicTest.class,
    GridCommandHandlerPartitionReconciliationCommonTest.class,
    GridCommandHandlerPartitionReconciliationExtendedTest.class,
    GridCommandHandlerPartitionReconciliationReplicatedPersistentTest.class,
    GridCommandHandlerPartitionReconciliationReplicatedTest.class,
    GridCommandHandlerPartitionReconciliationTxPersistentTest.class,
    GridCommandHandlerPartitionReconciliationTxTest.class,

    GridCommandHandlerSslWithSecurityTest.class,

    IgniteControlUtilityBaselineEventsLocalTest.class,
    IgniteControlUtilityBaselineEventsRemoteTest.class,

    GridCommandHandlerBrokenIndexTest.class,
    GridCommandHandlerIndexingTest.class,
    GridCommandHandlerIndexingWithSSLTest.class,
    GridCommandHandlerIndexingClusterByClassTest.class,
    GridCommandHandlerIndexingClusterByClassWithSSLTest.class,
    GridCommandHandlerIndexingCheckSizeTest.class,
    GridCommandHandlerIndexForceRebuildTest.class,
    GridCommandHandlerIndexListTest.class,
    GridCommandHandlerIndexRebuildStatusTest.class,
    GridCommandHandlerCheckIndexesInlineSizeTest.class,
    GridCommandHandlerInterruptCommandTest.class,
    GridCommandHandlerMetadataTest.class,
    GridCommandHandlerGetCacheSizeTest.class,

    GridCommandHandlerPropertiesTest.class,

    IgniteIndexReaderTest.class,
    IgniteWalConverterTest.class,
    IgniteWalConverterArgumentsTest.class,
    IgniteWalConverterSensitiveDataTest.class
})
public class IgniteControlUtilityTestSuite {
}
