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

import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsBinaryMetadataAsyncWritingTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsBinarySortObjectFieldsTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsMarshallerMappingRestoreOnNodeStartTest;
import org.apache.ignite.internal.processors.cache.persistence.RestorePartitionStateDuringCheckpointTest;
import org.apache.ignite.internal.processors.cache.persistence.db.IgniteCacheGroupsWithRestartsTest;
import org.apache.ignite.internal.processors.cache.persistence.db.IgniteLogicalRecoveryEncryptionTest;
import org.apache.ignite.internal.processors.cache.persistence.db.IgniteLogicalRecoveryTest;
import org.apache.ignite.internal.processors.cache.persistence.db.IgniteLogicalRecoveryWithParamsTest;
import org.apache.ignite.internal.processors.cache.persistence.db.IgniteSequentialNodeCrashRecoveryTest;
import org.apache.ignite.internal.processors.cache.persistence.db.file.IgnitePdsThreadInterruptionRandomAccessWalTest;
import org.apache.ignite.internal.processors.cache.persistence.db.file.IgnitePdsThreadInterruptionTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Test suite for tests that cover core PDS features and depend on indexing module.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({


    //

    IgnitePdsBinaryMetadataAsyncWritingTest.class,
    IgnitePdsMarshallerMappingRestoreOnNodeStartTest.class,
    IgnitePdsThreadInterruptionTest.class,
    IgnitePdsThreadInterruptionRandomAccessWalTest.class,

    IgnitePdsBinarySortObjectFieldsTest.class,

    IgniteLogicalRecoveryTest.class,
    IgniteLogicalRecoveryWithParamsTest.class,
    IgniteLogicalRecoveryEncryptionTest.class,

    IgniteSequentialNodeCrashRecoveryTest.class,

    IgniteCacheGroupsWithRestartsTest.class,
    RestorePartitionStateDuringCheckpointTest.class
})
public class IgnitePdsWithIndexingCoreTestSuite2 {
}
