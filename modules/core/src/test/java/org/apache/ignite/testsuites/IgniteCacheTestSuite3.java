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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.internal.processors.cache.IgniteCacheGroupsTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.DynamicSuite;
import org.junit.runner.RunWith;

/**
 * Test suite.
 */
@RunWith(DynamicSuite.class)
public class IgniteCacheTestSuite3 {
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

        GridTestUtils.addTestIfNeeded(suite, IgniteCacheGroupsTest.class, ignoredTests);

//        // Value consistency tests.
//        GridTestUtils.addTestIfNeeded(suite, GridCacheValueConsistencyAtomicSelfTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, GridCacheValueConsistencyAtomicNearEnabledSelfTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, GridCacheValueConsistencyTransactionalSelfTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, GridCacheValueConsistencyTransactionalNearEnabledSelfTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, GridCacheValueBytesPreloadingSelfTest.class, ignoredTests);
//
//        // Replicated cache.
//        GridTestUtils.addTestIfNeeded(suite, GridCacheReplicatedBasicApiTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, GridCacheReplicatedBasicOpSelfTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, GridCacheReplicatedBasicStoreSelfTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, GridCacheReplicatedGetAndTransformStoreSelfTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, GridCacheReplicatedAtomicGetAndTransformStoreSelfTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, GridCacheReplicatedEventSelfTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, GridCacheReplicatedEventDisabledSelfTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, GridCacheReplicatedSynchronousCommitTest.class, ignoredTests);
//
//        GridTestUtils.addTestIfNeeded(suite, GridCacheReplicatedLockSelfTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, GridCacheReplicatedMultiNodeLockSelfTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, GridCacheReplicatedMultiNodeSelfTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, GridCacheReplicatedNodeFailureSelfTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, GridCacheReplicatedTxSingleThreadedSelfTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, GridCacheReplicatedTxTimeoutSelfTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, GridCacheReplicatedPreloadSelfTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, GridCacheReplicatedPreloadLifecycleSelfTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, GridCacheSyncReplicatedPreloadSelfTest.class, ignoredTests);
//
//        GridTestUtils.addTestIfNeeded(suite, CacheLockChangingTopologyTest.class, ignoredTests);
//
//        //GridTestUtils.addTestIfNeeded(suite, GridCacheReplicatedMarshallerTxTest.class, ignoredTests);
//        //GridTestUtils.addTestIfNeeded(suite, GridCacheReplicatedOnheapFullApiSelfTest.class, ignoredTests);
//        //GridTestUtils.addTestIfNeeded(suite, GridCacheReplicatedOnheapMultiNodeFullApiSelfTest.class, ignoredTests);
//        //GridTestUtils.addTestIfNeeded(suite, GridCacheReplicatedTxConcurrentGetTest.class, ignoredTests);
//        //GridTestUtils.addTestIfNeeded(suite, GridCacheReplicatedTxReadTest.class, ignoredTests);
//
//        GridTestUtils.addTestIfNeeded(suite, CacheStartupInDeploymentModesTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, GridCacheConditionalDeploymentSelfTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, GridCacheAtomicEntryProcessorDeploymentSelfTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, GridCacheTransactionalEntryProcessorDeploymentSelfTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, IgniteCacheScanPredicateDeploymentSelfTest.class, ignoredTests);
//
//        GridTestUtils.addTestIfNeeded(suite, GridCachePutArrayValueSelfTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, GridCacheReplicatedEvictionEventSelfTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, GridCacheReplicatedTxMultiThreadedSelfTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, GridCacheReplicatedPreloadEventsSelfTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, GridCacheReplicatedPreloadStartStopEventsSelfTest.class, ignoredTests);
//
//        GridTestUtils.addTestIfNeeded(suite, IgniteTxReentryNearSelfTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, IgniteTxReentryColocatedSelfTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, IgniteTxExceptionNodeFailTest.class, ignoredTests);
//
//        // Test for byte array value special case.
//        GridTestUtils.addTestIfNeeded(suite, GridCacheLocalByteArrayValuesSelfTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, GridCacheNearPartitionedP2PEnabledByteArrayValuesSelfTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, GridCacheNearPartitionedP2PDisabledByteArrayValuesSelfTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedOnlyP2PEnabledByteArrayValuesSelfTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedOnlyP2PDisabledByteArrayValuesSelfTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, GridCacheReplicatedP2PEnabledByteArrayValuesSelfTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, GridCacheReplicatedP2PDisabledByteArrayValuesSelfTest.class, ignoredTests);
//
//        // Near-only cache.
//        suite.addAll(IgniteCacheNearOnlySelfTestSuite.suite(ignoredTests));
//
//        // Test cache with daemon nodes.
//        GridTestUtils.addTestIfNeeded(suite, GridCacheDaemonNodeLocalSelfTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, GridCacheDaemonNodePartitionedSelfTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, GridCacheDaemonNodeReplicatedSelfTest.class, ignoredTests);
//
//        // Write-behind.
//        suite.addAll(IgniteCacheWriteBehindTestSuite.suite(ignoredTests));
//
//        // Transform.
//        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedTransformWriteThroughBatchUpdateSelfTest.class, ignoredTests);
//
//        GridTestUtils.addTestIfNeeded(suite, GridCacheEntryVersionSelfTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, GridCacheVersionSelfTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, GridCacheVersionTopologyChangeTest.class, ignoredTests);
//
//        // Memory leak tests.
//        GridTestUtils.addTestIfNeeded(suite, GridCacheReferenceCleanupSelfTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, GridCacheReloadSelfTest.class, ignoredTests);
//
//        GridTestUtils.addTestIfNeeded(suite, GridCacheMixedModeSelfTest.class, ignoredTests);
//
//        // Cache interceptor tests.
//        suite.addAll(IgniteCacheInterceptorSelfTestSuite.suite(ignoredTests));
//
//        GridTestUtils.addTestIfNeeded(suite, IgniteTxGetAfterStopTest.class, ignoredTests);
//
//        GridTestUtils.addTestIfNeeded(suite, CacheAsyncOperationsTest.class, ignoredTests);
//
//        GridTestUtils.addTestIfNeeded(suite, IgniteTxRemoveTimeoutObjectsTest.class, ignoredTests);
//        GridTestUtils.addTestIfNeeded(suite, IgniteTxRemoveTimeoutObjectsNearTest.class, ignoredTests);

        return suite;
    }
}
