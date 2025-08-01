/*
 * Copyright 2025 GridGain Systems, Inc. and Contributors.
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
import org.apache.ignite.internal.processors.cache.ActiveOnStartPropertyTest;
import org.apache.ignite.internal.processors.cache.AutoActivationPropertyTest;
import org.apache.ignite.internal.processors.cache.ClusterStateOnStartPropertyTest;
import org.apache.ignite.internal.processors.cache.IgniteClusterActivateDeactivateTestWithPersistence;
import org.apache.ignite.internal.processors.cache.IgnitePdsDataRegionMetricsTxTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsCacheConfigurationFileConsistencyCheckTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsDestroyCacheTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsDestroyCacheWithoutCheckpointsTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsDynamicCacheTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsSporadicDataRecordsOnBackupTest;
import org.apache.ignite.internal.processors.cache.persistence.db.IgnitePdsCacheRestoreTest;
import org.apache.ignite.internal.processors.cache.persistence.db.IgnitePdsDataRegionMetricsTest;
import org.apache.ignite.internal.processors.cache.persistence.db.IgnitePdsWithTtlExpirationOnDeactivateTest;
import org.apache.ignite.internal.processors.cache.persistence.db.IgnitePdsWithTtlTest;
import org.apache.ignite.internal.processors.cache.persistence.db.IgnitePdsWithTtlTest2;
import org.apache.ignite.internal.processors.cache.persistence.db.file.DefaultPageSizeBackwardsCompatibilityTest;
import org.apache.ignite.internal.processors.cache.persistence.db.file.IgniteClockPageReplacementTest;
import org.apache.ignite.internal.processors.cache.persistence.db.file.IgnitePdsCheckpointSimpleTest;
import org.apache.ignite.internal.processors.cache.persistence.db.file.IgnitePdsCheckpointSimulationWithRealCpDisabledTest;
import org.apache.ignite.internal.processors.cache.persistence.db.file.IgnitePdsPageReplacementTest;
import org.apache.ignite.internal.processors.cache.persistence.db.file.IgniteRandomLruPageReplacementTest;
import org.apache.ignite.internal.processors.cache.persistence.db.file.IgniteSegmentLruPageReplacementTest;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.IgniteMetaStorageBasicTest;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedConfigurationPersistentTest;
import org.apache.ignite.internal.processors.database.IgniteDbDynamicCacheSelfTest;
import org.apache.ignite.internal.processors.database.IgniteDbMultiNodePutGetTest;
import org.apache.ignite.internal.processors.database.IgniteDbPutGetWithCacheStoreTest;
import org.apache.ignite.internal.processors.database.IgniteDbSingleNodePutGetTest;
import org.apache.ignite.internal.processors.database.IgniteDbSingleNodeTinyPutGetTest;
import org.apache.ignite.internal.processors.diagnostic.DiagnosticProcessorTest;
import org.apache.ignite.internal.processors.localtask.DurableBackgroundTasksProcessorSelfTest;
import org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStoragePersistentTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.DynamicSuite;
import org.junit.runner.RunWith;

/** */
@RunWith(DynamicSuite.class)
public class IgnitePdsTestSuite {
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

        addRealPageStoreTests(suite, ignoredTests);
        addRealPageStoreTestsLongRunning(suite, ignoredTests);

        return suite;
    }

    /**
     * Fills {@code suite} with PDS test subset, which operates with real page store, but requires long time to
     * execute.
     *
     * @param suite suite to add tests into.
     * @param ignoredTests Ignored tests.
     */
    private static void addRealPageStoreTestsLongRunning(List<Class<?>> suite, Collection<Class> ignoredTests) {
        // Basic PageMemory tests.
        GridTestUtils.addTestIfNeeded(suite, IgnitePdsPageReplacementTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, IgniteClockPageReplacementTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteRandomLruPageReplacementTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteSegmentLruPageReplacementTest.class, ignoredTests);
    }

    /**
     * Fills {@code suite} with PDS test subset, which operates with real page store and does actual disk operations.
     *
     * NOTE: These tests are also executed using I/O plugins.
     *
     * @param suite suite to add tests into.
     * @param ignoredTests Ignored tests.
     */
    public static void addRealPageStoreTests(List<Class<?>> suite, Collection<Class> ignoredTests) {

        // Checkpointing smoke-test.
        GridTestUtils.addTestIfNeeded(suite, IgnitePdsCheckpointSimulationWithRealCpDisabledTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgnitePdsCheckpointSimpleTest.class, ignoredTests);
        //GridTestUtils.addTestIfNeeded(suite, IgnitePersistenceSequentialCheckpointTest.class, ignoredTests);

        // Basic API tests.
        GridTestUtils.addTestIfNeeded(suite, IgniteDbSingleNodePutGetTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteDbMultiNodePutGetTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteDbSingleNodeTinyPutGetTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteDbDynamicCacheSelfTest.class, ignoredTests);

        // Persistence-enabled.
        GridTestUtils.addTestIfNeeded(suite, IgnitePdsDynamicCacheTest.class, ignoredTests);
        // TODO uncomment when https://issues.apache.org/jira/browse/IGNITE-7510 is fixed
        // GridTestUtils.addTestIfNeeded(suite, IgnitePdsClientNearCachePutGetTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteDbPutGetWithCacheStoreTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgnitePdsWithTtlTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgnitePdsWithTtlTest2.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgnitePdsWithTtlExpirationOnDeactivateTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgnitePdsSporadicDataRecordsOnBackupTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, IgniteClusterActivateDeactivateTestWithPersistence.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, IgnitePdsCacheRestoreTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgnitePdsDataRegionMetricsTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgnitePdsDataRegionMetricsTxTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, IgnitePdsDestroyCacheTest.class, ignoredTests);
        //GridTestUtils.addTestIfNeeded(suite, IgnitePdsRemoveDuringRebalancingTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgnitePdsDestroyCacheWithoutCheckpointsTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgnitePdsCacheConfigurationFileConsistencyCheckTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, DefaultPageSizeBackwardsCompatibilityTest.class, ignoredTests);

        //MetaStorage
        GridTestUtils.addTestIfNeeded(suite, IgniteMetaStorageBasicTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, DistributedMetaStoragePersistentTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, DistributedConfigurationPersistentTest.class, ignoredTests);

        //Diagnostic
        GridTestUtils.addTestIfNeeded(suite, DiagnosticProcessorTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, ActiveOnStartPropertyTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AutoActivationPropertyTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, ClusterStateOnStartPropertyTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, DurableBackgroundTasksProcessorSelfTest.class, ignoredTests);
    }
}
