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

import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.testframework.GridTestUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Topology validator test suite.
 */
public class IgniteTopologyValidatorTestSuite {
    /**
     * @param ignoredTests Ignored tests.
     * @return Topology validator tests suite.
     */
    public static List<Class<?>> suite(Collection<Class> ignoredTests) {
        List<Class<?>> suite = new ArrayList<>();

        GridTestUtils.addTestIfNeeded(suite, IgniteTopologyValidatorNearPartitionedAtomicCacheTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteTopologyValidatorNearPartitionedTxCacheTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteTopologyValidatorPartitionedAtomicCacheTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteTopologyValidatorPartitionedTxCacheTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteTopologyValidatorReplicatedAtomicCacheTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteTopologyValidatorReplicatedTxCacheTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, IgniteTopologyValidatorNearPartitionedAtomicCacheGroupsTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteTopologyValidatorNearPartitionedTxCacheGroupsTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteTopologyValidatorPartitionedAtomicCacheGroupsTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteTopologyValidatorPartitionedTxCacheGroupsTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteTopologyValidatorReplicatedAtomicCacheGroupsTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteTopologyValidatorReplicatedTxCacheGroupsTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, IgniteTopologyValidatorGridSplitCacheTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteTopologyValidatorGridSplitCacheWithCacheGroupTest.class, ignoredTests);

        return suite;
    }
}
