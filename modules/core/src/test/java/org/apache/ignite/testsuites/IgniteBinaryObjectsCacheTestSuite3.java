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
import org.apache.ignite.internal.binary.BinaryClassLoaderMultiJvmTest;
import org.apache.ignite.internal.binary.BinaryClassLoaderTest;
import org.apache.ignite.internal.processors.cache.binary.GridCacheBinaryAtomicEntryProcessorDeploymentSelfTest;
import org.apache.ignite.internal.processors.cache.binary.GridCacheBinaryTransactionalEntryProcessorDeploymentSelfTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.DynamicSuite;
import org.junit.runner.RunWith;

/**
 *  IgniteBinaryObjectsCacheTestSuite3 is kept together with {@link IgniteCacheTestSuite3}
 *  for backward compatibility.
 *
 *  In Ignite 2.0 tests
 *  -  http://ci.ignite.apache.org/viewType.html?buildTypeId=Ignite20Tests_IgniteCache3
 *  IgniteBinaryObjectsCacheTestSuite3 is used,
 *
 *  and in Ignite tests
 *  http://ci.ignite.apache.org/viewType.html?buildTypeId=IgniteTests_IgniteCache3
 *  - IgniteCacheTestSuite3.
 *  And if someone runs old run configs then most test will be executed anyway.
 *
 *  In future this suite may be merged with {@link IgniteCacheTestSuite3}
 *
 */
@RunWith(DynamicSuite.class)
public class IgniteBinaryObjectsCacheTestSuite3 {
    /**
     * @return Test suite.
     */
    public static List<Class<?>> suite() {
        return suite(null);
    }

    /**
     * @param ignoredTests Tests to ignore.
     * @return Test suite.
     */
    public static List<Class<?>> suite(Collection<Class> ignoredTests) {
        GridTestProperties.setProperty(GridTestProperties.ENTRY_PROCESSOR_CLASS_NAME,
            "org.apache.ignite.tests.p2p.CacheDeploymentBinaryEntryProcessor");

        List<Class<?>> suite = new ArrayList<>(IgniteCacheTestSuite3.suite(ignoredTests));

        GridTestUtils.addTestIfNeeded(suite, GridCacheBinaryAtomicEntryProcessorDeploymentSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheBinaryTransactionalEntryProcessorDeploymentSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, BinaryClassLoaderTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, BinaryClassLoaderMultiJvmTest.class, ignoredTests);

        return suite;
    }
}
