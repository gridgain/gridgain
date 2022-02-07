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

import org.apache.ignite.internal.processors.cache.multijvm.GridCacheAtomicClientOnlyMultiJvmFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.multijvm.GridCacheAtomicClientOnlyMultiJvmP2PDisabledFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.multijvm.GridCacheAtomicNearOnlyMultiJvmFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.multijvm.GridCacheAtomicNearOnlyMultiJvmP2PDisabledFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.multijvm.GridCacheAtomicOnheapMultiJvmFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.multijvm.GridCacheNearOnlyMultiJvmFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.multijvm.GridCacheNearOnlyMultiJvmP2PDisabledFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.multijvm.GridCachePartitionedNearDisabledAtomicOnheapMultiJvmFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.multijvm.GridCachePartitionedNearDisabledOnheapMultiJvmFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.multijvm.GridCachePartitionedOnheapMultiJvmFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.multijvm.GridCacheReplicatedNearOnlyMultiJvmFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.multijvm.GridCacheReplicatedOnheapMultiJvmFullApiSelfTest;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Multi-JVM test suite.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    GridCacheNearOnlyMultiJvmFullApiSelfTest.class,
    GridCacheNearOnlyMultiJvmP2PDisabledFullApiSelfTest.class,
    GridCacheReplicatedNearOnlyMultiJvmFullApiSelfTest.class,

    GridCacheAtomicClientOnlyMultiJvmFullApiSelfTest.class,
    GridCacheAtomicClientOnlyMultiJvmP2PDisabledFullApiSelfTest.class,

    GridCacheAtomicNearOnlyMultiJvmFullApiSelfTest.class,
    GridCacheAtomicNearOnlyMultiJvmP2PDisabledFullApiSelfTest.class,

    GridCacheAtomicOnheapMultiJvmFullApiSelfTest.class,
    GridCachePartitionedNearDisabledAtomicOnheapMultiJvmFullApiSelfTest.class,
    GridCachePartitionedNearDisabledOnheapMultiJvmFullApiSelfTest.class,
    GridCachePartitionedOnheapMultiJvmFullApiSelfTest.class,
    GridCacheReplicatedOnheapMultiJvmFullApiSelfTest.class
})
public class IgniteCacheFullApiMultiJvmSelfTestSuite2 {
    /** */
    @BeforeClass
    public static void init() {
        System.setProperty("H2_JDBC_CONNECTIONS", "500");
    }
}
