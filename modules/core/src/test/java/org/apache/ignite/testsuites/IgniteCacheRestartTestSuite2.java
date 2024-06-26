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

import org.apache.ignite.internal.processors.cache.GridCachePutAllFailoverSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheAtomicPutAllFailoverSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCachePutAllRestartTest;
import org.apache.ignite.internal.processors.cache.IgniteCachePutKeyAttachedBinaryObjectTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheAtomicNodeRestartTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheGetRestartTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.IgniteCacheRecreateTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.IgniteCacheAtomicReplicatedNodeRestartSelfTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Cache stability test suite on changing topology.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    IgniteCacheAtomicNodeRestartTest.class,
    IgniteCacheAtomicReplicatedNodeRestartSelfTest.class,

    IgniteCacheAtomicPutAllFailoverSelfTest.class,
    IgniteCachePutAllRestartTest.class,
    IgniteCachePutKeyAttachedBinaryObjectTest.class,
    GridCachePutAllFailoverSelfTest.class,

    // TODO IGNITE-4768.
    //suite.addTest(new JUnit4TestAdapter(IgniteBinaryMetadataUpdateNodeRestartTest.class,

    IgniteCacheRecreateTest.class,

    IgniteCacheGetRestartTest.class
})
public class IgniteCacheRestartTestSuite2 {
}
