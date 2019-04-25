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

import org.apache.ignite.internal.processors.cache.IgniteCacheLockPartitionOnAffinityRunAtomicCacheOpTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheLockPartitionOnAffinityRunTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheLockPartitionOnAffinityRunTxCacheOpTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheLockPartitionOnAffinityRunWithCollisionSpiTest;
import org.apache.ignite.internal.processors.database.baseline.IgniteBaselineLockPartitionOnAffinityRunAtomicCacheTest;
import org.apache.ignite.internal.processors.database.baseline.IgniteBaselineLockPartitionOnAffinityRunTxCacheTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Compute and Cache tests for affinityRun/Call. These tests is extracted into separate suite
 * because ones take a lot of time.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    IgniteCacheLockPartitionOnAffinityRunTest.class,
    IgniteCacheLockPartitionOnAffinityRunWithCollisionSpiTest.class,
    IgniteCacheLockPartitionOnAffinityRunAtomicCacheOpTest.class,
    IgniteBaselineLockPartitionOnAffinityRunAtomicCacheTest.class,
    IgniteBaselineLockPartitionOnAffinityRunTxCacheTest.class,
    IgniteCacheLockPartitionOnAffinityRunTxCacheOpTest.class
})
public class IgniteCacheAffinityRunTestSuite {
}
