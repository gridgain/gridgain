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

package org.apache.ignite.internal.processors.cache.distributed.near;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.processors.cache.distributed.GridCacheMultiNodeLockAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Test cases for multi-threaded tests.
 */
public class GridCachePartitionedMultiNodeLockSelfTest extends GridCacheMultiNodeLockAbstractTest {
    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration() {
        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(PARTITIONED);
        cc.setBackups(2); // 2 backups, so all nodes are involved.
        cc.setAtomicityMode(TRANSACTIONAL);
        cc.setNearConfiguration(new NearCacheConfiguration());

        return cc;
    }

    /** {@inheritDoc} */
    @Override protected boolean partitioned() {
        return true;
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testBasicLock() throws Exception {
        super.testBasicLock();
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testLockMultithreaded() throws Exception {
        super.testLockMultithreaded();
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testLockReentry() throws IgniteCheckedException {
        super.testLockReentry();
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testMultiNodeLock() throws Exception {
        super.testMultiNodeLock();
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testMultiNodeLockWithKeyLists() throws Exception {
        super.testMultiNodeLockWithKeyLists();
    }
}
