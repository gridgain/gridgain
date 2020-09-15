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

package org.apache.ignite.internal.processors.cache.distributed;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheInvalidStateException;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
public class CacheDetectLostPartitionsTest extends GridCommonAbstractTest {
    /** */
    private static final String TEST_CACHE_NAME = "testcache";

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /**
     * Test detect lost partitions on a client node when the cache init after partitions was lost.
     * @throws Exception
     */
    @Test
    public void testDetectLostPartitionedOnClient() throws Exception {
        IgniteEx ig = startGrids(2);

        awaitPartitionMapExchange();

        IgniteCache<Object, Object> cache = ig.getOrCreateCache(
            new CacheConfiguration<>(TEST_CACHE_NAME)
                .setPartitionLossPolicy(PartitionLossPolicy.READ_WRITE_SAFE)
        );

        for (int i = 0; i < 1000; i++)
            cache.put(i, i);

        IgniteEx client = startClientGrid(2);

        stopGrid(1);

        IgniteCache<Object, Object> cacheCl = client.cache(TEST_CACHE_NAME);

        assertFalse(cacheCl.lostPartitions().isEmpty());

        GridTestUtils.assertThrows(null, () -> cacheCl.get(1),
            CacheInvalidStateException.class, "partition data has been lost");

        GridTestUtils.assertThrows(null, () -> cacheCl.put(1, 1),
            CacheInvalidStateException.class, "partition data has been lost");
    }
}
