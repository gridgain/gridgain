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
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheInvalidStateException;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
public class CacheDetectLostPartitionsTest extends GridCommonAbstractTest {
    /** */
    private static final String TEST_CACHE_NAME = "testcache";

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * Test detect lost partitions on a client node when the cache init after partitions was lost.
     * @throws Exception
     */
    @Test
    public void testDetectLostPartitionsOnClient() throws Exception {
        IgniteEx ig = startGrids(2);

        awaitPartitionMapExchange();

        IgniteCache<Object, Object> cache1 = ig.createCache(getCacheConfig(TEST_CACHE_NAME + 1));

        IgniteCache<Object, Object> cache2 = ig.createCache(getCacheConfig(TEST_CACHE_NAME + 2));

        for (int i = 0; i < 1000; i++) {
            cache1.put(i, i);

            cache2.put(i, i);
        }

        IgniteEx client = startClientGrid(2);

        stopGrid(1);

        cache1 = client.cache(TEST_CACHE_NAME + 1);
        checkCache(cache1);

        cache2 = client.cache(TEST_CACHE_NAME + 2);
        checkCache(cache2);

        cache1.close();
        cache2.close();

        checkCache(client.cache(TEST_CACHE_NAME + 1));
        checkCache(client.cache(TEST_CACHE_NAME + 2));
    }

    /**
     * Test detect lost partitions on a client node when the cache was closed before partitions was lost.
     * @throws Exception
     */
    @Test
    public void testDetectLostPartitionsOnClientWithClosedCache() throws Exception {
        IgniteEx ig = startGrids(2);

        awaitPartitionMapExchange();

        IgniteCache<Object, Object> cacheSrv = ig.createCache(getCacheConfig(TEST_CACHE_NAME));

        for (int i = 0; i < 1000; i++)
            cacheSrv.put(i, i);

        IgniteEx client = startClientGrid(2);

        IgniteCache<Object, Object> cacheCl = client.cache(TEST_CACHE_NAME);

        cacheCl.close();

        stopGrid(1);

        cacheCl = client.cache(TEST_CACHE_NAME);

        checkCache(cacheCl);
    }

    /**
     * Test detect lost partitions on a server node which doesn't have partitions when the cache was closed
     * before partitions was lost.
     * @throws Exception
     */
    @Test
    public void testDetectLostPartitionsOnServerWithClosedCache() throws Exception {
        startGrids(3);

        awaitPartitionMapExchange();

        IgniteCache<Object, Object> cacheSrv1 = grid(1).createCache(
                getCacheConfig(TEST_CACHE_NAME)
                    .setNodeFilter(new NodeConsistentIdFilter(grid(2).localNode().consistentId()))
        );

        for (int i = 0; i < 1000; i++)
            cacheSrv1.put(i, i);

        IgniteEx ig2 = grid(2);

        IgniteCache<Object, Object> cacheSrv2 = ig2.cache(TEST_CACHE_NAME);

        cacheSrv2.close();

        stopGrid(1);

        cacheSrv2 = ig2.cache(TEST_CACHE_NAME);

        checkCache(cacheSrv2);
    }

    /** */
    private CacheConfiguration<Object, Object> getCacheConfig(String cacheName) {
        return new CacheConfiguration<>(cacheName)
                .setPartitionLossPolicy(PartitionLossPolicy.READ_WRITE_SAFE);
    }

    /** */
    private void checkCache(IgniteCache<Object, Object> cache) {
        assertFalse(cache.lostPartitions().isEmpty());

        GridTestUtils.assertThrows(null, () -> {
                    for (int i = 0; i < 1000; i++)
                        cache.get(i);
                },
                CacheInvalidStateException.class, "partition data has been lost");

        GridTestUtils.assertThrows(null, () -> {
                    for (int i = 0; i < 1000; i++)
                        cache.put(i, i);
                },
                CacheInvalidStateException.class, "partition data has been lost");
    }

    /** Filter by consistent id. */
    private static class NodeConsistentIdFilter implements IgnitePredicate<ClusterNode> {
        /** */
        private final Object consistentId;

        /**
         * @param consistentId Consistent id where cache should be started.
         */
        NodeConsistentIdFilter(Object consistentId) {
            this.consistentId = consistentId;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode node) {
            return !node.consistentId().equals(consistentId);
        }
    }
}
