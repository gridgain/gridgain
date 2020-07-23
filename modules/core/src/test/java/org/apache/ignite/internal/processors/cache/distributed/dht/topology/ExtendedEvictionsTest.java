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

package org.apache.ignite.internal.processors.cache.distributed.dht.topology;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Test;

@WithSystemProperty(key = "IGNITE_PRELOAD_RESEND_TIMEOUT", value = "0")
public class ExtendedEvictionsTest extends AbstractPartitionClearingTest {
    /**
     * Stopping the cache during clearing should be no-op, all partitions are expected to be cleared.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCacheGroupRestartDuringEviction_1() throws Exception {
        testOperationDuringEviction(false, 1, new Runnable() {
            @Override public void run() {
                doSleep(1000); // Wait a bit to give some time for partition state message to process on crd.

                grid(0).cache(DEFAULT_CACHE_NAME).close();
            }
        });
    }

    @Test
    public void testCacheGroupRestartDuringEviction_2() throws Exception {
        AtomicReference<IgniteInternalFuture> ref = new AtomicReference<>();

        testOperationDuringEviction(false, 1, new Runnable() {
            @Override public void run() {
                doSleep(1000); // Wait a bit to give some time for partition state message to process on crd.

                IgniteInternalFuture fut = GridTestUtils.runAsync(new Runnable() {
                    @Override public void run() {
                        grid(0).destroyCache(DEFAULT_CACHE_NAME);
                    }
                });

                doSleep(500);

                assertFalse(fut.isDone()); // Cache stop should be blocked by concurrent eviction.

                ref.set(fut);
            }
        });

        assertTrue(ref.get().isDone());

        PartitionsEvictManager mgr = grid(0).context().cache().context().evict();

        Map evictionGroupsMap = U.field(mgr, "evictionGroupsMap");

        assertEquals(1, evictionGroupsMap.size());

        IgniteCache<Object, Object> cache = grid(0).getOrCreateCache(cacheConfiguration());

        assertEquals(0, evictionGroupsMap.size());
    }

    public void testNodeStopDuringEviction() {

    }

    public void testCacheStopDuringEvictionMultiCacheGroup() {

    }

//    public void testPartitionMapRefreshAfterAllEvicted() {
//
//    }
//
//    public void testPartitionMapNotSendAfterAllClearedBeforeRebalancing() {
//
//    }
//
//    public void testConcurrentEvictionSamePartition() {
//
//    }
//
//    public void testEvictOneThreadInSysPool() {
//
//    }
}
