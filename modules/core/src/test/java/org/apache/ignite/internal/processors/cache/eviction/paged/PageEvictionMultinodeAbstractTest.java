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
package org.apache.ignite.internal.processors.cache.eviction.paged;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Ignore;
import org.junit.Test;

/**
 *
 */
public abstract class PageEvictionMultinodeAbstractTest extends PageEvictionAbstractTest {
    /** Cache modes. */
    private static final CacheMode[] CACHE_MODES = {CacheMode.PARTITIONED/*, CacheMode.REPLICATED*/};

    /** Atomicity modes. */
    private static final CacheAtomicityMode[] ATOMICITY_MODES = {
        CacheAtomicityMode.ATOMIC/*, CacheAtomicityMode.TRANSACTIONAL*/};

    /** Write modes. */
    private static final CacheWriteSynchronizationMode[] WRITE_MODES = {
        CacheWriteSynchronizationMode.PRIMARY_SYNC,
    //    CacheWriteSynchronizationMode.FULL_SYNC,
    //    CacheWriteSynchronizationMode.FULL_ASYNC
    };

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(1, false);

        startGrid("client");
    }

    /**
     * @return Client grid.
     */
    Ignite clientGrid() {
        return grid("client");
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration configuration = super.getConfiguration(gridName);

        if (gridName.startsWith("client"))
            configuration.setClientMode(true);

        return configuration;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 10 * 60 * 1000;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPageEviction() throws Exception {
        for (int i = 0; i < CACHE_MODES.length; i++) {
            for (int j = 0; j < ATOMICITY_MODES.length; j++) {
                for (int k = 0; k < WRITE_MODES.length; k++) {
                    if (i + j + Math.min(k, 1) <= 1) {
                        CacheConfiguration<Object, Object> cfg = cacheConfig(
                            "evict" + i + j + k, null, CACHE_MODES[i], ATOMICITY_MODES[j], WRITE_MODES[k]);

                        createCacheAndTestEviction(cfg);
                    }
                }
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
//    @Ignore("https://issues.apache.org/jira/browse/IGNITE-10738")
//    @Test
    public void testPageEvictionMvcc() throws Exception {
        for (int i = 0; i < CACHE_MODES.length; i++) {
            CacheConfiguration<Object, Object> cfg = cacheConfig(
                "evict" + i, null, CACHE_MODES[i], CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT,
                CacheWriteSynchronizationMode.FULL_SYNC);

            createCacheAndTestEviction(cfg);
        }
    }

    /**
     * @param cfg Config.
     * @throws Exception If failed.
     */
    protected void createCacheAndTestEviction(CacheConfiguration<Object, Object> cfg) throws Exception {
        IgniteCache<Object, Object> cache = grid(0).getOrCreateCache(cfg);

        AtomicInteger entriesToProcess = new AtomicInteger(ENTRIES);

        GridTestUtils.runMultiThreaded(() -> {
            ThreadLocalRandom r = ThreadLocalRandom.current();
            int i = entriesToProcess.decrementAndGet();
            while (i >= 0) {
                cache.put(i, new TestObject(PAGE_SIZE * 2)); // Fits in one page.

                if (i % (ENTRIES / 10) == 0)
                    System.out.println(">>> Entries put: " + i);

                i = entriesToProcess.decrementAndGet();
            }},
            Runtime.getRuntime().availableProcessors() * 4,
            "user-thread");

        int resultingSize = cache.size(CachePeekMode.PRIMARY);

        System.out.println(">>> Resulting size: " + resultingSize);

        // Eviction started, no OutOfMemory occurred, success.
        //assertTrue(resultingSize < ENTRIES * 10 / 11);

        clientGrid().destroyCache(cfg.getName());
    }
}
