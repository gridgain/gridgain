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

package org.apache.ignite.internal.processors.cache;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import org.apache.ignite.DataRegionMetrics;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.AbstractFailureHandler;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.internal.mem.IgniteOutOfMemoryException;
import org.apache.ignite.internal.pagemem.impl.PageMemoryNoStoreImpl;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.configuration.DataPageEvictionMode.DISABLED;

/**
 * Tests behavior of IgniteCache when {@link IgniteOutOfMemoryException} is thrown.
 */
public class CacheIgniteOutOfMemoryExceptionTest extends GridCommonAbstractTest {
    /** Minimal region size. */
    private static final long DATA_REGION_SIZE = 10L * U.MB;

    /** Huge data region. */
    private static final long HUGE_DATA_REGION_SIZE = U.GB;

    /** Region name. */
    private static final String HUGE_DATA_REGION_NAME = "hugeRegion";

    /** Page size. */
    private static final long PAGE_SIZE = 4 * 1024;

    /** */
    private static final int ATTEMPTS_NUM = 3;

    /** Node failure occurs. */
    private static final AtomicBoolean failure = new AtomicBoolean(false);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setPageSize(4096)
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setMaxSize(DATA_REGION_SIZE)
                    .setPageEvictionMode(DISABLED)
                    .setPersistenceEnabled(false)
                    .setMetricsEnabled(true))
            .setDataRegionConfigurations(
                new DataRegionConfiguration()
                    .setName(HUGE_DATA_REGION_NAME)
                    .setMaxSize(HUGE_DATA_REGION_SIZE)
                    .setPersistenceEnabled(false)
                    .setMetricsEnabled(true)));

        cfg.setFailureHandler(new AbstractFailureHandler() {
            /** {@inheritDoc} */
            @Override protected boolean handle(Ignite ignite, FailureContext failureCtx) {
                failure.set(true);

                // Do not invalidate a node context.
                return false;
            }
        });

        cfg.setCacheConfiguration(
            cacheConfiguration("test-atomic", ATOMIC, true),
            cacheConfiguration("test-tx", TRANSACTIONAL, true));

        return cfg;
    }

    /**
     * Creates a new cache configuration with the given cache atomicity mode.
     *
     * @param mode Cache atomicity mode.
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration(String cacheName, CacheAtomicityMode mode, boolean defaultDataRegion) {
        return new CacheConfiguration(cacheName)
            .setAtomicityMode(mode)
            .setAffinity(new RendezvousAffinityFunction(false, 32))
            .setBackups(1)
            .setReadFromBackup(false)
            .setDataRegionName(defaultDataRegion ? null : HUGE_DATA_REGION_NAME)
            .setEagerTtl(false);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLoadAndClearAtomicCache() throws Exception {
        loadAndClearCache("test-atomic", ATTEMPTS_NUM);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLoadAndClearTransactionalCache() throws Exception {
        loadAndClearCache("test-tx", ATTEMPTS_NUM);
    }

    /**
     * Creates a new cache with the given atomicity node and tries to load & clear it in a loop.
     * It is assumed that {@link IgniteOutOfMemoryException} is thrown during loading the cache,
     * however {@link IgniteCache#clear()} should return the cache to the operable state.
     *
     * @param cacheName Cache name.
     * @param attempts Number of attempts to load and clear the cache.
     */
    private void loadAndClearCache(String cacheName, int attempts) {
        IgniteCache<Object, Object> cache = grid(0).cache(cacheName);

        for (int i = 0; i < attempts; ++i) {
            try {
                for (int key = 0; key < 500_000; ++key)
                    cache.put(key, new byte[4000]);

                fail("OutOfMemoryException hasn't been thrown");
            }
            catch (Exception e) {
                assertTrue(
                    "Exception has been thrown, but the exception type is unexpected [exc=" + e + ']',
                    X.hasCause(e, IgniteOutOfMemoryException.class));

                assertTrue("Failure handler should be called due to IOOM.", failure.get());
            }

            // Let's check that the cache can be cleared without any errors.
            failure.set(false);

            try {
                cache.clear();
            }
            catch (Exception e) {
                fail("Clearing the cache should not trigger any exception [exc=" + e + ']');
            }

            assertFalse("Failure handler should not be called during clearing the cache.", failure.get());
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testHugeEntry() throws Exception {
        // The maximum number of pages that can be allocated.
        long maxPages = ((DATA_REGION_SIZE / (PAGE_SIZE + PageMemoryNoStoreImpl.PAGE_OVERHEAD)));

        // The number of pages that can be hypothetically allocated by user excluding overhead.
        long possibleAvailablePages = maxPages - getDefaultRegionMetrics().getTotalUsedPages() - 42;

        IgniteCache<Object, Object> cache = grid(0).cache("test-atomic");

        try {
            grid(0).cache("test-atomic").put(0, new byte[(int)(possibleAvailablePages * PAGE_SIZE)]);

            fail("The implementation should reserve at least 256 pages for internal needs " +
                    "[maxPages=" + maxPages + ", totalUsed=" + getDefaultRegionMetrics().getTotalUsedPages() + ']');
        }
        catch (Exception e) {
            assertTrue(
                "Exception has been thrown, but the exception type is unexpected [exc=" + e + ']',
                X.hasCause(e, IgniteOutOfMemoryException.class));

            assertTrue("Failure handler should be called due to IOOM.", failure.get());
        }

        // Let's check that the cache can be cleared without any errors.
        failure.set(false);

        try {
            cache.clear();
        }
        catch (Exception e) {
            fail("Clearing the cache should not trigger any exception [exc=" + e + ']');
        }

        assertFalse("Failure handler should not be called during clearing the cache.", failure.get());
    }

    /**
     * Tests that contains operation does not require loading the whole entry.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testContains() throws Exception {
        testFastLocalGet(
            grid(0).getOrCreateCache(cacheConfiguration("test-atomic-huge", ATOMIC, false)),
            null);
    }

    /**
     * Tests that contains operation does not require loading the whole entry.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testContainsWithExpiryPolicy() throws Exception {
        testFastLocalGet(
            grid(0).getOrCreateCache(cacheConfiguration("test-atomic-huge", ATOMIC, false)),
            new CreatedExpiryPolicy(new Duration(TimeUnit.MINUTES, 1)));
    }

    /**
     * Tests that contains operation does not require loading the whole entry.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testTxContains() throws Exception {
        testFastLocalGet(
            grid(0).getOrCreateCache(cacheConfiguration("test-tx-huge", TRANSACTIONAL, false)),
            null);
    }

    /**
     * Tests that contains operation does not require loading the whole entry.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testTxContainsWithExpiryPolicy() throws Exception {
        testFastLocalGet(
            grid(0).getOrCreateCache(cacheConfiguration("test-tx-huge", TRANSACTIONAL, false)),
            new CreatedExpiryPolicy(new Duration(TimeUnit.MINUTES, 1)));
    }

    /**
     * @param cache Cache name.
     * @param expiryPolicy Expiry policy.
     */
    private void testFastLocalGet(IgniteCache<Integer, Object> cache, ExpiryPolicy expiryPolicy) {
        try {
            testContains(cache, primaryKey(cache), expiryPolicy);
        }
        finally {
            cache.destroy();
        }
    }

    /**
     * Tests that contains operation does not require loading the whole entry.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testContainsFromBackup() throws Exception {
        testContainsFromBackup0(
            grid(0).getOrCreateCache(cacheConfiguration("test-atomic-huge", ATOMIC, false)),
            null);
    }

    @Test
    public void testTxContainsFromBackup() throws Exception {
        testContainsFromBackup0(
            grid(0).getOrCreateCache(cacheConfiguration("test-tx-huge", TRANSACTIONAL, false)),
            null);
    }

    public void testContainsFromBackup0(IgniteCache<Integer, Object> cache, ExpiryPolicy expiryPolicy) throws Exception {
        startGrid(1);

        try {
            testContains(cache, backupKey(cache), expiryPolicy);
        }
        finally {
            cache.destroy();

            stopGrid(1);
        }
    }

    private void testContains(IgniteCache<Integer, Object> cache, Integer key, ExpiryPolicy expiryPolicy) {
        Runtime.getRuntime().gc();

        int blobSize = (int) (512 * U.MB);

        if (expiryPolicy != null)
            cache = cache.withExpiryPolicy(expiryPolicy);

        cache.put(key, new byte[blobSize]);

        long usedMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        long totalFreeMemory = Runtime.getRuntime().maxMemory() - usedMemory;

        // Let's occupy all free memory.
        List<Object> unused = new ArrayList<>();
        while (blobSize < totalFreeMemory) {
            try {
                unused.add(new byte[(int) (512 * U.MB)]);
            }
            catch (OutOfMemoryError e) {
                // We don't have enough space to allocate a new continous block.
                break;
            }

            usedMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
            totalFreeMemory = Runtime.getRuntime().maxMemory() - usedMemory;
        }

        assertTrue(cache.containsKey(key));
    }

    /**
     * @return DataRegionMetrics for the default data region.
     */
    private DataRegionMetrics getDefaultRegionMetrics() {
        return grid(0).dataRegionMetrics().stream().filter(d -> d.getName().equals("default")).findFirst().get();
    }
}
