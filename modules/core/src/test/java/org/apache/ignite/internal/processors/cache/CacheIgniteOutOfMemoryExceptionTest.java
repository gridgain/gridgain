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

import java.util.concurrent.TimeUnit;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.mem.IgniteOutOfMemoryException;
import org.apache.ignite.internal.pagemem.impl.PageMemoryNoStoreImpl;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.LOCAL;

/**
 * Tests behavior of IgniteCache when {@link IgniteOutOfMemoryException} is thrown.
 */
@WithSystemProperty(key = "IGNITE_TTL_EXPIRE_BATCH_SIZE", value = "0") // Disable implicit clearing on cache op.
@WithSystemProperty(key = "CLEANUP_WORKER_SLEEP_INTERVAL", value = "100000000") // Disable background cleanup.
@WithSystemProperty(key = "IGNITE_UNWIND_THROTTLING_TIMEOUT", value = "0") // Disable unwind throttling.
public class CacheIgniteOutOfMemoryExceptionTest extends AbstractCacheIgniteOutOfMemoryExceptionTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLoadAndClearAtomicCache() throws Exception {
        loadAndClearCache(ATOMIC, ATTEMPTS_NUM);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLoadAndClearTransactionalCache() throws Exception {
        loadAndClearCache(TRANSACTIONAL, ATTEMPTS_NUM);
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

        IgniteCache<Object, Object> cache = grid(0).cache(ATOMIC.name());

        try {
            grid(0).cache(ATOMIC.name()).put(0, new byte[(int)(possibleAvailablePages * PAGE_SIZE)]);

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
     */
    @Test
    public void testContainsKeyLocal() {
        IgniteCache<Integer, Object> cache = grid(0)
            .getOrCreateCache(cacheConfiguration(ATOMIC, HUGE_ATOMIC_CACHE_NAME, HUGE_DATA_REGION_NAME).setCacheMode(LOCAL));

        testContains(cache, primaryKeys(cache, 1, 0), true);
    }

    /**
     * Tests that contains operation does not require loading the whole entry.
     */
    @Test
    public void testContainsKeyLocalWithExpiryPolicy() {
        CacheConfiguration<Integer, Object> ccfg = cacheConfiguration(ATOMIC, HUGE_ATOMIC_CACHE_NAME, HUGE_DATA_REGION_NAME)
            .setCacheMode(LOCAL)
            .setEagerTtl(false)
            .setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(new Duration(TimeUnit.MINUTES, 1)));

        IgniteCache<Integer, Object> cache = grid(0).getOrCreateCache(ccfg);

        testContains(cache, primaryKeys(cache, 1, 0), true);

        cache.destroy();

        ccfg = cacheConfiguration(ATOMIC, HUGE_ATOMIC_CACHE_NAME, HUGE_DATA_REGION_NAME)
            .setCacheMode(LOCAL)
            .setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(new Duration(TimeUnit.MILLISECONDS, 10)))
            .setEagerTtl(false);

        cache = grid(0).getOrCreateCache(ccfg);

        testContains(cache, primaryKeys(cache, 1, 0), false);
    }

    /**
     * Tests that contains operation does not require loading the whole entry.
     */
    @Test
    public void testContainsKey() {
        IgniteCache<Integer, Object> cache = grid(0)
            .getOrCreateCache(cacheConfiguration(ATOMIC, HUGE_ATOMIC_CACHE_NAME, HUGE_DATA_REGION_NAME));

        testContains(cache, primaryKeys(cache, 1, 0), true);
    }

    /**
     * Tests that contains operation does not require loading the whole entry.
     */
    @Test
    public void testContainsKeys() {
        IgniteCache<Integer, Object> cache = grid(0)
            .getOrCreateCache(cacheConfiguration(ATOMIC, HUGE_ATOMIC_CACHE_NAME, HUGE_DATA_REGION_NAME));

        testContains(cache, primaryKeys(cache, 3, 0), true);
    }

    /**
     * Tests that contains operation does not require loading the whole entry.
     */
    @Test
    public void testContainsKeyWithExpiryPolicy() {
        CacheConfiguration<Integer, Object> ccfg = cacheConfiguration(ATOMIC, HUGE_ATOMIC_CACHE_NAME, HUGE_DATA_REGION_NAME)
            .setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(new Duration(TimeUnit.MINUTES, 1)))
            .setEagerTtl(false);

        IgniteCache<Integer, Object> cache = grid(0).getOrCreateCache(ccfg);

        testContains(cache, primaryKeys(cache, 1, 0), true);

        cache.destroy();

        ccfg = cacheConfiguration(ATOMIC, HUGE_ATOMIC_CACHE_NAME, HUGE_DATA_REGION_NAME)
            .setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(new Duration(TimeUnit.MILLISECONDS, 10)))
            .setEagerTtl(false);

        cache = grid(0).getOrCreateCache(ccfg);

        testContains(cache, primaryKeys(cache, 1, 0), false);
    }

    /**
     * Tests that contains operation does not require loading the whole entry.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testContainsKeyReadFromBackupDisabled() {
        CacheConfiguration<Integer, Object> ccfg = cacheConfiguration(ATOMIC, HUGE_ATOMIC_CACHE_NAME, HUGE_DATA_REGION_NAME)
            .setReadFromBackup(false)
            .setBackups(1);

        IgniteCache<Integer, Object> cache = grid(0).getOrCreateCache(ccfg);

        testContains(cache, primaryKeys(cache, 1, 0), true);
    }

    /**
     * Tests that contains operation does not require loading the whole entry.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testContainsKeyTwoNodes() throws Exception {
        CacheConfiguration<Integer, Object> ccfg = cacheConfiguration(ATOMIC, HUGE_ATOMIC_CACHE_NAME, HUGE_DATA_REGION_NAME)
            .setReadFromBackup(false)
            .setBackups(1);

        IgniteCache<Integer, Object> cache = grid(0).getOrCreateCache(ccfg);

        testContainsFromBackup0(cache, 1);
    }

    /**
     * Tests that contains operation does not require loading the whole entry.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testContainsKeysTwoNodes() throws Exception {
        CacheConfiguration<Integer, Object> ccfg = cacheConfiguration(ATOMIC, HUGE_ATOMIC_CACHE_NAME, HUGE_DATA_REGION_NAME)
            .setReadFromBackup(false)
            .setBackups(1);

        IgniteCache<Integer, Object> cache = grid(0).getOrCreateCache(ccfg);

        testContainsFromBackup0(cache, 3);
    }

    /**
     * Tests that contains operation does not require loading the whole entry.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testContainsKeyWithExpiryPolicyTwoNodes() throws Exception {
        CacheConfiguration<Integer, Object> ccfg = cacheConfiguration(ATOMIC, HUGE_ATOMIC_CACHE_NAME, HUGE_DATA_REGION_NAME)
            .setReadFromBackup(false)
            .setBackups(1)
            .setEagerTtl(false)
            .setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(new Duration(TimeUnit.MINUTES, 1)));

        IgniteCache<Integer, Object> cache = grid(0).getOrCreateCache(ccfg);

        testContainsFromBackup0(cache, 1);
    }
}
