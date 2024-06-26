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

package org.apache.ignite.cache.store;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.cache.Cache;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import javax.cache.integration.CompletionListenerFuture;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractSelfTest;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Test check that cache values removes from cache on expiry.
 */
public class IgniteCacheExpiryStoreLoadSelfTest extends GridCacheAbstractSelfTest {
    /** Expected time to live in milliseconds. */
    private static final int TIME_TO_LIVE = 1000;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return PARTITIONED;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected CacheConfiguration cacheConfiguration(String igniteInstanceName) throws Exception {
        CacheConfiguration cfg = super.cacheConfiguration(igniteInstanceName);

        cfg.setCacheStoreFactory(singletonFactory(new TestStore()));
        cfg.setReadThrough(true);
        cfg.setWriteThrough(true);
        cfg.setLoadPreviousValue(true);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLoadCacheWithExpiry() throws Exception {
        checkLoad(false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLoadCacheWithExpiryAsync() throws Exception {
        checkLoad(true);
    }

    /**
     * @param async If {@code true} uses asynchronous method.
     * @throws Exception If failed.
     */
    private void checkLoad(boolean async) throws Exception {
        IgniteCache<String, Integer> cache = jcache(0)
           .withExpiryPolicy(new CreatedExpiryPolicy(new Duration(MILLISECONDS, TIME_TO_LIVE)));

         List<Integer> keys = new ArrayList<>();

        keys.add(primaryKey(jcache(0)));
        keys.add(primaryKey(jcache(1)));
        keys.add(primaryKey(jcache(2)));

        if (async)
            cache.loadCacheAsync(null, keys.toArray(new Integer[3])).get();
        else
            cache.loadCache(null, keys.toArray(new Integer[3]));

        assertEquals(3, cache.size(CachePeekMode.PRIMARY));

        assertTrue(GridTestUtils.waitForCondition(() -> cache.size() == 0, 10_000));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLocalLoadCacheWithExpiry() throws Exception {
        checkLocalLoad(false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLocalLoadCacheWithExpiryAsync() throws Exception {
        checkLocalLoad(true);
    }

    /**
     * @param async If {@code true} uses asynchronous method.
     * @throws Exception If failed.
     */
    private void checkLocalLoad(boolean async) throws Exception {
        final IgniteCache<String, Integer> cache = jcache(0)
            .withExpiryPolicy(new CreatedExpiryPolicy(new Duration(MILLISECONDS, TIME_TO_LIVE)));

        List<Integer> keys = primaryKeys(cache, 3);

        if (async)
            cache.localLoadCacheAsync(null, keys.toArray(new Integer[3])).get();
        else
            cache.localLoadCache(null, keys.toArray(new Integer[3]));

        assertEquals(3, cache.localSize());

        assertTrue(GridTestUtils.waitForCondition(() -> cache.localSize() == 0, 10_000));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLoadAllWithExpiry() throws Exception {
        IgniteCache<Integer, Integer> cache = ignite(0).<Integer, Integer>cache(DEFAULT_CACHE_NAME)
            .withExpiryPolicy(new CreatedExpiryPolicy(new Duration(MILLISECONDS, TIME_TO_LIVE)));

        Set<Integer> keys = new HashSet<>();

        keys.add(primaryKey(jcache(0)));
        keys.add(primaryKey(jcache(1)));
        keys.add(primaryKey(jcache(2)));

        CompletionListenerFuture fut = new CompletionListenerFuture();

        cache.loadAll(keys, false, fut);

        fut.get();

        assertEquals(3, cache.size(CachePeekMode.PRIMARY));

        assertTrue(GridTestUtils.waitForCondition(() -> cache.localSize() == 0, 10_000));
    }

    /**
     * Test cache store.
     */
    private static class TestStore implements CacheStore<Integer, Integer> {
        /** {@inheritDoc} */
        @Override public void loadCache(IgniteBiInClosure<Integer, Integer> clo,
            @Nullable Object... args) throws CacheLoaderException {
            assertNotNull(args);
            assertTrue(args.length > 0);

            for (Object arg : args) {
                Integer k = (Integer)arg;

                clo.apply(k, k);
            }
        }

        /** {@inheritDoc} */
        @Override public void sessionEnd(boolean commit) throws CacheWriterException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public Integer load(Integer key) throws CacheLoaderException {
            return key;
        }

        /** {@inheritDoc} */
        @Override public Map<Integer, Integer> loadAll(Iterable<? extends Integer> keys) {
            Map<Integer, Integer> map = new HashMap<>();

            for (Integer key : keys)
                map.put(key, key);

            return map;
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<? extends Integer, ? extends Integer> entry) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void writeAll(Collection<Cache.Entry<? extends Integer, ? extends Integer>> entries) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void deleteAll(Collection<?> keys) {
            // No-op.
        }
    }
}
