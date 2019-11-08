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

package org.apache.ignite.p2p;

import java.lang.reflect.Constructor;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class GridP2PScanQueryWithClientTransformerTest extends GridCommonAbstractTest {
    /** Test class loader. */
    private static final ClassLoader TEST_CLASS_LOADER;

    /** */
    private static final String TRANSFORMER_CLASS_NAME = "org.apache.ignite.tests.p2p.cache.ScanQueryTestTransformer";

    /** */
    private static final String TRANSFORMER_CLOSURE_NAME = "org.apache.ignite.tests.p2p.cache.ScanQueryTestTransformerWrapper$1";

    /** */
    private static final int SCALE_FACTOR = 7;

    /** */
    private static final int CACHE_SIZE = 10;

    /** Initialize ClassLoader. */
    static {
        try {
            TEST_CLASS_LOADER = new URLClassLoader(
                new URL[] {new URL(GridTestProperties.getProperty("p2p.uri.cls"))},
                GridP2PScanQueryWithClientTransformerTest.class.getClassLoader());
        }
        catch (MalformedURLException e) {
            throw new RuntimeException("Define property p2p.uri.cls", e);
        }
    }

    /**
     * Verifies that Scan Query transformer is loaded by p2p mechanism when it is missing on server's classpath.
     */
    @Test
    public void testScanQueryCursorWithExternalClass() throws Exception {
        IgniteEx ig0 = startGrid(0);

        IgniteCache<Integer, Integer> cache = ig0.createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME));

        int sumPopulated = populateCache(cache);

        IgniteEx client = startClientGrid(1);

        IgniteCache<Object, Object> clientCache = client.getOrCreateCache(DEFAULT_CACHE_NAME);

        QueryCursor<Integer> query = clientCache.query(new ScanQuery<Integer, Integer>(), loadTransformerClass());

        int sumQueried = 0;

        for (Integer val : query) {
            sumQueried += val;
        }

        assertTrue(sumQueried == sumPopulated * SCALE_FACTOR);
    }

    @Test
    public void testScanQueryGetAllWithExternalClass() throws Exception {
        IgniteEx ig0 = startGrid(0);

        IgniteCache<Integer, Integer> cache = ig0.createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME));

        populateCache(cache);

        IgniteEx client = startClientGrid(1);

        IgniteCache<Object, Object> clientCache = client.getOrCreateCache(DEFAULT_CACHE_NAME);

        QueryCursor<Integer> query = clientCache.query(new ScanQuery<Integer, Integer>(), loadTransformerClass());

        List<Integer> results = query.getAll();

        assertNotNull(results);
        assertEquals(CACHE_SIZE, results.size());
    }

    @Test
    public void testScanQueryCursorWithAnonymousClass() throws Exception {
        IgniteEx ig0 = startGrid(0);

        IgniteCache<Integer, Integer> cache = ig0.createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME));

        populateCache(cache);

        IgniteEx client = startClientGrid(1);

        IgniteCache<Object, Object> clientCache = client.getOrCreateCache(DEFAULT_CACHE_NAME);

        QueryCursor<Integer> query = clientCache.query(new ScanQuery<Integer, Integer>(), loadTransformerClosure());

        List<Integer> results = query.getAll();
    }

    /**
     * @param cache Cache to populate.
     */
    private int populateCache(IgniteCache cache) {
        int sum = 0;

        for (int i = 0; i < CACHE_SIZE; i++) {
            sum += i;

            cache.put(i, i);
        }

        return sum;
    }

    /**
     * Loads class for query transformer from another package so server doesn't have access to it.
     *
     * @return Instance of transformer class.
     * @throws Exception If load has failed.
     */
    private IgniteClosure loadTransformerClass() throws Exception {
        Constructor ctor = TEST_CLASS_LOADER.loadClass(TRANSFORMER_CLASS_NAME).getConstructor(int.class);

        return (IgniteClosure)ctor.newInstance(SCALE_FACTOR);
    }

    private IgniteClosure loadTransformerClosure() throws Exception {
        return (IgniteClosure)TEST_CLASS_LOADER.loadClass(TRANSFORMER_CLOSURE_NAME).newInstance();
    }
}
