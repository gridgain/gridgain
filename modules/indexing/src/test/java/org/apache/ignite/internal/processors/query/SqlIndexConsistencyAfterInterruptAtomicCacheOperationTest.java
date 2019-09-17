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

package org.apache.ignite.internal.processors.query;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 *
 */
@RunWith(Parameterized.class)
public class SqlIndexConsistencyAfterInterruptAtomicCacheOperationTest extends AbstractIndexingCommonTest {
    /** Keys count. */
    private static final int KEYS = 1000;

    /**
     * Test's parameters.
     */
    @Parameterized.Parameters(name = "atomicity={0}")
    public static Iterable<Object[]> params() {
        return Arrays.asList(
            new Object[] {CacheAtomicityMode.ATOMIC},
            new Object[] {CacheAtomicityMode.TRANSACTIONAL}
        );
    }

    /** Enable persistence for the test. */
    @Parameterized.Parameter(0)
    public CacheAtomicityMode atomicity;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception On error.
     */
    @Test
    public void testCachePut() throws Exception {
        IgniteEx ign = startGrid(0);

        IgniteCache<Object, Object> cache = ign.createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAtomicityMode(atomicity)
            .setIndexedTypes(Integer.class, Integer.class));

        Thread t = new Thread(() -> {
            cache.put(1, 1);
        });

        t.start();

        t.interrupt();

        t.join();

        assertEquals(cache.size(), cache.query(new SqlFieldsQuery("select * from Integer")).getAll().size());
    }

    /**
     * @throws Exception On error.
     */
    @Test
    public void testCachePutAll() throws Exception {
        IgniteEx ign = startGrid(0);

        IgniteCache<Object, Object> cache = ign.createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAtomicityMode(atomicity)
            .setIndexedTypes(Integer.class, Integer.class));

        final Map<Integer, Integer> batch = new HashMap<>();

        for (int i = 0; i < KEYS; ++i)
            batch.put(i, i);

        Thread t = new Thread(() -> {
            cache.putAll(batch);
        });

        t.start();
        t.interrupt();
        t.join();

        assertEquals(cache.size(), cache.query(new SqlFieldsQuery("select * from Integer")).getAll().size());
    }

    /**
     * @throws Exception On error.
     */
    @Test
    public void testCacheRemove() throws Exception {
        IgniteEx ign = startGrid(0);

        IgniteCache<Object, Object> cache = ign.createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAtomicityMode(atomicity)
            .setIndexedTypes(Integer.class, Integer.class));

        cache.put(1, 1);

        Thread t = new Thread(() -> {
            cache.remove(1);
        });

        t.start();

        t.interrupt();

        t.join();

        assertEquals(cache.size(), cache.query(new SqlFieldsQuery("select * from Integer")).getAll().size());
    }

    /**
     * @throws Exception On error.
     */
    @Test
    public void testCacheRemoveAll() throws Exception {
        IgniteEx ign = startGrid(0);

        IgniteCache<Object, Object> cache = ign.createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAtomicityMode(atomicity)
            .setIndexedTypes(Integer.class, Integer.class));

        final Map<Integer, Integer> batch = new HashMap<>();

        for (int i = 0; i < KEYS; ++i)
            batch.put(i, i);

        cache.putAll(batch);

        Thread t = new Thread(() -> {
            cache.removeAll(batch.keySet());
        });

        t.start();
        t.interrupt();
        t.join();

        assertEquals(cache.size(), cache.query(new SqlFieldsQuery("select * from Integer")).getAll().size());
    }

    /**
     * @throws Exception On error.
     */
    @Test
    public void testCacheInsert() throws Exception {
        IgniteEx ign = startGrid(0);

        IgniteCache<Object, Object> cache = ign.createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAtomicityMode(atomicity)
            .setIndexedTypes(Integer.class, Integer.class));

        Thread t = new Thread(() -> cache.query(new SqlFieldsQuery("INSERT INTO Integer (_KEY, _VAL) VALUES (1, 1)")));

        t.start();
        t.interrupt();
        t.join();

        assertEquals(cache.size(), cache.query(new SqlFieldsQuery("select * from Integer")).getAll().size());
    }

    /**
     * @throws Exception On error.
     */
    @Test
    public void testCacheDelete() throws Exception {
        IgniteEx ign = startGrid(0);

        IgniteCache<Object, Object> cache = ign.createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAtomicityMode(atomicity)
            .setIndexedTypes(Integer.class, Integer.class));

        final Map<Integer, Integer> batch = new HashMap<>();

        for (int i = 0; i < KEYS; ++i)
            batch.put(i, i);

        cache.putAll(batch);

        Thread t = new Thread(() -> cache.query(new SqlFieldsQuery("DELETE FROM Integer WHERE _KEY > " + KEYS / 2)));

        t.start();
        t.interrupt();
        t.join();

        assertEquals(cache.size(), cache.query(new SqlFieldsQuery("select * from Integer")).getAll().size());
    }
}