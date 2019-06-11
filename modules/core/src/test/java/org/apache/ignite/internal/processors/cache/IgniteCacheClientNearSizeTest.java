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

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Tests for @{cache.size()} against near client cache.
 */
public class IgniteCacheClientNearSizeTest extends IgniteCacheAbstractTest {

    /** Keys count. */
    private static final int KEYS_COUNT = 1_000;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected CacheAtomicityMode atomicityMode() {
        return TRANSACTIONAL;
    }

    /** {@inheritDoc} */
    @Override protected NearCacheConfiguration nearConfiguration() {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setClientMode(igniteInstanceName.contains(String.valueOf(gridCount())));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        storeMap.clear();
    }

    /**
     * Check @{cache.size()} against near client cache.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCacheNearSize() throws Exception {
        Set<Integer> keys = prepareKeys();

        startGridsMultiThreaded(gridCount());

        startGrid(gridCount());

        awaitPartitionMapExchange();

        IgniteCache<Integer, Integer> atomicCache = grid(gridCount()).createCache(
            new CacheConfiguration<Integer, Integer>("TestAtomic")
            .setBackups(1).setReadFromBackup(false), new NearCacheConfiguration<>());

        IgniteCache<Integer, Integer> atomicCacheReadBackup = grid(gridCount()).createCache(
            new CacheConfiguration<Integer, Integer>("TestAtomicReadBackup")
            .setBackups(1).setReadFromBackup(true), new NearCacheConfiguration<>());

        IgniteCache<Integer, Integer> txCache = grid(gridCount()).createCache(
            new CacheConfiguration<Integer, Integer>("TestTx")
            .setBackups(1).setReadFromBackup(false).setAtomicityMode(TRANSACTIONAL), new NearCacheConfiguration<>());

        IgniteCache<Integer, Integer> txCacheReadBackup = grid(gridCount()).createCache(
            new CacheConfiguration<Integer, Integer>("TestTxReadBackup")
            .setBackups(1).setReadFromBackup(true).setAtomicityMode(TRANSACTIONAL), new NearCacheConfiguration<>());

        assertFalse(grid(0).localNode().isClient());

        load(grid(0).getOrCreateCache("TestAtomic"), keys);
        load(grid(0).getOrCreateCache("TestAtomicReadBackup"), keys);
        load(grid(0).getOrCreateCache("TestTx"), keys);
        load(grid(0).getOrCreateCache("TestTxReadBackup"), keys);

        checkNearCacheSize(atomicCache, keys);
        checkNearCacheSize(atomicCacheReadBackup, keys);
        checkNearCacheSize(txCache, keys);
        checkNearCacheSize(txCacheReadBackup, keys);
    }

    /**
     * Verify that {@code cache.size(CachePeekMode.NEAR)} on client cache returns correct amount.
     *
     * @param cache Cache to check.
     * @param keys Keys to check.
     */
    private void checkNearCacheSize(IgniteCache<Integer, Integer> cache, Set<Integer> keys) {
        assertTrue("Unexpected cache client mode.", grid(gridCount()).localNode().isClient());

        assertTrue("Unexpected cache near mode.", ((IgniteKernal)grid(gridCount())).internalCache(cache.getName()).isNear());

        assertEquals("Unexpected cache size within CachePeekMode.PRIMARY mode.", KEYS_COUNT, cache.size(CachePeekMode.PRIMARY));

        assertEquals("Unexpected cache size within CachePeekMode.NEAR mode.", 0, cache.size(CachePeekMode.NEAR));

        Map<Integer, ?> data = cache.getAll(keys);

        assertEquals("Unexpected data size.",KEYS_COUNT, data.size());

        assertEquals("Unexpected cache size within CachePeekMode.NEAR mode.", KEYS_COUNT, cache.size(CachePeekMode.NEAR));
    }

    /**
     * Populate cache with data.
     *
     * @param cache Cache to populate with data.
     * @param keys Keys to put.
     */
    private static void load(IgniteCache<Integer, Integer> cache, Set<Integer> keys) {
        Map<Integer, Integer> map = new TreeMap<>();

        for (int key : keys)
            map.put(key, key);

        cache.putAll(map);
    }

    /**
     * @return Prepared keys.
     */
    public Set<Integer> prepareKeys() {
        Set<Integer> keys = new TreeSet<>();

        for (int i = 0; i < KEYS_COUNT; i++)
            keys.add(i);

        return keys;
    }
}
