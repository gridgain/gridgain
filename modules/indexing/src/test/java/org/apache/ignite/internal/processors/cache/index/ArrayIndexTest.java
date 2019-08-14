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

package org.apache.ignite.internal.processors.cache.index;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.junit.After;
import org.junit.Test;

/**
 * Checks that sql operation works by arrays.
 */
public class ArrayIndexTest extends AbstractIndexingCommonTest {
    /**
     * @throws Exception if fails.
     */
    @After
    public void tearDown() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setPersistenceEnabled(true)
            )
        );

        cfg.setCacheConfiguration(
            new CacheConfiguration(DEFAULT_CACHE_NAME)
                .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
                .setQueryEntities(Collections.singleton(
                    new QueryEntity()
                        .setKeyType(byte[].class.getName())
                        .setValueType(Integer.class.getName())
                ))
        );

        return cfg;
    }

    /**
     *
     */
    @Test
    public void shouldSelectAllRows() throws Exception {
        IgniteEx ex = startGrid(0);

        ex.cluster().active(true);

        IgniteCache<Object, Object> cache = ex.getOrCreateCache(DEFAULT_CACHE_NAME);

        cache.put(new byte[] {8, 9, 10}, 3);
        cache.put(new byte[] {1, 2, 3}, 1);
        cache.put(new byte[] {5, 6, 7}, 2);

        List<List<?>> sorted = cache.query(new SqlFieldsQuery("select _key, _val from Integer")).getAll();

        assertEquals(3, sorted.size());

        List<?> first = sorted.get(0);
        assertTrue(Objects.deepEquals(first.get(0), new byte[] {1, 2, 3}));
        assertTrue(Objects.deepEquals(first.get(1), 1));

        List<?> second = sorted.get(1);
        assertTrue(Objects.deepEquals(second.get(0), new byte[] {5, 6, 7}));
        assertTrue(Objects.deepEquals(second.get(1), 2));

        List<?> third = sorted.get(2);
        assertTrue(Objects.deepEquals(third.get(0), new byte[] {8, 9, 10}));
        assertTrue(Objects.deepEquals(third.get(1), 3));
    }

    /**
     *
     */
    @Test
    public void shouldSelectParticularValue() throws Exception {
        IgniteEx ex = startGrid(0);

        ex.cluster().active(true);

        IgniteCache<Object, Object> cache = ex.getOrCreateCache(DEFAULT_CACHE_NAME);

        cache.put(new byte[] {1, 2, 3}, 1);
        cache.put(new byte[] {5, 6, 7}, 2);
        cache.put(new byte[] {8, 9, 10}, 3);

        List<List<?>> res = cache.query(new SqlFieldsQuery("select _key, _val from Integer where _key = ?")
            .setArgs(new byte[] {5, 6, 7}))
            .getAll();

        assertEquals(1, res.size());

        List<?> row = res.get(0);
        assertTrue(Objects.deepEquals(row.get(0), new byte[] {5, 6, 7}));
        assertTrue(Objects.deepEquals(row.get(1), 2));
    }
}
