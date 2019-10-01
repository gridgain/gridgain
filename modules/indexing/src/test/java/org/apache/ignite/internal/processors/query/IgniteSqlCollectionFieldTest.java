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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for OOM in case of collections in key/value.
 */
@RunWith(JUnit4.class)
public class IgniteSqlCollectionFieldTest extends AbstractIndexingCommonTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGrids(3);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** */
    @Test
    public void testKeySimpleToEntity() throws Exception {
        IgniteCache<Integer, Entity> c = grid(0).createCache(
            new CacheConfiguration<Integer, Entity>("KeySimpleToEntity")
            .setIndexedTypes(Integer.class, Entity.class));

        for (int i = 0; i < 20; i++) {
            Collection coll = new ArrayList();

            for (int j = 0; j < 2048; j++) {
                byte[] entry = new byte[2048];
                entry[i] = (byte)i;
                entry[j] = (byte)j;
                coll.add(entry);
            }

            c.put(i, new Entity(Integer.toString(i), coll));
        }

        assertEquals(10000.0, defAllocPages(), 2500.0);

        assertEquals(20, c.query(new SqlFieldsQuery("Select * from Entity")).getAll().size());
    }

    /** */
    @Test
    public void testKeyEntityToEntity() throws Exception {
        IgniteCache<KeyEntity, Entity> c = grid(0).createCache(
            new CacheConfiguration<KeyEntity, Entity>("KeyEntityToEntity")
                .setIndexedTypes(KeyEntity.class, Entity.class));

        for (int i = 0; i < 20; i++) {
            Collection coll = new ArrayList();

            for (int j = 0; j < 1000; j++) {
                byte[] entry = new byte[2048];
                entry[i] = (byte)i;
                entry[j] = (byte)j;
                coll.add(entry);
            }

            c.put(new KeyEntity(Integer.toString(-i), coll), new Entity(Integer.toString(i), coll));
        }

        assertEquals(10000.0, defAllocPages(), 2500.0);

        assertEquals(20, c.query(new SqlFieldsQuery("Select * from Entity")).getAll().size());
    }

    /** */
    @Test
    public void testKeyNestedEntityToEntity() throws Exception {
        IgniteCache<KeyEntity, Entity> c = grid(0).createCache(
            new CacheConfiguration<KeyEntity, Entity>("KeyNestedEntityToEntity")
                .setIndexedTypes(KeyEntity.class, Entity.class));

        for (int i = 0; i < 20; i++) {
            Collection keyColl = new ArrayList();
            Collection coll = new ArrayList();

            for (int k = 0; k < 1000; k++) {
                Collection nestedColl = new ArrayList();

                for (int j = 0; j < 1000; j++)
                    nestedColl.add(j);

                keyColl.add(new Entity(Integer.toString(k), nestedColl));
            }

            for (int j = 0; j < 100; j++) {
                byte[] entry = new byte[2048];
                entry[i] = (byte)i;
                entry[j] = (byte)j;
                coll.add(entry);
            }

            c.put(new KeyEntity(Integer.toString(-i), keyColl), new Entity(Integer.toString(i), coll));
        }

        assertEquals(10000.0, defAllocPages(), 2500.0);

        assertEquals(20, c.query(new SqlFieldsQuery("Select * from Entity")).getAll().size());
    }


    /** */
    @Test
    public void testKeyEntityToSimple() throws Exception {
        IgniteCache<KeyEntity, Integer> c = grid(0).createCache(
            new CacheConfiguration<KeyEntity, Integer>("KeyEntityToSimple")
                .setIndexedTypes(KeyEntity.class, Integer.class));

        for (int i = 0; i < 20; i++) {
            Collection coll = new ArrayList();

            for (int j = 0; j < 3072; j++) {
                byte[] entry = new byte[3072];
                entry[i] = (byte)i;
                entry[j] = (byte)j;
                coll.add(entry);
            }

            c.put(new KeyEntity(Integer.toString(i), coll), i);
        }

        assertEquals(10000.0, defAllocPages(), 2500.0);

        assertEquals(20, c.query(new SqlFieldsQuery("Select * from Integer")).getAll().size());
    }

    /** */
    @Test
    public void testKeySimpleToNestedEntity() throws Exception {
        IgniteCache<KeyEntity, Entity> c = grid(0).createCache(
            new CacheConfiguration<KeyEntity, Entity>("KeySimpleToNestedEntity")
                .setIndexedTypes(KeyEntity.class, Entity.class));

        for (int i = 0; i < 20; i++) {
            Collection valColl = new ArrayList();
            Collection coll = new ArrayList();

            for (int k = 0; k < 1000; k++) {
                Collection nestedColl = new ArrayList();

                for (int j = 0; j < 700; j++)
                    nestedColl.add(j);

                valColl.add(new Entity(Integer.toString(k), nestedColl));
            }

            for (int j = 0; j < 100; j++) {
                byte[] entry = new byte[2048];
                entry[i] = (byte)i;
                entry[j] = (byte)j;
                coll.add(entry);
            }

            c.put(new KeyEntity(Integer.toString(-i), coll), new Entity(Integer.toString(i), valColl));
        }

        assertEquals(10000.0, defAllocPages(), 2500.0);

        assertEquals(20, c.query(new SqlFieldsQuery("Select * from Entity")).getAll().size());
    }

    /** */
    private long defAllocPages() {
        return grid(0).dataRegionMetrics("default").getTotalAllocatedPages();
    }

    /** */
    private static class KeyEntity {
        /** */
        @QuerySqlField
        private String keyName;

        /** */
        @QuerySqlField
        private Collection keyPayload;

        /** */
        public KeyEntity(String name, Collection payload) {
            this.keyName = name;
            this.keyPayload = payload;
        }
    }

    /** */
    private static class Entity {
        /** */
        @QuerySqlField
        private String name;

        /** */
        @QuerySqlField
        private Collection payload;

        /** */
        public Entity(String name, Collection payload) {
            this.name = name;
            this.payload = payload;
        }
    }
}
