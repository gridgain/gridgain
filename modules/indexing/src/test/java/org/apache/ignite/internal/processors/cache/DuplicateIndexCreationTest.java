/*
 * Copyright 2023 GridGain Systems, Inc. and Contributors.
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

import java.io.Serializable;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** Duplicate index tests. */
public class DuplicateIndexCreationTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);
        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(true)
                .setMaxSize(256 * 1024L * 1024L)));
        cfg.setCacheConfiguration(
            new CacheConfiguration<>()
                .setName(DEFAULT_CACHE_NAME)
                .setSqlSchema("PUBLIC")
                .setIndexedTypes(Integer.class, Person.class),
            new CacheConfiguration<>()
                .setName("CACHE_2")
                .setSqlSchema("PUBLIC")
                .setIndexedTypes(Integer.class, Department.class));
        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();
        stopAllGrids();
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();
        stopAllGrids();
        cleanPersistenceDir();
    }

    /** Repeatedly create index with the default name, rerun cluster. */
    @Test
    public void testIndexCreationDefaultName() throws Exception {
        SqlFieldsQuery queryCreateIndex = new SqlFieldsQuery("CREATE INDEX ON PUBLIC.PERSON (NAME)");
        SqlFieldsQuery queryCreateIndexIfNotExist = new SqlFieldsQuery("CREATE INDEX IF NOT EXISTS ON PUBLIC.PERSON (NAME)");

        IgniteEx node = startGrid(0);
        node.cluster().state(ClusterState.ACTIVE);
        {
            IgniteCache<Object, Object> cache = node.cache(DEFAULT_CACHE_NAME);

            cache.query(queryCreateIndex).getAll();

            GridTestUtils.assertThrows(log, () -> cache.query(queryCreateIndex).getAll(), CacheException.class, null);
            GridTestUtils.assertThrows(log, () -> cache.query(queryCreateIndexIfNotExist).getAll(), CacheException.class, null);

            stopGrid(0);
            startGrid(0);

            stopGrid(0);
            cleanPersistenceDir();
        }

        node = startGrid(0);
        node.cluster().state(ClusterState.ACTIVE);
        {
            IgniteCache<Object, Object> cache = node.cache(DEFAULT_CACHE_NAME);

            cache.query(queryCreateIndexIfNotExist).getAll();
            GridTestUtils.assertThrows(log, () -> cache.query(queryCreateIndex).getAll(), CacheException.class, null);

            stopGrid(0);
            startGrid(0);
        }
    }

    /** Repeatedly create index with the default name, rerun cluster. */
    @Test
    public void testIndexCreationViaAnnotations() throws Exception {
        SqlFieldsQuery queryCreateIndex = new SqlFieldsQuery("CREATE INDEX PERSON_NAME_IDX ON PUBLIC.PERSON (NAME)");
        SqlFieldsQuery queryCreateIndexIfNotExist = new SqlFieldsQuery("CREATE INDEX PERSON_NAME_IDX IF NOT EXISTS ON PUBLIC.PERSON (NAME)");

        IgniteEx node = startGrid(0);
        node.cluster().state(ClusterState.ACTIVE);
        {
            IgniteCache<Object, Object> cache = node.cache(DEFAULT_CACHE_NAME);

            GridTestUtils.assertThrows(log, () -> cache.query(queryCreateIndex).getAll(), CacheException.class, null);
            GridTestUtils.assertThrows(log, () -> cache.query(queryCreateIndexIfNotExist).getAll(), CacheException.class, null);

            stopGrid(0);
            startGrid(0);

            stopGrid(0);
            cleanPersistenceDir();
        }

        node = startGrid(0);
        node.cluster().state(ClusterState.ACTIVE);
        {
            IgniteCache<Object, Object> cache = node.cache(DEFAULT_CACHE_NAME);

            GridTestUtils.assertThrows(log, () -> cache.query(queryCreateIndex).getAll(), CacheException.class, null);
            GridTestUtils.assertThrows(log, () -> cache.query(queryCreateIndexIfNotExist).getAll(), CacheException.class, null);

            stopGrid(0);
            startGrid(0);
        }
    }

    /** Repeatedly create index with the same name, rerun cluster. */
    @Test
    public void testIndexCreation() throws Exception {
        SqlFieldsQuery queryCreateIndex = createIndexQuery("PERSON", false);
        SqlFieldsQuery queryCreateIndexIfNotExist = createIndexQuery("PERSON", true);

        IgniteEx node = startGrid(0);
        node.cluster().state(ClusterState.ACTIVE);
        {
            IgniteCache<Object, Object> cache = node.cache(DEFAULT_CACHE_NAME);

            cache.query(queryCreateIndex).getAll();

            GridTestUtils.assertThrows(log, () -> cache.query(queryCreateIndex).getAll(), CacheException.class, null);
            cache.query(queryCreateIndexIfNotExist).getAll();

            stopGrid(0);
            startGrid(0);

            stopGrid(0);
            cleanPersistenceDir();
        }

        node = startGrid(0);
        node.cluster().state(ClusterState.ACTIVE);
        {
            IgniteCache<Object, Object> cache = node.cache(DEFAULT_CACHE_NAME);
            cache.query(queryCreateIndexIfNotExist).getAll();
            GridTestUtils.assertThrows(log, () -> cache.query(queryCreateIndex).getAll(), CacheException.class, null);

            stopGrid(0);
            startGrid(0);
        }
    }

    /** Create index with the same name on different caches + rerun cluster. */
    @Test
    public void testIndexCreationDifferentCaches() throws Exception {
        SqlFieldsQuery queryCreateIndex = createIndexQuery("DEPARTMENT", false);
        SqlFieldsQuery queryCreateIndexIfNotExist = createIndexQuery("DEPARTMENT", true);

        IgniteEx node = startGrid(0);
        node.cluster().state(ClusterState.ACTIVE);
        {
            IgniteCache<Object, Object> cache = node.cache(DEFAULT_CACHE_NAME);

            cache.query(createIndexQuery("PERSON", false)).getAll();

            GridTestUtils.assertThrows(log, () -> cache.query(queryCreateIndex).getAll(), CacheException.class, null);
            cache.query(queryCreateIndexIfNotExist).getAll();

            stopGrid(0);
            startGrid(0);

            stopGrid(0);
            cleanPersistenceDir();
        }

        node = startGrid(0);
        node.cluster().state(ClusterState.ACTIVE);
        {
            IgniteCache<Object, Object> cache = node.cache(DEFAULT_CACHE_NAME);

            cache.query(queryCreateIndexIfNotExist).getAll();
            GridTestUtils.assertThrows(log, () -> cache.query(queryCreateIndex).getAll(), CacheException.class, null);

            stopGrid(0);
            startGrid(0);
        }
    }

    private static SqlFieldsQuery createIndexQuery(String tableName, boolean ifNotExists) {
        assert !tableName.isEmpty();

        return new SqlFieldsQuery(
            "CREATE INDEX " +
                (ifNotExists ? "IF NOT EXISTS " : "") +
                " NAME_IDX ON PUBLIC." +
                tableName +
                " (NAME)"
        );
    }

    /**
     * Person class.
     */
    private static class Person implements Serializable {
        /** Indexed name. */
        @QuerySqlField(index = true)
        public String name;
    }


    /**
     * Department class.
     */
    private static class Department implements Serializable {
        /** Indexed name. */
        @QuerySqlField
        public String name;
    }
}
