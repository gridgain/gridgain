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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.testframework.GridTestUtils;
import org.h2.api.AggregateFunction;
import org.junit.Test;

/**
 * Tests for registration custom aggregation functions.
 */
public class IgniteSqlCustomAggregationTest extends AbstractIndexingCommonTest {
    /** */
    private static final String CACHE_NAME = "cache";

    /** {@inheritDoc} */
    @Override
    protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration<PersonKey, Person> cacheCfg = new CacheConfiguration<PersonKey, Person>(CACHE_NAME)
            .setIndexedTypes(PersonKey.class, Person.class)
            .setCacheMode(CacheMode.PARTITIONED)
            .setBackups(0);

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids(false);
    }

    /**
     * @throws Exception If error.
     */
    @Test
    public void javaAggregationFunctionNonCollocated() throws Exception {
        startGrids(2);

        IgniteEx ignite = startClientGrid(2);

        IgniteCache<PersonKey, Person> cache = ignite.cache(CACHE_NAME);

        loadCache(cache);

        registerFunction();

        GridTestUtils.assertThrowsAnyCause(log, new Callable<Object>() {
            @Override
            public Object call() {
                cache.query(new SqlFieldsQuery("select departmentId, ACCUMULATE(name) from \"cache\".Person group by departmentId")).getAll();

                return null;
            }
        }, IgniteSQLException.class, "Custom aggregation function is not supported for not collocated data");
    }

    /**
     * @throws Exception If error.
     */
    @Test
    public void javaAggregationFunctionCollocated() throws Exception {
        startGrids(2);

        IgniteEx ignite = startClientGrid(2);

        IgniteCache<PersonKey, Person> cache = ignite.cache(CACHE_NAME);

        loadCache(cache);

        registerFunction();

        final List<List<?>> lists = cache.query(new SqlFieldsQuery("select companyId, ACCUMULATE(id) from \"cache\".Person group by companyId").setCollocated(true)).getAll();
        for (List<?> row : lists) {
            final Integer companyId = (Integer)row.get(0);
            final Object[] ids = (Object[])row.get(1);

            assertEquals(10, ids.length);
        }
    }

    /**
     * @throws IgniteCheckedException If registration failed.
     */
    private void registerFunction() throws IgniteCheckedException {
        for (int i = 0; i < 3; i++)
            ((IgniteH2Indexing)grid(i).context().query().getIndexing()).connections().executeStatement(null, "CREATE AGGREGATE IF NOT EXISTS ACCUMULATE FOR \"" + AccumulateFunction.class.getName() + "\"");
    }

    /** */
    private IgniteCache<PersonKey, Person> loadCache(IgniteCache<PersonKey, Person> cache) {
        for (int i = 0; i < 100; i++)
            cache.put(new PersonKey(i, i % 10, i % 5), new Person("name" + i));

        return cache;
    }

    /** */
    static class PersonKey {

        /** */
        @QuerySqlField
        int id;

        /** */
        @QuerySqlField
        @AffinityKeyMapped
        int companyId;

        /** */
        @QuerySqlField
        int departmentId;

        /** */
        public PersonKey(int id, int companyId, int departmentId) {
            this.id = id;
            this.companyId = companyId;
            this.departmentId = departmentId;
        }
    }

    /** */
    static class Person {
        /** */
        @QuerySqlField
        String name;

        /** */
        public Person(String name) {
            this.name = name;
        }
    }

    /**
     * Test aggregation function for collecting objects.
     */
    public static class AccumulateFunction implements AggregateFunction {

        /** */
        List<Object> result = new ArrayList<>();

        /** {@inheritDoc} */
        @Override
        public void init(Connection conn) throws SQLException {
        }

        /** {@inheritDoc} */
        @Override
        public int getType(int[] inputTypes) throws SQLException {
            return 0;
        }

        /** {@inheritDoc} */
        @Override
        public void add(Object value) throws SQLException {
            result.add(value);
        }

        /** {@inheritDoc} */
        @Override
        public Object getResult() throws SQLException {
            return result.toArray();
        }
    }
}
