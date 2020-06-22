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
import java.util.stream.Collectors;
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
import org.h2.jdbc.JdbcSQLSyntaxErrorException;
import org.junit.Test;

/**
 * Tests for registration custom aggregation functions.
 */
public class IgniteSqlCustomAggregationTest extends AbstractIndexingCommonTest {
    /** */
    private static final String CACHE_NAME = "cache";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration<PersonKey, Person> cacheCfg = new CacheConfiguration<PersonKey, Person>(CACHE_NAME)
            .setIndexedTypes(PersonKey.class, Person.class)
            .setCacheMode(CacheMode.PARTITIONED)
            .setBackups(0);

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGrids(3);
        startClientGrid(3);
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
    public void testAggregateFunctionsCollocated() throws Exception {
        IgniteEx ignite = grid(3);

        IgniteCache<PersonKey, Person> cache = ignite.cache(CACHE_NAME);

        loadCacheWithoutNullValues(cache);

        //check with grouping by
        List<List<?>> rows = cache.query(new SqlFieldsQuery(
            "select companyId,FIRSTVALUE(NAME,AGE), LASTVALUE(UPPER(NAME),AGE) from \"cache\".Person group by companyId")
            .setCollocated(true)).getAll();

        assertEquals(10, rows.size());

        for (List<?> row : rows) {
            Integer companyId = (Integer)row.get(0);
            String youngest = (String)row.get(1);
            String oldest = (String)row.get(2);

            assertEquals("name" + companyId, youngest);
            assertEquals("name9".toUpperCase() + companyId, oldest);
        }

        //check without grouping by
        rows = cache.query(new SqlFieldsQuery(
            "select FIRSTVALUE(NAME,AGE), LASTVALUE(UPPER(NAME),AGE) from \"cache\".Person where companyId = 1")
            .setCollocated(true)).getAll();

        assertEquals(1, rows.size());

        List<?> row = rows.get(0);

        assertEquals("name1", row.get(0));
        assertEquals("NAME91", row.get(1));
    }

    /**
     * @throws Exception If error.
     */
    @Test
    public void testAggregateFunctionsWithDistinctArgument() throws Exception {
        IgniteEx ignite = grid(3);

        for (int i = 0; i < 4; i++) {
            ((IgniteH2Indexing)grid(i).context().query().getIndexing()).
                registerAggregateFunction("ACCUMULATE", AccumulateFunction.class);
        }

        IgniteCache<PersonKey, Person> cache = ignite.cache(CACHE_NAME);

        loadCacheWithoutNullValues(cache);

        List<List<?>> rows = cache.query(new SqlFieldsQuery(
            "select companyId,ACCUMULATE(DISTINCT companyId) from \"cache\".Person group by companyId")
            .setCollocated(true)).getAll();

        assertEquals(10, rows.size());

        for (List<?> row : rows) {
            Integer companyId = (Integer)row.get(0);
            List list = (List)row.get(1);

            assertEquals(1, list.size());
            assertEquals(companyId, list.get(0));
        }
    }

    /**
     * @throws Exception If error.
     */
    @Test
    public void testAggregateFunctionsWithDistinctResult() throws Exception {
        IgniteEx ignite = grid(3);

        IgniteCache<PersonKey, Person> cache = ignite.cache(CACHE_NAME);

        loadCacheWithoutNullValues(cache);

        List<List<?>> rows = cache.query(new SqlFieldsQuery(
            "select DISTINCT departmentId,FIRSTVALUE(departmentId, companyId) from \"cache\".Person group by departmentId")
            .setCollocated(true)).getAll();

        // Check that the result set has distinct results
        assertEquals(5, rows.stream().map(r -> r.get(0)).collect(Collectors.toSet()).size());

        for (List<?> row : rows)
            assertEquals((Integer)row.get(0), (Integer)row.get(1));

    }

    /**
     * @throws Exception If error.
     */
    @Test
    public void testAggregateFunctionsNotCollocated() throws Exception {
        IgniteEx ignite = grid(3);

        IgniteCache<PersonKey, Person> cache = ignite.cache(CACHE_NAME);

        loadCacheWithoutNullValues(cache);

        GridTestUtils.assertThrowsAnyCause(log, new Callable<Object>() {
            @Override public Object call() {
                cache.query(new SqlFieldsQuery(
                    "select companyId,FIRSTVALUE(NAME,AGE), LASTVALUE(NAME,AGE) from \"cache\".Person group by companyId")).getAll();

                return null;
            }
        }, IgniteSQLException.class, "Custom aggregation function is not supported for not collocated data");
    }

    /**
     * @throws Exception If error.
     */
    @Test
    public void testParsingAggregateFunctions() throws Exception {
        IgniteEx ignite = grid(3);

        IgniteCache<PersonKey, Person> cache = ignite.cache(CACHE_NAME);

        //check calling without arguments
        GridTestUtils.assertThrowsAnyCause(log, new Callable<Object>() {
            @Override public Object call() {
                cache.query(new SqlFieldsQuery(
                    "select companyId, FIRSTVALUE(), LASTVALUE() from \"cache\".Person group by companyId")
                    .setCollocated(true)
                ).getAll();

                return null;
            }
        }, JdbcSQLSyntaxErrorException.class, null);

        //check distinct for sorting field
        GridTestUtils.assertThrowsAnyCause(log, new Callable<Object>() {
            @Override public Object call() {
                cache.query(new SqlFieldsQuery(
                    "select companyId,FIRSTVALUE(name, DISTINCT age), LASTVALUE(name, DISTINCT age) from \"cache\".Person group by companyId")
                    .setCollocated(true)
                ).getAll();

                return null;
            }
        }, JdbcSQLSyntaxErrorException.class, null);

        //check invalid function name
        GridTestUtils.assertThrowsAnyCause(log, new Callable<Object>() {
            @Override public Object call() throws IgniteCheckedException {
                for (int i = 0; i < 4; i++) {
                    ((IgniteH2Indexing)grid(i).context().query().getIndexing()).
                        registerAggregateFunction("ACCUMULATE%", AccumulateFunction.class);
                }

                return null;
            }
        }, IgniteCheckedException.class, null);
    }

    /**
     * @throws Exception If error.
     */
    @Test
    public void testAggregateFunctionsForNullValues() throws Exception {
        IgniteEx ignite = grid(3);

        IgniteCache<PersonKey, Person> cache = ignite.cache(CACHE_NAME);

        // check on empty table
        List<List<?>> rows = cache.query(new SqlFieldsQuery(
            "select companyId,FIRSTVALUE(NAME,AGE), LASTVALUE(NAME,AGE) from \"cache\".Person group by companyId")
            .setCollocated(true)).getAll();

        assertEquals(0, rows.size());

        loadCacheWithNullValues(cache);

        //check null values for compared and returned fields
        rows = cache.query(new SqlFieldsQuery(
            "select companyId,FIRSTVALUE(NAME,AGE), LASTVALUE(NAME,AGE) from \"cache\".Person group by companyId order by companyId")
            .setCollocated(true)).getAll();

        List<?> first = rows.get(0);
        assertEquals(1, first.get(0));
        assertEquals("name10", first.get(1));
        assertEquals("name11", first.get(2));

        List<?> second = rows.get(1);
        assertEquals(2, second.get(0));
        assertNull(second.get(1));
        assertEquals("name11", second.get(2));
    }

    /** */
    private IgniteCache<PersonKey, Person> loadCacheWithoutNullValues(IgniteCache<PersonKey, Person> cache) {
        for (int i = 0; i < 100; i++)
            cache.put(new PersonKey(i, i % 10), new Person("name" + i, i, i % 5));

        return cache;
    }

    /** */
    private IgniteCache<PersonKey, Person> loadCacheWithNullValues(IgniteCache<PersonKey, Person> cache) {
        cache.put(new PersonKey(10, 1), new Person("name10", 1, null));
        cache.put(new PersonKey(11, 1), new Person("name11", null, null));
        cache.put(new PersonKey(12, 1), new Person("name12", 3, null));
        cache.put(new PersonKey(20, 2), new Person(null, 1, null));
        cache.put(new PersonKey(21, 2), new Person("name11", 2, null));

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
        PersonKey(int id, int companyId) {
            this.id = id;
            this.companyId = companyId;
        }
    }

    /** */
    static class Person {
        /** */
        @QuerySqlField
        String name;

        /** */
        @QuerySqlField
        Integer age;

        /** */
        @QuerySqlField
        Integer departmentId;

        /** */
        Person(String name, Integer age, Integer departmentId) {
            this.name = name;
            this.age = age;
            this.departmentId = departmentId;
        }
    }

    /**
     * Test aggregation function for collecting objects.
     */
    public static class AccumulateFunction implements AggregateFunction {
        /** */
        private List<Object> res = new ArrayList<>();

        /** {@inheritDoc} */
        @Override public void init(Connection conn) throws SQLException {
        }

        /** {@inheritDoc} */
        @Override public int getType(int[] inputTypes) throws SQLException {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public void add(Object value) throws SQLException {
            if (value instanceof Object[])
                res.add(((Object[])value)[0]);
            else
                res.add(value);
        }

        /** {@inheritDoc} */
        @Override public Object getResult() throws SQLException {
            return res;
        }
    }
}
