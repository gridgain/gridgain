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

package org.apache.ignite.internal.processors.query.h2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.query.h2.H2Utils.IGNITE_SQL_ALLOW_IMPLICIT_PK;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/**
 * Basic tests to check the possibility of creating tables without specifying a primary key.
 */
public class TableWithImplicitPkTest extends GridCommonAbstractTest {
    /** Nodes count. */
    private static final int NODES_CNT = 3;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(cacheCfg())
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(
                        new DataRegionConfiguration()
                            .setPersistenceEnabled(true)
                    )
            );
    }

    /**
     * @return Cache configuration.
     */
    private CacheConfiguration<?, ?> cacheCfg() {
        return new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAffinity(new RendezvousAffinityFunction(false, 16))
            .setBackups(1);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        cleanPersistenceDir();

        startGridsMultiThreaded(NODES_CNT)
            .addCacheConfiguration(cacheCfg());
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        List<String> cachesToDestroy = grid(0).cacheNames().stream()
            .filter(name -> !DEFAULT_CACHE_NAME.equals(name))
            .collect(Collectors.toList());

        if (!cachesToDestroy.isEmpty()) {
            grid(0).destroyCaches(cachesToDestroy);
            awaitPartitionMapExchange();
        }
    }

    @Test
    @WithSystemProperty(key = IGNITE_SQL_ALLOW_IMPLICIT_PK, value = "true")
    public void testBasicOperations() {
        // "WRAP_KEY" option is not supported.
        assertThrows(log, () -> querySql("CREATE TABLE integers(i INTEGER) WITH \"wrap_key=true\""),
            IgniteSQLException.class, null);

        // Table without columns. 
        assertThrows(log, () -> querySql("CREATE TABLE integers()"),
            IgniteSQLException.class, "Table must have at least one non PRIMARY KEY column");

        querySql("CREATE TABLE integers(i INTEGER)");

        int rowsCnt = 5;
        Set<Integer> expNums = new HashSet<>();

        for (int n = 0; n < rowsCnt; n++) {
            expNums.add(n);

            querySql("INSERT INTO integers VALUES(?)", n);
        }

        List<List<?>> rows = querySql("SELECT * FROM integers");
        assertEquals(rowsCnt, rows.size());

        Set<Integer> resSet = new HashSet<>();

        for (List<?> row : rows) {
            assertEquals(1, row.size());

            resSet.add((Integer)row.get(0));
        }

        assertEquals(expNums, resSet);
    }

    @Test
    @WithSystemProperty(key = IGNITE_SQL_ALLOW_IMPLICIT_PK, value = "true")
    public void testCacheApiCompatibility() {
        querySql("CREATE TABLE person(name VARCHAR, age INT) WITH \"value_type=" + Person.class.getName() + "\"");

        IgniteCache<UUID, Person> cache = grid(0).cache("SQL_PUBLIC_PERSON");

        // Prepare test data.
        String[] names = {"Hektor", "Emma", "Tom", "Gloria", "Brad", "Ann", "Will", "Courtney"};
        List<Person> persons = new ArrayList<>(names.length);

        for (String name : names)
            persons.add(new Person(name, ThreadLocalRandom.current().nextInt(100)));

        // Put part of the entries using the cache API.
        Set<UUID> expKeys = new HashSet<>();

        for (int i = 0; i < names.length / 2; i++) {
            UUID id = UUID.randomUUID();

            expKeys.add(id);

            cache.put(id, persons.get(i));
        }

        // Put another part using SQL API.
        for (int i = names.length / 2; i < names.length; i++) {
            Person person = persons.get(i);
            querySql("INSERT INTO person VALUES(?, ?)", person.getName(), person.getAge());
        }

        // Ensure all entries has been stored.
        List<List<?>> rows = querySql("SELECT _key, * FROM person");

        assertEquals(names.length, cache.size());
        assertEquals(names.length, rows.size());

        // Ensure all records are visible using SQL.
        Map<UUID, Person> sqlPersons = rows.stream()
            .collect(Collectors.toMap(l -> (UUID)l.get(0), l -> new Person((String)l.get(1), (Integer)l.get(2))));

        assertEquals(new HashSet<>(persons), new HashSet<>(sqlPersons.values()));
        assertTrue(sqlPersons.keySet().containsAll(expKeys));

        // Ensure all records are visible using cache API.
        Map<UUID, Person> cachePersons = new HashMap<>();

        for (Cache.Entry<UUID, Person> e : cache)
            cachePersons.put(e.getKey(), e.getValue());

        assertEquals(sqlPersons, cachePersons);
        assertTrue(cachePersons.keySet().containsAll(expKeys));

        for (Map.Entry<UUID, Person> p : sqlPersons.entrySet()) {
            assertEquals(cache.get(p.getKey()), p.getValue());
        }
    }

    @Test
    @WithSystemProperty(key = IGNITE_SQL_ALLOW_IMPLICIT_PK, value = "true")
    public void testNodeRestart() throws Exception {
        querySql("CREATE TABLE uuids(i UUID) with \"template=default\"");

        int dataCnt = 100;
        Set<UUID> expRows = new HashSet<>();
        BiConsumer<Integer, Integer> fillData = (off, cnt) -> {
            for (int i = off; i < cnt; i++) {
                UUID id = UUID.randomUUID();

                expRows.add(id);

                querySql("INSERT INTO uuids(i) VALUES(?)", id);
            }
        };

        // Create 50% of rows.
        fillData.accept(0, dataCnt / 2);

        stopAllGrids();

        System.clearProperty(IGNITE_SQL_ALLOW_IMPLICIT_PK);

        startGridsMultiThreaded(NODES_CNT);

        assertThrows(log, () -> querySql("CREATE TABLE test(i INT)"), IgniteSQLException.class, null);

        // Create remaining 50% of rows.
        fillData.accept(dataCnt / 2, dataCnt);

        // Stop some node and check the data.
        stopGrid(0);

        try {
            List<List<?>> rows = querySql("SELECT * FROM uuids");
            assertEquals(dataCnt, rows.size());

            Set<UUID> resRows = rows.stream().map(l -> (UUID)l.get(0)).collect(Collectors.toSet());
            assertEquals(expRows, resRows);
        } finally {
            startGrid(0);
        }
    }

    /**
     * @param sql SQL query.
     * @param args Arguments.
     * @return List of results.
     */
    private List<List<?>> querySql(String sql, Object... args) {
        IgniteCache<Object, Object> cache = F.first(G.allGrids()).cache(DEFAULT_CACHE_NAME);

        try (FieldsQueryCursor<List<?>> cur = cache.query(new SqlFieldsQuery(sql).setArgs(args))) {
            assertNotNull(cur);

            return cur.getAll();
        }
    }

    /** Person entity. */
    private static class Person implements Serializable {
        /** Person name. */
        @QuerySqlField
        private final String name;

        /** Person name. */
        @QuerySqlField
        private final Integer age;

        /**
         * @param name Person name.
         * @param age Person age.
         */
        public Person(String name, Integer age) {
            this.name = name;
            this.age = age;
        }

        /** @return Person name. */
        public String getName() {
            return name;
        }

        /** @return Person age. */
        public Integer getAge() {
            return age;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            Person person = (Person)o;

            return Objects.equals(name, person.name) && Objects.equals(age, person.age);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(name, age);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Person{" + "name='" + name + '\'' + ", age=" + age + '}';
        }
    }
}
