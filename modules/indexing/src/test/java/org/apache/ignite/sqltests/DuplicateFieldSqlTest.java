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

package org.apache.ignite.sqltests;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.junit.Test;

import java.util.AbstractMap.SimpleEntry;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

/** Test SQL statements on a cache where Key and Value types have fields with same name. */
public class DuplicateFieldSqlTest {
    /** Cache name. */
    private static final String CACHE_NAME = "CACHE";

    /** Table name. */
    private static final String TBL_NAME = CACHE_NAME + "." + Person.class.getSimpleName();

    /**
     * SQL <code>INSERT INTO CACHE</code> with Key and Value fields having same name initializes both the Key and
     * Value fields.
     */
    @Test
    public void insertingIntoKeyAndValueFieldsWithSameNameInitializesBothFields() {
        try (Ignite ignite = Ignition.start(igniteConfiguration())) {
            IgniteCache<PersonKey, Person> cache = ignite.createCache(cacheConfiguration());

            final PersonKey K = new PersonKey(1, "11111");
            final String NAME = "Name1";

            cache.query(
                new SqlFieldsQuery("INSERT INTO " + TBL_NAME + " (ID, PASSPORTNO, NAME) VALUES (?, ?, ?)")
                    .setArgs(K.id, K.passportNo, NAME)
            ).getAll();

            Person v = cache.get(K);

            assertEquals(K.passportNo, v.passportNo);
            assertEquals(NAME, v.name);
        }
    }

    /**
     * SQL <code>SELECT FROM CACHE</code> with Key and Value fields having same name extracts the field belonging
     * to Value.
     */
    @Test
    public void queryingKeyAndValueFieldsWithSameNameDifferentValuesFails() {
        try (Ignite ignite = Ignition.start(igniteConfiguration())) {
            IgniteCache<PersonKey, Person> cache = ignite.createCache(cacheConfiguration());

            final PersonKey K = new PersonKey(1, "11111");
            final Person V = new Person("22222", "Name1");

            cache.put(K, V);

            List<?> row = cache
                .query(new SqlFieldsQuery("SELECT ID, PASSPORTNO, NAME FROM " + TBL_NAME))
                .getAll()
                .get(0);

            assertEquals(K.id, row.get(0));
            assertEquals(V.passportNo, row.get(1));
            assertEquals(V.name, row.get(2));
        }
    }

    /** Common Ignite configuration. */
    private static IgniteConfiguration igniteConfiguration() {
        return new IgniteConfiguration()
            .setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(
                new TcpDiscoveryVmIpFinder().setAddresses(Collections.singleton("127.0.0.1:47500"))
            ));
    }

    /** Cache configuration with Key and Value types having fields with same name exposed to SQL. */
    private static CacheConfiguration<PersonKey, Person> cacheConfiguration() {
        return new CacheConfiguration<PersonKey, Person>(CACHE_NAME)
            .setQueryEntities(Collections.singleton(
                new QueryEntity(PersonKey.class, Person.class)
                    .setFields(
                        Stream.of(
                            new SimpleEntry<>("id", int.class.getName()),
                            new SimpleEntry<>("passportNo", String.class.getName()),
                            new SimpleEntry<>("name", String.class.getName())
                        ).collect(Collectors.toMap(
                            SimpleEntry::getKey,
                            SimpleEntry::getValue,
                            (u, v) -> {
                                throw new IllegalStateException(String.format("Duplicate field of type %s", u));
                            },
                            LinkedHashMap::new
                        ))
                    )
                    .setKeyFields(Collections.singleton("id"))
                    .setKeyValueFields(Collections.singleton("passportNo"))
            ));
    }

    /** Key */
    private static final class PersonKey {
        /** ID */
        private final int id;

        /** Passport Number */
        private final String passportNo;

        /** Constructor */
        PersonKey(int id, String passportNo) {
            this.id = id;
            this.passportNo = passportNo;
        }
    }

    /** Value */
    private static final class Person {
        /** Passport Number */
        private final String passportNo;

        /** Name */
        private final String name;

        /** Constructor */
        Person(String passportNo, String name) {
            this.passportNo = passportNo;
            this.name = name;
        }
    }
}