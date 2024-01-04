/*
 * Copyright 2021 GridGain Systems, Inc. and Contributors.
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

import static org.apache.ignite.IgniteSystemProperties.IGNITE_SENSITIVE_DATA_LOGGING;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
public class EnumClassImplementingIndexedInterfaceTest extends GridCommonAbstractTest {
    /** */
    private static final String PERSON_CACHE = "Person";

    /** */
    private static IgniteEx ignite;

    /** */
    private static final int KEY = 0;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        ignite = startGrids(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        ignite.destroyCache(PERSON_CACHE);
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "hash")
    public void testInsertTableVarColumns() {
        checkCachePutInsert(startSqlPersonCache());
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "hash")
    public void testInsertValueVarColumns() {
        checkCachePutInsert(startPersonCache());
    }

    /** */
    private void checkCachePutInsert(IgniteCache<Integer, Person> cache) {
        SqlFieldsQuery sqlSelect = new SqlFieldsQuery("SELECT CONVERT(role, varchar), CONVERT(title, varchar) from " + PERSON_CACHE);

        Arrays.stream(RoleEnum.values()).forEach(role -> {
            Title title = role.ordinal() % 2 == 0 ? new TitleClass1() : new TitleClass2();
            Person person = new Person(role, title, role.toString());

            cache.put(KEY, person);
            assertEquals(person, cache.get(KEY));

            cache.clear();

            cache.query(sqlInsertQuery(role, title, role.toString()));
            assertEquals(person, cache.get(KEY));

            List<List<?>> res = cache.query(sqlSelect).getAll();

            Object checkEnum = res.get(0).get(0);
            assertEquals(role.toString(), checkEnum);

            Object checkCustom = res.get(0).get(1);
            assertEquals(title.toString(), checkCustom);

            cache.clear();
        });
    }

    /** */
    private IgniteCache<Integer, Person> startPersonCache() {
        return ignite.createCache(new CacheConfiguration<Integer, Person>()
            .setName(PERSON_CACHE)
            .setQueryEntities(Collections.singletonList(personQueryEntity())));
    }

    /** */
    private IgniteCache<Integer, Person> startSqlPersonCache() {
        ignite.context().query().querySqlFields(new SqlFieldsQuery(
            "create table " + PERSON_CACHE + "(" +
            "   id int PRIMARY KEY," +
            "   role Object," +
            "   title Object," +
            "   desc varchar(5)" +
            ") with \"CACHE_NAME=" + PERSON_CACHE + ",VALUE_TYPE=" + Person.class.getName() + "\""), false);

        return ignite.cache(PERSON_CACHE);
    }

    /** */
    private SqlFieldsQuery sqlInsertQuery(Role role, Title title, String description) {
        return new SqlFieldsQuery("insert into " + PERSON_CACHE + "(id, role, title, desc) values (?, ?, ?, ?)")
            .setArgs(KEY, role, title, description);
    }

    /** */
    private QueryEntity personQueryEntity() {
        QueryEntity entity = new QueryEntity(Integer.class, Person.class);
        entity.setKeyFieldName("id");
        entity.addQueryField("id", Integer.class.getName(), "ID");

        return entity;
    }

    /** */
    interface Role {
    }

    /** */
    enum RoleEnum implements Role {
        /** */
        ROLE1,

        /** */
        ROLE2,

        /** */
        ROLE3
    }

    /** */
    interface Title {
        /** */
        public String getTitle();
    }

    /** */
    static class TitleClass1 implements Title {
        /** */
        private final String title1 = "title1";

        /** */
        @Override public String getTitle() {
            return title1;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            TitleClass1 titleCls = (TitleClass1)o;

            return Objects.equals(title1, titleCls.title1);
        }

        /** */
        @Override public int hashCode() {
            return 0;
        }

        /** */
        @Override public String toString() {
            return title1;
        }
    }

    /** */
    static class TitleClass2 implements Title {
        /** */
        private final String title2 = "title2";

        /** */
        @Override public String getTitle() {
            return title2;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            TitleClass2 titleCls = (TitleClass2)o;

            return Objects.equals(title2, titleCls.title2);
        }

        /** */
        @Override public int hashCode() { return 1; }

        /** */
        @Override public String toString() {
            return title2;
        }
    }

    /** */
    static class Person {
        /** */
        @QuerySqlField(index = true)
        private final Role role;

        /** */
        @QuerySqlField(index = true)
        private final Title title;

        /** */
        @QuerySqlField
        private final String desc;

        /** */
        Person(Role role, Title title, String desc) {
            this.role = role;
            this.title = title;
            this.desc = desc;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            Person person = (Person)o;

            return Objects.equals(role, person.role) && Objects.equals(title, person.title) && Objects.equals(desc,
                    person.desc);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int result = Objects.hash(role);
            result = 31 * result + Objects.hash(title);
            result = 31 * result + Objects.hash(desc);
            return result;
        }
    }
}
