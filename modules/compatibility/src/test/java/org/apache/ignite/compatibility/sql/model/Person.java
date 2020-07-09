/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.compatibility.sql.model;

import java.util.Collections;
import java.util.Random;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

import static org.apache.ignite.compatibility.sql.model.City.Factory.CITY_CNT;
import static org.apache.ignite.compatibility.sql.model.Department.Factory.DEPS_CNT;
import static org.apache.ignite.compatibility.sql.model.ModelUtil.randomString;

/**
 * Person model.
 */
public class Person {
    /** */
    @QuerySqlField
    private final String name;

    /** */
    @QuerySqlField
    private final int depId;

    /** */
    @QuerySqlField
    private final int age;

    /** */
    @QuerySqlField
    private final int cityId;

    /** */
    @QuerySqlField
    private final String position;

    /** */
    public Person(String name, int depId, int age, int cityId, String position) {
        this.name = name;
        this.depId = depId;
        this.age = age;
        this.cityId = cityId;
        this.position = position;
    }

    /** */
    public String name() {
        return name;
    }

    /** */
    public int depId() {
        return depId;
    }

    /** */
    public int age() {
        return age;
    }

    /** */
    public int cityId() {
        return cityId;
    }

    /** */
    public String position() {
        return position;
    }

    /** */
    public static class Factory implements ModelFactory {
        /** */
        private static final String TABLE_NAME = "person";

        /** */
        private static final int PERSON_CNT = 10_000;

        /** */
        private Random rnd;

        /** */
        private final QueryEntity qryEntity;

        /** */
        public Factory() {
            rnd = new Random();

            qryEntity = new QueryEntity(Long.class, Person.class)
                .setKeyFieldName("id")
                .addQueryField("id", Long.class.getName(), null)
                .setIndexes(
                    Collections.singleton(new QueryIndex("depId", QueryIndexType.SORTED))
                )
                .setTableName(TABLE_NAME);
        }

        /** {@inheritDoc} */
        @Override public void init(int seed) {
            rnd = new Random(seed);
        }

        /** {@inheritDoc} */
        @Override public Person createRandom() {
            return new Person(
                randomString(rnd, 1, 10), // name
                rnd.nextInt(DEPS_CNT), // depId
                rnd.nextInt(60), // age
                rnd.nextInt(CITY_CNT), // cityId
                randomString(rnd, 0, 10) // position
            );
        }

        /** {@inheritDoc} */
        @Override public QueryEntity queryEntity() {
            return qryEntity;
        }

        /** {@inheritDoc} */
        @Override public String tableName() {
            return TABLE_NAME;
        }

        /** {@inheritDoc} */
        @Override public int count() {
            return PERSON_CNT;
        }
    }
}
