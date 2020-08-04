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

import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

import static java.util.Collections.singletonList;
import static org.apache.ignite.compatibility.sql.model.ModelUtil.randomString;

/**
 * Country model.
 */
public class Country {
    /** */
    @QuerySqlField
    private final String name;

    /** */
    @QuerySqlField
    private final String phoneCode;

    /** */
    @QuerySqlField
    private final int population;

    /** */
    public Country(String name, String phoneCode, int population) {
        this.name = name;
        this.phoneCode = phoneCode;
        this.population = population;
    }

    /** */
    public String name() {
        return name;
    }

    /** */
    public String phoneCode() {
        return phoneCode;
    }

    /** */
    public int population() {
        return population;
    }

    /** */
    public static class Factory extends AbstractModelFactory<Country> {
        /** */
        private static final String TABLE_NAME = "country";

        /** */
        public static final int COUNTRY_CNT = 50;

        /** */
        public Factory() {
            super(
                new QueryEntity(Long.class, Country.class)
                    .setKeyFieldName("id")
                    .addQueryField("id", Long.class.getName(), null)
                    .setIndexes(
                        singletonList(new QueryIndex(singletonList("name"), QueryIndexType.SORTED))
                    )
                    .setTableName(TABLE_NAME)
            );
        }

        /** {@inheritDoc} */
        @Override public Country createRandom() {
            return new Country(
                randomString(rnd, 5, 10), // name
                randomString(rnd, 3, 3), // phone code
                rnd.nextInt(1_000) // population
            );
        }

        /** {@inheritDoc} */
        @Override public String tableName() {
            return TABLE_NAME;
        }

        /** {@inheritDoc} */
        @Override public int count() {
            return COUNTRY_CNT;
        }
    }
}
