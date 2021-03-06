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

import java.util.Arrays;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

import static java.util.Collections.singletonList;
import static org.apache.ignite.compatibility.sql.model.Country.Factory.COUNTRY_CNT;
import static org.apache.ignite.compatibility.sql.model.ModelUtil.randomString;

/**
 * City model.
 */
public class City {
    /** */
    @QuerySqlField
    private final String name;

    /** */
    @QuerySqlField
    private final String zipCode;

    /** */
    @QuerySqlField
    private final int countryId;

    /** */
    @QuerySqlField
    private final int population;

    /** */
    public City(String name, String zipCode, int countryId, int population) {
        this.name = name;
        this.zipCode = zipCode;
        this.countryId = countryId;
        this.population = population;
    }

    /** */
    public String name() {
        return name;
    }

    /** */
    public String zipCode() {
        return zipCode;
    }

    /** */
    public int countryId() {
        return countryId;
    }

    /** */
    public int population() {
        return population;
    }

    /** */
    public static class Factory extends AbstractModelFactory<City> {
        /** */
        private static final String TABLE_NAME = "city";

        /** */
        public static final int CITY_CNT = 100;

        /** */
        public Factory() {
            super(
                new QueryEntity(Long.class, City.class)
                    .setKeyFieldName("id")
                    .addQueryField("id", Long.class.getName(), null)
                    .setIndexes(
                        singletonList(new QueryIndex(Arrays.asList("countryId", "zipCode"), QueryIndexType.SORTED))
                    )
                    .setTableName(TABLE_NAME)
            );
        }

        /** {@inheritDoc} */
        @Override public City createRandom() {
            return new City(
                randomString(rnd, 5, 10), // name
                randomString(rnd, 5, 5), // zip code
                rnd.nextInt(COUNTRY_CNT), // country id
                rnd.nextInt(1_000) // population
            );
        }

        /** {@inheritDoc} */
        @Override public String tableName() {
            return TABLE_NAME;
        }

        /** {@inheritDoc} */
        @Override public int count() {
            return CITY_CNT;
        }
    }
}
