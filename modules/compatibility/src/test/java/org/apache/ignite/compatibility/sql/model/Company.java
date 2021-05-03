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
import java.util.Collections;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

import static org.apache.ignite.compatibility.sql.model.City.Factory.CITY_CNT;
import static org.apache.ignite.compatibility.sql.model.ModelUtil.randomString;

/**
 * Company model.
 */
public class Company {
    /** */
    @QuerySqlField
    private final String name;

    /** */
    @QuerySqlField
    private final String addr;

    /** */
    @QuerySqlField
    private final int headCnt;

    /** */
    @QuerySqlField
    private final int cityId;

    /** */
    public Company(String name, String addr, int headCnt, int cityId) {
        this.name = name;
        this.addr = addr;
        this.headCnt = headCnt;
        this.cityId = cityId;
    }

    /** */
    public String name() {
        return name;
    }

    /** */
    public String address() {
        return addr;
    }

    /** */
    public int headCount() {
        return headCnt;
    }

    /** */
    public int cityId() {
        return cityId;
    }

    /** */
    public static class Factory extends AbstractModelFactory<Company> {
        /** */
        private static final String TABLE_NAME = "company";

        /** */
        public static final int COMPANY_CNT = 500;

        /** */
        public Factory() {
            super(
                new QueryEntity(Long.class, Company.class)
                    .setKeyFieldName("id")
                    .addQueryField("id", Long.class.getName(), null)
                    .setIndexes(Arrays.asList(
                        new QueryIndex(Collections.singletonList("name"), QueryIndexType.SORTED),
                        new QueryIndex(Arrays.asList("cityId", "addr"), QueryIndexType.SORTED),
                        new QueryIndex(Arrays.asList("id", "cityId", "addr"), QueryIndexType.SORTED)
                    ))
                    .setTableName(TABLE_NAME)
            );
        }

        /** {@inheritDoc} */
        @Override public Company createRandom() {
            return new Company(
                randomString(rnd, 5, 10), // name
                randomString(rnd, 10, 30), // address
                rnd.nextInt(1_000), // head count
                rnd.nextInt(CITY_CNT) // city id
            );
        }

        /** {@inheritDoc} */
        @Override public String tableName() {
            return TABLE_NAME;
        }

        /** {@inheritDoc} */
        @Override public int count() {
            return COMPANY_CNT;
        }
    }
}
