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

import static org.apache.ignite.compatibility.sql.model.City.Factory.CITY_CNT;
import static org.apache.ignite.compatibility.sql.model.Company.Factory.COMPANY_CNT;
import static org.apache.ignite.compatibility.sql.model.ModelUtil.randomString;

/**
 * Department model.
 */
public class Department {
    /** */
    @QuerySqlField
    private final String name;

    /** */
    @QuerySqlField
    private final int headCnt;

    /** */
    @QuerySqlField
    private final int cityId;

    /** */
    @QuerySqlField
    private final int companyId;

    /** */
    public Department(String name, int headCnt, int cityId, int companyId) {
        this.name = name;
        this.headCnt = headCnt;
        this.cityId = cityId;
        this.companyId = companyId;
    }

    /** */
    public String name() {
        return name;
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
    public int companyId() {
        return companyId;
    }

    /** */
    public static class Factory extends AbstractModelFactory<Department> {
        /** */
        private static final String TABLE_NAME = "department";

        /** */
        public static final int DEPS_CNT = 1000;

        /** */
        public Factory() {
            super(
                new QueryEntity(Long.class, Department.class)
                    .setKeyFieldName("id")
                    .addQueryField("id", Long.class.getName(), null)
                    .setIndexes(Arrays.asList(
                        new QueryIndex("companyId", QueryIndexType.SORTED),
                        new QueryIndex(Arrays.asList("cityId", "headCnt"), QueryIndexType.SORTED),
                        new QueryIndex(Arrays.asList("companyId", "cityId", "headCnt"), QueryIndexType.SORTED)
                    ))
                    .setTableName(TABLE_NAME)
            );
        }

        /** {@inheritDoc} */
        @Override public Department createRandom() {
            return new Department(
                randomString(rnd, 5, 10), // name
                rnd.nextInt(20), // head count
                rnd.nextInt(CITY_CNT), // city id
                rnd.nextInt(COMPANY_CNT) // company id
            );
        }

        /** {@inheritDoc} */
        @Override public String tableName() {
            return TABLE_NAME;
        }

        /** {@inheritDoc} */
        @Override public int count() {
            return DEPS_CNT;
        }
    }
}
