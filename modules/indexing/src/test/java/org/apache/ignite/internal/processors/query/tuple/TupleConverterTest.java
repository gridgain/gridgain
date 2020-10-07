/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.tuple;

import java.util.Collections;
import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class TupleConverterTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDataRegionConfigurations(
                    new DataRegionConfiguration()
                        .setName("persisted")
                        .setPersistenceEnabled(true)
                ));

        CacheConfiguration ccfg = new CacheConfiguration("test")
            .setDataRegionName("persisted")
            .setIndexedTypes(Long.class, ValueType.class);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    @Override
    protected void beforeTestsStarted() throws Exception {
        cleanPersistenceDir();
    }

    @Test
    public void testInsertSelect() throws Exception {
        IgniteEx ig = startGrid(0);

        ig.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Long, ValueType> cache = ig.cache("test");

        for (int i = 0; i < 1000; i++) {
            cache.put((long)i, new ValueType(
                i,
                i,
                i % 2 == 0 ? null : i,
                i % 2 == 0 ? null : (long)i,
                i % 2 == 0 ? null : "value-" + i));
        }

        List<List<?>> res = cache.query(new SqlFieldsQuery("select " +
            "intVal, longVal, boxedIntVal, boxedLongVal, strVal " +
            "from ValueType")).getAll();

        for (List<?> row : res)
            System.out.println(row);
    }

    private static class ValueType {
        @QuerySqlField
        private int intVal;
        @QuerySqlField
        private long longVal;

        @QuerySqlField
        private Integer boxedIntVal;
        @QuerySqlField
        private Long boxedLongVal;

        @QuerySqlField
        private String strVal;

        public ValueType(
            int intVal,
            long longVal,
            Integer boxedIntVal,
            Long boxedLongVal,
            String strVal
        ) {
            this.intVal = intVal;
            this.longVal = longVal;
            this.boxedIntVal = boxedIntVal;
            this.boxedLongVal = boxedLongVal;
            this.strVal = strVal;
        }
    }
}
