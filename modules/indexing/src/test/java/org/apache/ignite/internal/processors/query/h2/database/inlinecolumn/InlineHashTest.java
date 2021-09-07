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
package org.apache.ignite.internal.processors.query.h2.database.inlinecolumn;

import java.util.HashSet;
import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheKeyConfiguration;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_MAX_INDEX_PAYLOAD_SIZE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_SENSITIVE_DATA_LOGGING;
import static org.apache.ignite.internal.processors.query.h2.database.H2Tree.IGNITE_THROTTLE_INLINE_SIZE_CALCULATION;

/**
 *
 */
public class InlineHashTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                .setQueryEntities(asList(
                    new QueryEntity(KeyClass.class, ValClass.class)
                        .setKeyFields(new HashSet<>(asList("name", "int0")))
                    //.setIndexes(asList(new QueryIndex()))
                ))
                //.setKeyConfiguration(new CacheKeyConfiguration(KeyClass.class).setAffinityKeyFieldName("name"))
            );
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "plain")
    @WithSystemProperty(key = IGNITE_THROTTLE_INLINE_SIZE_CALCULATION, value = "3")
    @WithSystemProperty(key = IGNITE_MAX_INDEX_PAYLOAD_SIZE, value = "8")
    public void test() throws Exception {
        IgniteEx ignite = startGrids(1);

        IgniteCache<KeyClass, ValClass> cache = ignite.getOrCreateCache(DEFAULT_CACHE_NAME);

        //for (int i = 0; i < 1_000_000_000; i++)
        //for (int i = 0; i < 10; i++)
        //    cache.put(new KeyClass("k" + i, i), new ValClass("v" + i, i));

        cache.put(new KeyClass("k1117323", 1117323), new ValClass("v61626", 61626));
        cache.put(new KeyClass("k1117323", 1117323), new ValClass("v61626", 61626));
        cache.put(new KeyClass("k4059899", 4059899), new ValClass("v3738091", 3738091));

        List<List<?>> list = cache.query(new SqlFieldsQuery("select * from ValClass where val = ?").setArgs("v5")).getAll();

        if (!list.isEmpty())
            log.warning(list.get(0).toString());

        Object o = cache.query(new SqlFieldsQuery("select count(*) from ValClass")).getAll().get(0);

        log.warning(o.toString());
    }

    private static class KeyClass {
        @QuerySqlField
        private final String name;

        @QuerySqlField
        private final Integer int0;

        public KeyClass(String name, int int0) {
            this.name = name;
            this.int0 = int0;
        }
    }

    private static class ValClass {
        @QuerySqlField(index = true)
        private final String val;

        @QuerySqlField(index = true)
        private final Integer int1;

        public ValClass(String val, int i) {
            this.val = val;
            this.int1 = i;
        }
    }

}
