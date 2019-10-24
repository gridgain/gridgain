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
package org.apache.ignite.internal.processors.cache;

import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.junit.Test;

/**
 * TODO: Add class description.
 */
public class IgniteCacheQueryLargeRecordTest extends GridCacheAbstractSelfTest{

    @Test
    public void testMemoryLeak() {
        IgniteCache<Object, Object> cache = grid(0).cache(DEFAULT_CACHE_NAME);
        for (long i = 0; i < 1000; i++) {
            Person value = new Person(new byte[1024 * 1024]);

            cache.put(i, value);
        }

        System.out.println("Loaded");

        for (int i = 0; i < 10_000; i++) {

            if (i == 10)
                System.out.println("Ads");

            FieldsQueryCursor<List<?>> query =
                cache.query(new SqlFieldsQuery("select * from Person limit 10").setLazy(true));
            query.iterator().next();
            query.close();
            System.out.println(i);
        }

    }

    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);
        cfg.setCacheConfiguration(
            new CacheConfiguration(DEFAULT_CACHE_NAME).setIndexedTypes(Long.class, Person.class));
                             //       .setIndexedTypes(MyKey.class, MyValue.class)
        cfg.setDataStorageConfiguration(new DataStorageConfiguration().setMetricsEnabled(true)
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setMetricsEnabled(true)));
        return cfg;
    }

    @Override protected int gridCount() {
        return 1;
    }

    @Override protected long getTestTimeout() {
        return 120 * 1000; // TODO: CODE: implement.
    }

    private static class Person {
        byte[] b;
        public Person(byte[] b) {
            this.b = b;
        }
    }
}
