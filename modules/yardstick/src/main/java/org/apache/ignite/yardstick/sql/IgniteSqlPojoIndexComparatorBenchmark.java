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

package org.apache.ignite.yardstick.sql;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteSemaphore;
import org.apache.ignite.cache.CacheKeyConfiguration;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.yardstick.IgniteAbstractBenchmark;
import org.jetbrains.annotations.NotNull;
import org.yardstickframework.BenchmarkConfiguration;

import static org.yardstickframework.BenchmarkUtils.println;

/**
 * Ignite benchmark that performs query operations with joins.
 */
public class IgniteSqlPojoIndexComparatorBenchmark extends IgniteAbstractBenchmark {
    /** Cache name for benchmark. */
    private static final String CACHE_NAME = "CACHE_POJO";

    /** Template of SELECT query with filtering by indexed column. */
    private static final String SELECT_VALUE_TEMPLATE =
        "SELECT val, idxVal FROM \"%s\" USE INDEX(\"%s\") WHERE %s = ?";

    /** Index field name. */
    private static final String IDX_FIELD_NAME = "idxVal";

    /** Index name. */
    private static final String IDX_NAME = "IDXVAL_IDX";

    /** SQL Query. */
    private static final String SQL_QUERY = String.format(SELECT_VALUE_TEMPLATE, CACHE_NAME, IDX_NAME, IDX_FIELD_NAME);

    /** How many entries should be preloaded and within which range. */
    private final int range = args.range();


    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        printParameters();

        IgniteSemaphore sem = ignite().semaphore("setup", 1, true, true);

        try {
            if (sem.tryAcquire()) {
                println(cfg, "Create tables...");

                init();
            }
            else {
                // Acquire (wait setup by other client) and immediately release/
                println(cfg, "Waits for setup...");

                sem.acquire();
            }
        }
        finally {
            sem.release();
        }
    }

    /**
     *
     */
    private void init() {
        Class<?> keyCls = TestKeyWithIdx.class;
        Class<?> idxCls = TestPojo.class;

        // Create cache
        LinkedHashMap<String, String> fields = new LinkedHashMap<>(2);

        fields.put("idxVal", idxCls.getName());
        fields.put("val", Integer.class.getName());

        QueryEntity qe = new QueryEntity(keyCls.getName(), Integer.class.getName())
            .setTableName(CACHE_NAME)
            .setValueFieldName("val")
            .setFields(fields);

        qe.setKeyFields(Collections.singleton(IDX_FIELD_NAME));
        qe.setIndexes(Collections.singleton(new QueryIndex(IDX_FIELD_NAME, true, IDX_NAME)));

        ignite().createCache(
            new CacheConfiguration<Object, Integer>(CACHE_NAME)
                .setKeyConfiguration(new CacheKeyConfiguration((TestKeyWithIdx.class).getName(), "idxVal"))
                .setQueryEntities(Collections.singletonList(qe)).setSqlSchema("PUBLIC"));

        println(cfg, "Populate cache: " + CACHE_NAME + ", range: " + range);

        try (IgniteDataStreamer<Object, Integer> stream = ignite().dataStreamer(CACHE_NAME)) {
            stream.allowOverwrite(false);

            for (long k = 0; k < range; ++k)
                stream.addData(nextKey(), ThreadLocalRandom.current().nextInt());
        }

        println(cfg, "Cache populated: " + CACHE_NAME);
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        Object key = nextKey();

        ignite().<Object, Integer>cache(CACHE_NAME).put(key, ThreadLocalRandom.current().nextInt());

        List<List<?>> sqlRes = ignite().cache(CACHE_NAME).query(new SqlFieldsQuery(SQL_QUERY).setArgs(key)).getAll();

        return true;
    }

    /** Creates next random key. */
    private Object nextKey() {
        return new TestPojo(ThreadLocalRandom.current().nextInt(range));
    }

    /** */
    private void printParameters() {
        println("Benchmark parameter:");
        println("    range: " + range);
    }

    /**
     * Test class for verify index over Java Object.
     */
    private static class TestPojo implements Comparable<TestPojo> {
        /** Value. */
        private final int val;

        /** */
        public TestPojo(int val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public int compareTo(@NotNull TestPojo o) {
            if (o == null)
                return 1;

            return Integer.compare(val, o.val);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            TestPojo pojo = (TestPojo)o;

            return val == pojo.val;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(val);
        }
    }

    /**
     * Test class for use like cache key with additional indexed column.
     *
     * @param <T> Type of the additional indexed column.
     */
    private static class TestKeyWithIdx<T> {
        /** Value. */
        private final int val;

        /** Indexed value. */
        private final T idxVal;

        /** */
        public TestKeyWithIdx(int val, T idxVal) {
            this.val = val;
            this.idxVal = idxVal;
        }
    }
}
