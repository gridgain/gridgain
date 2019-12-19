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

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteSemaphore;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.yardstick.IgniteAbstractBenchmark;
import org.yardstickframework.BenchmarkConfiguration;

import static org.yardstickframework.BenchmarkUtils.println;

/**
 * Ignite benchmark that performs query operations with joins.
 */
public class IgniteInlineIndexBenchmark extends IgniteAbstractBenchmark {
    /** Cache name for benchmark. */
    private String cacheName;

    /** */
    private Class<?> keyCls;

    /** WWhether ke should be type of Java object or simple (e.g. integer or long). */
    private boolean isJavaObj;

    /** Whether to run init step or skip it. */
    private boolean initStep;

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        initStep = args.getBooleanParameter("init", false);
        isJavaObj = args.getBooleanParameter("javaObject", false);

        if (isJavaObj) {
            cacheName = "CACHE_POJO";
            keyCls = TestKey.class;
        }
        else {
            cacheName = "CACHE_LONG";
            keyCls = Long.class;
        }

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
        ignite().createCache(
            new CacheConfiguration<Object, Integer>(cacheName)
                .setIndexedTypes(keyCls, Integer.class)
        );

        println(cfg, "Populate cache: " + cacheName + ", range: " + args.range());

        try (IgniteDataStreamer<Object, Integer> stream = ignite().dataStreamer(cacheName)) {
            stream.allowOverwrite(false);

            for (long k = 0; k < args.range(); ++k)
                stream.addData(nextKey(), ThreadLocalRandom.current().nextInt());
        }

        println(cfg, "Cache populated: " + cacheName);
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        ignite().<Object, Integer>cache(cacheName).put(nextKey(), ThreadLocalRandom.current().nextInt());

        return true;
    }

    /** Creates next random key. */
    private Object nextKey() {
        return isJavaObj
            ? new TestKey(ThreadLocalRandom.current().nextLong())
            : ThreadLocalRandom.current().nextLong();
    }

    /**
     * @param sql SQL query.
     * @param args Query parameters.
     * @return Results cursor.
     */
    private void sql(String sql, Object... args) {
        ((IgniteEx)ignite()).context().query().querySqlFields(new SqlFieldsQuery(sql).setArgs(args), false).getAll();
    }

    /**
     *
     */
    private void printParameters() {
        println("Benchmark parameter:");
        println("    init: " + initStep);
        println("    is JavaObject: " + isJavaObj);
    }

    static class TestKey {
        private final long id;

        public TestKey(long id) {
            this.id = id;
        }
    }
}
