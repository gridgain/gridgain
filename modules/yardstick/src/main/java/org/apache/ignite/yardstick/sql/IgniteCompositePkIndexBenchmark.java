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

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSemaphore;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.yardstick.IgniteAbstractBenchmark;
import org.yardstickframework.BenchmarkConfiguration;

import static org.yardstickframework.BenchmarkUtils.println;

/**
 * Ignite benchmark for composite PK.
 */
public class IgniteCompositePkIndexBenchmark extends IgniteAbstractBenchmark {
    /** Cache name. */
    private static final String cacheName = "TEST";

    /**
     * Enum of key types possible for testing
     */
    private enum TestedType {
        /** */
        JAVA_OBJECT,
        /** */
        PRIMITIVE,
        /** */
        DECIMAL,
        /** */
        LARGE_DECIMAL,
        /** */
        SMALL_DECIMAL
    }

    /** How many entries should be preloaded and within which range. */
    private int range;

    /** Key class. */
    private Class<?> keyCls;

    /** Value class. */
    private Class<?> valCls;

    private Function<Integer, Object> keyCreator;

    private Function<Integer, Object> valCreator;

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        range = args.range();
        String keyClsName = args.getStringParameter("keyClass", TestKey2Integers.class.getSimpleName());
        String valClsName = args.getStringParameter("valClass", Value.class.getSimpleName());

        keyCls = Class.forName(IgniteCompositePkIndexBenchmark.class.getName() + "$" + keyClsName);
        final Constructor<?> keyConstructor = keyCls.getConstructor(int.class);

        keyCreator = (key) -> {
            try {
                return keyConstructor.newInstance(key);
            }
            catch (Exception e) {
                throw new IgniteException("Unexpected exception", e);
            }
        };

        valCls = Class.forName(IgniteCompositePkIndexBenchmark.class.getName() + "$" + valClsName);
        final Constructor<?> valConstructor = valCls.getConstructor(int.class);

        valCreator = (key) -> {
            try {
                return valConstructor.newInstance(key);
            }
            catch (Exception e) {
                throw new IgniteException("Unexpected exception", e);
            }
        };

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
            new CacheConfiguration(cacheName)
                .setSqlSchema("PUBLIC")
                .setQueryEntities(Collections.singleton(
                    new QueryEntity(keyCls, valCls)
                        .setTableName("TEST")
                )
            )
        );

        println(cfg, "Populate cache, range: " + range);

        try (IgniteDataStreamer<Object, Object> stream = ignite().dataStreamer(cacheName)) {
            stream.allowOverwrite(false);

            for (int k = 0; k < range; ++k)
                stream.addData(keyCreator.apply(k), valCreator.apply(k));
        }

        println(cfg, "Cache populated. ");
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        int k = ThreadLocalRandom.current().nextInt(range);
        int v = ThreadLocalRandom.current().nextInt(range);

        ignite().cache(cacheName).put(keyCreator.apply(k), valCreator.apply(v));

        return true;
    }


    /** */
    private void printParameters() {
        println("Benchmark parameter:");
        println("    range: " + range);
        println("    key: " + keyCls.getSimpleName());
        println("    val: " + valCls.getSimpleName());
    }

    /** */
    public static class TestKey2Integers {
        /** */
        @QuerySqlField
        private final int id0;

        @QuerySqlField
        private final int id1;

        /** */
        public TestKey2Integers(int key) {
            this.id0 = 0;
            this.id1 = key;
        }
    }

    /** */
    public static class TestKeyHugeStringAndInteger {
        /** Prefix. */
        private static final String PREFIX = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" +
            "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" +
            "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

        /** */
        @QuerySqlField
        private final String id0;

        @QuerySqlField
        private final int id1;

        /** */
        public TestKeyHugeStringAndInteger(int key) {
            this.id0 = PREFIX + key;
            this.id1 = key;
        }
    }

    /** */
    public static class TestKey8Integers {
        /** */
        @QuerySqlField
        private final int id0;

        @QuerySqlField
        private final int id1;

        @QuerySqlField
        private final int id2;

        @QuerySqlField
        private final int id3;

        @QuerySqlField
        private final int id4;

        @QuerySqlField
        private final int id5;

        @QuerySqlField
        private final int id6;

        @QuerySqlField
        private final int id7;

        /** */
        public TestKey8Integers(int key) {
            this.id0 = 0;
            this.id1 = key / 100_000;
            this.id2 = key / 10_000;
            this.id3 = key / 1_000;
            this.id4 = key / 1000;
            this.id5 = key / 100;
            this.id6 = key / 10;
            this.id7 = key;
        }
    }

    /** */
    public static class Value {
        /** */
        @QuerySqlField
        private final int valInt;

        @QuerySqlField
        private final String valStr;

        /** */
        public Value(int key) {
            this.valInt = key;
            this.valStr = "val_str" + key;
        }
    }

    /** */
    public static class ValueIndexed {
        /** */
        @QuerySqlField(index = true)
        private final int valInt;

        @QuerySqlField(index = true)
        private final String valStr;

        /** */
        public ValueIndexed(int key) {
            this.valInt = key;
            this.valStr = "val_str" + key;
        }
    }
}
