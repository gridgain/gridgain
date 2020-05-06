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

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteSemaphore;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.yardstick.IgniteAbstractBenchmark;
import org.yardstickframework.BenchmarkConfiguration;

import static org.yardstickframework.BenchmarkUtils.println;

/**
 * Ignite benchmark that performs query operations with joins.
 */
public class IgniteInlineIndexBenchmark extends IgniteAbstractBenchmark {

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

    /* List of double keys generated before execution */
    private final List<Double> doubleKeys = new ArrayList<>();

    /** Cache name for benchmark. */
    private String cacheName;

    /** */
    private Class<?> keyCls;

    /** How many entries should be preloaded and within which range. */
    private int range;

    /** Type of the key object for the test */
    private TestedType testedType;

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        range = args.range();
        testedType = TestedType.valueOf(args.getStringParameter("testedType", TestedType.PRIMITIVE.toString()));

        switch (testedType) {
            case JAVA_OBJECT:
                cacheName = "CACHE_POJO";
                keyCls = TestKey.class;
                break;
            case PRIMITIVE:
                cacheName = "CACHE_LONG";
                keyCls = Integer.class;
                break;
            case DECIMAL:
                generateDoublekeys(doubleKeys);
                cacheName = "CACHE_DECIMAL";
                keyCls = BigDecimal.class;
                break;
            case LARGE_DECIMAL:
                generateDoublekeys(doubleKeys);
                cacheName = "CACHE_LARGE_DECIMAL";
                keyCls = BigDecimal.class;
                break;
            case SMALL_DECIMAL:
                generateDoublekeys(doubleKeys);
                cacheName = "CACHE_SMALL_DECIMAL";
                keyCls = BigDecimal.class;
                break;
            default:
                throw new Exception(testedType + "is not expected type");
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

    private void generateDoublekeys(List<Double> keys) {
        while (keys.size() < range)
            keys.add(ThreadLocalRandom.current().nextDouble(range));
    }

    /**
     *
     */
    private void init() {
        ignite().createCache(
            new CacheConfiguration<Object, Integer>(cacheName)
                .setIndexedTypes(keyCls, Integer.class)
        );

        println(cfg, "Populate cache: " + cacheName + ", range: " + range);

        try (IgniteDataStreamer<Object, Integer> stream = ignite().dataStreamer(cacheName)) {
            stream.allowOverwrite(false);

            for (long k = 0; k < range; ++k)
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
        switch (testedType) {
            case JAVA_OBJECT:
                return new TestKey(ThreadLocalRandom.current().nextInt(range));
            case PRIMITIVE:
                return ThreadLocalRandom.current().nextInt(range);
            case DECIMAL:
                return BigDecimal.valueOf(doubleKeys.get(ThreadLocalRandom.current().nextInt(range)));
            case LARGE_DECIMAL:
                return BigDecimal.valueOf(doubleKeys.get(ThreadLocalRandom.current().nextInt(range))).pow(3);
            case SMALL_DECIMAL:
                // low enough to use intCompact of BigDecimal
                return BigDecimal.valueOf(doubleKeys.get(ThreadLocalRandom.current().nextInt(range))).
                    divide(BigDecimal.TEN, RoundingMode.HALF_DOWN).setScale(2, RoundingMode.HALF_UP);
            default:
                return null;
        }
    }

    /** */
    private void printParameters() {
        println("Benchmark parameter:");
        println("    range: " + range);
        println("    testedType: " + testedType);
    }

    /** Pojo that used as key value for object's inlining benchmark. */
    static class TestKey {
        /** */
        private final int id;

        /** */
        public TestKey(int id) {
            this.id = id;
        }
    }
}
