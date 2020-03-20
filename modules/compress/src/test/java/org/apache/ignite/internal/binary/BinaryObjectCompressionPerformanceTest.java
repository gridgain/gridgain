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

package org.apache.ignite.internal.binary;

import java.util.Date;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
public class BinaryObjectCompressionPerformanceTest extends GridCommonAbstractTest {
    /** */
    private static final long LARGE_PRIME = 4294967291L;

    /** */
    private static final int CYCLE = 100_000;

    /** */
    private static final int MAX_ITEMS = 3 * CYCLE;

    /** */
    @Test
    public void testPrimitiveKey() throws Exception {
        Ignite ignite = startGrid(0);

        IgniteCache c = ignite.createCache(new CacheConfiguration<>("foo"));
        long time = System.currentTimeMillis();
        for (int i = 0; i < MAX_ITEMS; i++) {
            long seed = (i * i) % LARGE_PRIME;
            c.put(seed, new TestObject(seed));

            if (i > CYCLE) {
                int idToRmv = i - CYCLE;

                c.remove((idToRmv * idToRmv) % LARGE_PRIME);
            }
        }

        log.info("Time took: " + ((System.currentTimeMillis() - time) / 1_000) + "s");

        ignite.close();
    }

    /** */
    @Test
    public void testCompositeKey() throws Exception {
        Ignite ignite = startGrid(0);

        IgniteCache c = ignite.createCache(new CacheConfiguration<>("foo"));
        long time = System.currentTimeMillis();
        for (int i = 0; i < CYCLE; i++) {
            long seed = (i * i) % LARGE_PRIME;
            c.put(new TestKey(seed), new TestObject(seed));

            if (i > CYCLE) {
                int idToRmv = i - CYCLE;

                c.remove(new TestKey((idToRmv * idToRmv) % LARGE_PRIME));
            }
        }

        log.info("Time took: " + ((System.currentTimeMillis() - time) / 1_000) + "s");

        ignite.close();
    }


    /** */
    private static class TestObject {
        /** */
        private byte bVal;

        /** */
        private char cVal;

        /** */
        private short sVal;

        /** */
        private int iVal;

        /** */
        private long lVal;

        /** */
        private float fVal;

        /** */
        private double dVal;

        /** */
        private String strVal;

        /** */
        private String dateVal;

        /**
         * @param seed Seed.
         */
        private TestObject(long seed) {
            bVal = (byte)seed;
            cVal = (char)seed;
            sVal = (short)seed;
            strVal = Long.toString(seed);
            iVal = (int)seed;
            lVal = seed;
            fVal = seed;
            dVal = seed;
            dateVal = new Date(seed).toString();
        }
    }

    /**
     *
     */
    @SuppressWarnings("UnusedDeclaration")
    private static class TestKey {
        /** */
        private String key;

        /** */
        @AffinityKeyMapped
        private int affKey;

        /**
         * @param seed Seed.
         */
        private TestKey(long seed) {
            key = Long.toHexString(seed);
            affKey = 0xfff & (int)seed;
        }
    }
}
