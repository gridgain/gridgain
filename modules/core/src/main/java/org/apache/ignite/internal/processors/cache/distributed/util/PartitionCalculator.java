/*
 * Copyright 2026 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.cache.distributed.util;

import java.util.BitSet;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_PARTITION_CALCULATION_STRATEGY;
import static org.apache.ignite.IgniteSystemProperties.getString;

/**
 * Calculates a partition index based on collection of keys and defined {@link Strategy}.
 */
public class PartitionCalculator {
    /** The default strategy to calculate a partition index when it is not explicitly specified. */
    public static final Strategy DEFAULT_STRATEGY = Strategy.FIRST_KEY;

    /** The strategy that is used to calculate a partition index for atomic batch updates. */
    public static final Strategy STRATEGY =
        Strategy.of(getString(IGNITE_PARTITION_CALCULATION_STRATEGY, DEFAULT_STRATEGY.name()));

    /** Undefined partition number. */
    public static final int UNDEFINED_PARTITION = -1;

    public enum Strategy {
        /** First key defines a partition. */
        FIRST_KEY,

        /** Chose a random key to define a partition. */
        RANDOM_KEY,

        /**
         * Collects all partitions enlisted in an operation
         * and randomly calculates a result based on the collection of partitions.
         **/
        RANDOM_PARTITION_IN_BATCH;

        /**
         * Returns a strategy represented by the given {@code name}.
         * If the {@code val} is {@code null} or does not match any available strategies,
         * then returns {@link #DEFAULT_STRATEGY}.
         *
         * @param val Name of the strategy.
         * @return Strategy instance represented by {@code val}.
         */
        public static Strategy of(String val) {
            if (val != null && !val.isEmpty()) {
                for (Strategy s : Strategy.values()) {
                    if (s.name().equalsIgnoreCase(val))
                        return s;
                }
            }

            return DEFAULT_STRATEGY;
        }
    }

    /**
     * @param keys Collection of keys to define a partition index.
     * @return partition index.
     */
    public static int calculate(List<KeyCacheObject> keys) {
        return calculate(keys, STRATEGY);
    }

    /**
     * Calculates and returns a partition index based on the given {@code keys} and {@code strategy}.
     * Returns {@link #UNDEFINED_PARTITION} if the collection of keys is empty or {@code null}.
     *
     * @param keys Collection of keys to define a partition index.
     * @param strategy Strategy that is used to calculate a partition index.
     * @return partition index.
     * @throws IllegalArgumentException if the given {@code strategy} is not supported.
     */
    public static int calculate(List<KeyCacheObject> keys, Strategy strategy) {
        if (keys == null || keys.isEmpty())
            return UNDEFINED_PARTITION;

        switch (strategy) {
            case FIRST_KEY:
                return keys.get(0).partition();

            case RANDOM_KEY: {
                ThreadLocalRandom rnd = ThreadLocalRandom.current();
                return keys.get(rnd.nextInt(0, keys.size())).partition();
            }

            case RANDOM_PARTITION_IN_BATCH: {
                BitSet uniqueParts = new BitSet();

                for (int i = 0; i < keys.size(); ++i)
                    uniqueParts.set(keys.get(i).partition());

                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                int[] res = uniqueParts.stream().toArray();
                return res[rnd.nextInt(res.length)];
            }

            default:
                throw new IllegalArgumentException();
        }
    }
}
