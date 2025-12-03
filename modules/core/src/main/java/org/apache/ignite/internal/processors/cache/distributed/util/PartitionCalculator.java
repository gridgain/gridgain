/*
 * Copyright 2025 GridGain Systems, Inc. and Contributors.
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
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;

/**
 * Calculates a partition index based on collection of keys and defined {@link Strategy}.
 */
public class PartitionCalculator {
    /** Property name to define a strategy. */
    public static final String IGNITE_PART_CALCULATION_STRATEGY = "IGNITE_PART_CALC_STRATEGY";

    /** Default strategy. */
    public static final Strategy STRATEGY =
        Strategy.of(IgniteSystemProperties.getString(IGNITE_PART_CALCULATION_STRATEGY, Strategy.RANDOM.name()));

    public enum Strategy {
        /** First key defines a partition. */
        FIRST,

        /** Chose a random key to define a partition. */
        RANDOM,

        /**
         * Collects all partitions enlisted in an operation
         * and randomly calculates a result based on the collection of partitions.
         **/
        RANDOM2;

        static Strategy of(String val) {
            if (val != null && !val.isEmpty()) {
                for (Strategy s : Strategy.values()) {
                    if (s.name().equalsIgnoreCase(val))
                        return s;
                }
            }

            return RANDOM;
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
     * @param keys Collection of keys to define a partition index.
     * @param strategy Strategy that is used to calculate a partition index.
     * @return partition index.
     */
    public static int calculate(List<KeyCacheObject> keys, Strategy strategy) {
        if (keys == null || keys.isEmpty())
            return -1;

        switch (strategy) {
            case FIRST:
                return keys.get(0).partition();

            case RANDOM: {
                ThreadLocalRandom rnd = ThreadLocalRandom.current();
                return keys.get(rnd.nextInt(0, keys.size())).partition();
            }

            case RANDOM2: {
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
