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

package org.apache.ignite.compatibility.sql.randomsql;

import java.util.Objects;
import java.util.Random;

/**
 * Classic dice with 6 sides.
 */
public class Dice {
    /** */
    private static final int SIDES_COUNT = 6;

    /** */
    private final Random rnd;

    /**
     * @param rnd Random.
     */
    public Dice(Random rnd) {
        this.rnd = Objects.requireNonNull(rnd);
    }

    /**
     * Roll this dice one times.
     *
     * @return Random value within interval [1..6].
     */
    public int roll() {
        return rollNTimes(1);
    }

    /**
     * Roll this dice twice.
     *
     * @return Random value within interval [2..12].
     */
    public int rollTwice() {
        return rollNTimes(2);
    }

    /**
     * Roll this dice {@code N} times.
     *
     * @param n Amount of time to roll dice. Should be greater than 0.
     * @return Random value within interval [N..N*6].
     */
    public int rollNTimes(int n) {
        if (n <= 0)
            throw new IllegalArgumentException("'n' should be greater than 0");

        return n + rnd.nextInt(n * (SIDES_COUNT - 1) + 1);
    }
}
