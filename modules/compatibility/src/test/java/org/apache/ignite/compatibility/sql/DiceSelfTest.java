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

package org.apache.ignite.compatibility.sql;

import java.util.Arrays;
import java.util.Random;
import org.apache.ignite.compatibility.sql.randomsql.Dice;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.inRange;
import static org.junit.Assert.assertThat;

/** */
public class DiceSelfTest extends GridCommonAbstractTest {
    /**
     * Test verifies that dice returns random value in expected range.
     */
    @Test
    public void verifyDiceReturnsValuesInRange() {
        Dice dice = new Dice(new Random());

        for (int i = 0; i < 10_000; i++) {
            assertThat(dice.roll(), inRange(1, 6));
            assertThat(dice.rollTwice(), inRange(2, 12));

            int times = i / 100 + 1;
            assertThat(dice.rollNTimes(times), inRange(times, times * 6));
        }

        for (Integer i : Arrays.asList(0, -1)) {
            try {
                dice.rollNTimes(i);

                fail("exception won't be thrown");
            }
            catch (IllegalArgumentException ignored) {
                // legal exception, nothing to do
            }
        }
    }

    /**
     * Test verifies that you can't roll dice less than 1 time.
     */
    @Test
    public void diceValidationCheck() {
        try {
            new Dice(null);

            fail("exception won't be thrown");
        }
        catch (NullPointerException ignored) {
            // legal exception, nothing to do
        }

        Dice dice = new Dice(new Random());

        for (Integer i : Arrays.asList(0, -1, -5, -10)) {
            try {
                dice.rollNTimes(i);

                fail("exception won't be thrown");
            }
            catch (IllegalArgumentException ignored) {
                // legal exception, nothing to do
            }
        }

        assertThat(dice.rollNTimes(1), inRange(1, 6));
    }
}
