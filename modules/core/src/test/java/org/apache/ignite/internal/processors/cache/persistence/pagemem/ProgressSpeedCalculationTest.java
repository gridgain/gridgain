/*
 * Copyright 2021 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.cache.persistence.pagemem;

import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

/**
 * Tests for {@link ProgressSpeedCalculation}.
 */
public class ProgressSpeedCalculationTest {
    /** The object under test. */
    private final ProgressSpeedCalculation calculation = new ProgressSpeedCalculation();

    /***/
    @Test
    public void getOpsPerSecondCalculatesCorrectSpeed() {
        calculation.setProgress(1000, 0);

        assertThat(calculation.getOpsPerSecond(1_000_000_000), is(1000L));
    }

    /***/
    @Test
    public void getOpsPerSecondShouldReturnZeroWhenNoValueIsRegisteredYet() {
        assertThat(calculation.getOpsPerSecond(System.nanoTime()), is(0L));
    }

    /***/
    @Test
    public void getOpsPerSecondReadOnlyShouldReturnZeroWhenNoValueIsRegisteredYet() {
        assertThat(calculation.getOpsPerSecondReadOnly(), is(0L));
    }

    /***/
    @Test
    public void closeIntervalAffectsSubsequentGetOpsPerSecond() throws InterruptedException {
        putNonZeroProgressToHistory();

        assertThat(calculation.getOpsPerSecond(System.nanoTime()), is(greaterThan(0L)));
    }

    /***/
    private void putNonZeroProgressToHistory() throws InterruptedException {
        calculation.setProgress(1000, System.nanoTime());
        Thread.sleep(10);
        calculation.closeInterval();
    }

    /***/
    @Test
    public void closeIntervalAffectsSubsequentGetOpsPerSecondReadOnly() throws InterruptedException {
        putNonZeroProgressToHistory();

        assertThat(calculation.getOpsPerSecondReadOnly(), is(greaterThan(0L)));
    }
}
