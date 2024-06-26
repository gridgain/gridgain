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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link ExponentialBackoff}.
 */
public class ExponentialBackoffTest {
    /** Starting backoff duration used for test scenarios. */
    private static final long STARTING_BACKOFF_NANOS = 1000;

    /** Backoff ratio used for test scenarios. */
    private static final double BACKOFF_RATIO = 1.1;

    /** The object under test. */
    private final ExponentialBackoff backoff = new ExponentialBackoff(STARTING_BACKOFF_NANOS, BACKOFF_RATIO);

    @Test
    public void firstBackoffDurationShouldEqualStartingDuration() {
        assertThat(backoff.nextDuration(), is(STARTING_BACKOFF_NANOS));
    }

    @Test
    public void nextBackoffDurationShouldBeLongerThanPreviousOne() {
        backoff.nextDuration();

        assertThat(backoff.nextDuration(), equalTo((long) (STARTING_BACKOFF_NANOS * BACKOFF_RATIO)));
    }

    @Test
    public void resetInvocationShouldResetTheBackoffToInitialState() {
        backoff.nextDuration();
        backoff.nextDuration();
        backoff.reset();

        assertThat(backoff.nextDuration(), is(STARTING_BACKOFF_NANOS));
    }

    @Test
    public void resetShouldReturnFalseWhenBackoffIsAlreadyAtInitialState() {
        assertFalse(backoff.reset());
    }

    @Test
    public void resetShouldReturnTrueWhenBackoffIsNotAtInitialState() {
        backoff.nextDuration();

        assertTrue(backoff.reset());
    }
}
