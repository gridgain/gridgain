/*
 * Copyright 2022 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.cache.persistence.evict;

import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link RandomLruPageEvictionTracker}.
 */
public class RandomLruPageEvictionTrackerTest {
    /**
     * Tests that {@link RandomLruPageEvictionTracker#trackingArraySize(int)} correctly handles situations
     * when the size does not fit {@code int} range.
     */
    @Test
    public void trackingArraySizeMayExceed2Gb() {
        assertThat(RandomLruPageEvictionTracker.trackingArraySize(1024 * 1024 * 1024), is(4L * 1024L * 1024L * 1024L));
    }

    /**
     * Tests that {@link RandomLruPageEvictionTracker#trackingArrayOffset(int)} correctly handles situations
     * when the size does not fit {@code int} range.
     */
    @Test
    public void trackingArrayOffsetMayExceed2Gb() {
        assertThat(
            RandomLruPageEvictionTracker.trackingArrayOffset(1024 * 1024 * 1024),
            is(4L * 1024L * 1024L * 1024L)
        );
    }
}
