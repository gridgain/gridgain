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

package org.apache.ignite.internal.util;

/**
 * A mutable long for use in collections.
 */
public class GridMutableLong {
    /** Long value. */
    private long v;

    /**
     * Constructor.
     *
     * @param v Long value.
     */
    public GridMutableLong(long v) {
        this.v = v;
    }

    /**
     * Default constructor.
     */
    public GridMutableLong() {
    }

    /**
     * Increments by one the current value.
     *
     * @return Updated value.
     */
    public final long incrementAndGet() {
        return ++v;
    }

    /**
     * Gets the current value.
     *
     * @return Current value.
     */
    public long get() {
        return v;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return Long.toString(v);
    }
}
