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

package org.apache.ignite.internal.processors.query.h2.opt.statistics;

import org.gridgain.internal.h2.value.Value;

/**
 * Values statistic in particular column.
 */
public class ColumnStatistics {
    /** Minimum value in column or {@code null} if there are no non null values in the column. */
    private final Value min;

    /** Maximum value in column or {@code null} if there are no non null values in the column. */
    private final Value max;

    /** Percent of null values in column. */
    private final int nulls;

    /** Percent of different values in column, i.e. 100 means that all values are unique, 0% means that all values
     * are the same. */
    private final int cardinality;

    private final byte[] raw;

    /**
     * Constructor.
     *
     * @param min min value in column or {@code null}.
     * @param max max value in column or {@code null}.
     * @param nulls percent of null values in column
     * @param cardinality percent of unique value in column
     * @param raw raw data to aggregate statistics.
     */
    public ColumnStatistics(Value min, Value max, int nulls, int cardinality, byte[] raw) {
        this.min = min;
        this.max = max;
        this.nulls = nulls;
        this.cardinality = cardinality;
        this.raw = raw;
    }

    public Value min() {
        return min;
    }

    public Value max() {
        return max;
    }

    public int nulls() {
        return nulls;
    }

    public int cardinality() {
        return cardinality;
    }

    public byte[] raw() {
        return raw;
    }
}
