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
package org.apache.ignite.internal.processors.query.stat;

import org.apache.ignite.internal.util.typedef.internal.S;
import org.gridgain.internal.h2.value.Value;

import java.util.Arrays;
import java.util.Objects;

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

    /**
     * Percent of different values in column, i.e. 100 means that all values are unique, 0% means that all values
     * are the same.
     */
    private final int cardinality;

    /** Total number of values in column. */
    private final long total;

    /** Average size in bytes, for variable size only. */
    private final int size;

    /** Raw data. */
    private final byte[] raw;

    /** Version. */
    private final long ver;

    /**
     * Constructor.
     *
     * @param min Min value in column or {@code null}.
     * @param max Max value in column or {@code null}.
     * @param nulls Percent of null values in column.
     * @param cardinality Percent of unique value in column.
     * @param total Total number of values in column.
     * @param size Average size in bytes, for variable size only.
     * @param raw Raw data to aggregate statistics.
     */
    public ColumnStatistics(Value min, Value max, int nulls, int cardinality, long total, int size, byte[] raw) {
        this(min, max, nulls, cardinality, total, size, raw, 0);
    }

    /**
     * Constructor.
     *
     * @param min Min value in column or {@code null}.
     * @param max Max value in column or {@code null}.
     * @param nulls Percent of null values in column.
     * @param cardinality Percent of unique value in column.
     * @param total Total number of values in column.
     * @param size Average size in bytes, for variable size only.
     * @param raw Raw data to aggregate statistics.
     */
    public ColumnStatistics(
        Value min,
        Value max,
        int nulls,
        int cardinality,
        long total,
        int size,
        byte[] raw,
        long ver
    ) {
        this.min = min;
        this.max = max;
        this.nulls = nulls;
        this.cardinality = cardinality;
        this.total = total;
        this.size = size;
        this.raw = raw;
        this.ver = ver;
    }

    /**
     * @return Min value in column.
     */
    public Value min() {
        return min;
    }

    /**
     * @return Max value in column.
     */
    public Value max() {
        return max;
    }

    /**
     * @return Percent of null values.
     */
    public int nulls() {
        return nulls;
    }

    /**
     * @return Percent of unique not null values.
     */
    public int cardinality() {
        return cardinality;
    }

    /**
     * @return Total number of values in column.
     */
    public long total() {
        return total;
    }

    /**
     * @return Average size in bytes, for variable size only.
     */
    public int size() {
        return size;
    }

    /**
     * @return Raw value needed to aggregate statistics.
     */
    public byte[] raw() {
        return raw;
    }

    /**
     * @return Statistic's version.
     */
    public long version() {
        return ver;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ColumnStatistics that = (ColumnStatistics) o;
        return nulls == that.nulls &&
                cardinality == that.cardinality &&
                total == that.total &&
                size == that.size &&
                ver == that.ver &&
                Objects.equals(min, that.min) &&
                Objects.equals(max, that.max) &&
                Arrays.equals(raw, that.raw);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int result = Objects.hash(min, max, nulls, cardinality, total, size, ver);
        result = 31 * result + Arrays.hashCode(raw);
        return result;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ColumnStatistics.class, this);
    }
}
