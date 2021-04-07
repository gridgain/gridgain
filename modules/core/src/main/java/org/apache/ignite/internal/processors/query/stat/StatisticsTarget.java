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
package org.apache.ignite.internal.processors.query.stat;

import java.util.Arrays;
import java.util.Objects;

import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Target to collect statistics by.
 */
public class StatisticsTarget {
    /** Statistic key. */
    @GridToStringInclude
    private final StatisticsKey key;

    /** Column names. */
    @GridToStringInclude
    private final String[] columns;

    /**
     * Constructor.
     *
     * @param schema Schema name.
     * @param obj Object name.
     * @param columns Array of column names or {@code null} if target - all columns.
     */
    public StatisticsTarget(String schema, String obj, String... columns) {
        this(new StatisticsKey(schema, obj), columns);
    }

    /**
     * Constructor.
     *
     * @param key Statistic key.
     * @param columns Array of column names or {@code null} if target - all columns.
     */
    public StatisticsTarget(StatisticsKey key, String... columns) {
        this.key = key;
        this.columns = (columns == null || columns.length == 0) ? null : columns;
    }

    /**
     * @return Schema name.
     */
    public String schema() {
        return key.schema();
    }

    /** Object name. */
    public String obj() {
        return key().obj();
    }

    /** Columns array. */
    public String[] columns() {
        return columns;
    }

    /** Statistic key (schema and table name). */
    public StatisticsKey key() {
        return key;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StatisticsTarget that = (StatisticsTarget) o;

        return Objects.equals(key, that.key) &&
                Arrays.equals(columns, that.columns);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int result = Objects.hash(key);
        result = 31 * result + Arrays.hashCode(columns);
        return result;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(StatisticsTarget.class, this);
    }
}
