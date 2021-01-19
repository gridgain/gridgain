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

/**
 * Target to collect statistics by.
 */
public class StatisticsTarget {
    /** Schema name. */
    private final String schema;

    /** Object (table or index) name. */
    private final String obj;

    /**  */
    private final String[] columns;

    /**
     * Constructor.
     *
     * @param schema Schema name.
     * @param obj Object name.
     * @param columns Array of column names or {@code null} if target - all columns.
     */
    public StatisticsTarget(String schema, String obj, String... columns) {
        this.schema = schema;
        this.obj = obj;
        this.columns = columns;
    }

    /**
     * @return Schema name.
     */
    public String schema() {
        return schema;
    }

    /** Object name. */
    public String obj() {
        return obj;
    }

    /** Columns array. */
    public String[] columns() {
        return columns;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StatisticsTarget that = (StatisticsTarget) o;
        return Objects.equals(schema, that.schema) &&
                Objects.equals(obj, that.obj) &&
                Arrays.equals(columns, that.columns);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int result = Objects.hash(schema, obj);
        result = 31 * result + Arrays.hashCode(columns);
        return result;
    }
}
