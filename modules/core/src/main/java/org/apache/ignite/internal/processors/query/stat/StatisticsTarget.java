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
