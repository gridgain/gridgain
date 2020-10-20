package org.apache.ignite.internal.processors.query.stat;

import java.util.Objects;

/**
 * Statistics key.
 */
public class StatsKey {

    /** Object schema. */
    private String schema;

    /** Object name. */
    private String obj;

    /**
     * Constructor.
     *
     * @param schema object schema.
     * @param obj object name.
     */
    public StatsKey(String schema, String obj) {
        this.schema = schema;
        this.obj = obj;
    }

    /**
     * @return schema name.
     */
    public String schema() {
        return schema;
    }

    /**
     * @return object name.
     */
    public String obj() {
        return obj;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StatsKey statsKey = (StatsKey) o;
        return Objects.equals(schema, statsKey.schema) &&
                Objects.equals(obj, statsKey.obj);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(schema, obj);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "StatsKey{" +
                "schema='" + schema + '\'' +
                ", obj='" + obj + '\'' +
                '}';
    }
}
