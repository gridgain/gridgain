package org.apache.ignite.internal.processors.query.stat.config;

import org.apache.ignite.internal.util.typedef.internal.S;

import java.util.Objects;

/** */
public class StatisticsColumnOverrides {
    /** Percent of null values in column. If {@code null} - use calculated value. */
    private final Long nulls;

    /** Number of distinct values in column. If {@code null} - use calculated value. */
    private final Long distinct;

    /** Total number of values in column. If {@code null} - use calculated value. */
    private final Long total;

    /** Average size in bytes, for variable size only. If {@code null} - use calculated value. */
    private final Integer size;


    public StatisticsColumnOverrides(Long nulls, Long distinct, Long total, Integer size) {
        this.nulls = nulls;
        this.distinct = distinct;
        this.total = total;
        this.size = size;
    }

    public Long nulls() {
        return nulls;
    }

    public Long distinct() {
        return distinct;
    }

    public Long total() {
        return total;
    }

    public Integer size() {
        return size;
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StatisticsColumnOverrides that = (StatisticsColumnOverrides) o;
        return Objects.equals(nulls, that.nulls) && Objects.equals(distinct, that.distinct) &&
            Objects.equals(total, that.total) && Objects.equals(size, that.size);
    }

    @Override public int hashCode() {
        return Objects.hash(nulls, distinct, total, size);
    }

    @Override public String toString(){
        return S.toString(StatisticsColumnOverrides.class, this);
    }
}
