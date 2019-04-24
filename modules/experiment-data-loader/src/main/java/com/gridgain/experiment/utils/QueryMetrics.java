package com.gridgain.experiment.utils;

import java.util.Objects;

public class QueryMetrics {

    private String sql;

    private long executionTime;

    private int resultSetSize;

    public QueryMetrics(String sql, long executionTime, int resultSetSize) {
        this.sql = sql;
        this.executionTime = executionTime;
        this.resultSetSize = resultSetSize;
    }

    public QueryMetrics(String sql, long executionTime) {
        this(sql, executionTime, 0);
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public long getExecutionTime() {
        return executionTime;
    }

    public void setExecutionTime(long executionTime) {
        this.executionTime = executionTime;
    }

    public int getResultSetSize() {
        return resultSetSize;
    }

    public void setResultSetSize(int resultSetSize) {
        this.resultSetSize = resultSetSize;
    }

    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        QueryMetrics metrics = (QueryMetrics)o;
        return executionTime == metrics.executionTime &&
            resultSetSize == metrics.resultSetSize &&
            Objects.equals(sql, metrics.sql);
    }

    @Override public int hashCode() {
        return Objects.hash(sql, executionTime, resultSetSize);
    }
}
