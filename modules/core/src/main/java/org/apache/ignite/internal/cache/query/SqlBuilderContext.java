package org.apache.ignite.internal.cache.query;

public interface SqlBuilderContext {
    String columnName();

    boolean nullable();
}
