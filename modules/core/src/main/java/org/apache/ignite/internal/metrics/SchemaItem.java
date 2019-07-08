package org.apache.ignite.internal.metrics;

public class SchemaItem {
    private final String key;
    private final int type;

    public SchemaItem(String key, int type) {
        this.key = key;
        this.type = type;
    }
}
