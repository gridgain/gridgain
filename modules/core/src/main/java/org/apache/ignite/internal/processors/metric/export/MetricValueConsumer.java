package org.apache.ignite.internal.processors.metric.export;

public interface MetricValueConsumer {

    void onBoolean(String name, boolean val);

    void onInt(String name, int val);

    void onLong(String name, long val);

    void onDouble(String name, double val);
}
