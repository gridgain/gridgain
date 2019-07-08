package org.apache.ignite.internal.metrics;

@FunctionalInterface
public interface IntGauge {
    int value();
}
