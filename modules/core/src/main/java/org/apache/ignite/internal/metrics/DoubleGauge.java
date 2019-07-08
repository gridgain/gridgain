package org.apache.ignite.internal.metrics;

@FunctionalInterface
public interface DoubleGauge {
    double value();
}
