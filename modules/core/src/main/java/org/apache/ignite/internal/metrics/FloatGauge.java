package org.apache.ignite.internal.metrics;

@FunctionalInterface
public interface FloatGauge {
    float value();
}
