package org.apache.ignite.internal.metrics;

@FunctionalInterface
public interface LongGauge extends Gauge {
    long value();
}
