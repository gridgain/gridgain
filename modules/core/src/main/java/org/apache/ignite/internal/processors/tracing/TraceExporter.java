package org.apache.ignite.internal.processors.tracing;

public interface TraceExporter {
    void start();
    void stop();
}
