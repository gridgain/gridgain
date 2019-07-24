package org.apache.ignite.opencensus.spi.tracing;

import io.opencensus.trace.TraceComponent;

public interface OpenCensusTraceExporter {
    /**
     * @param igniteInstanceName Ignite instance name.
     */
    public void start(TraceComponent traceComponent, String igniteInstanceName);
    /**
     *
     */
    public void stop(TraceComponent traceComponent);
}
