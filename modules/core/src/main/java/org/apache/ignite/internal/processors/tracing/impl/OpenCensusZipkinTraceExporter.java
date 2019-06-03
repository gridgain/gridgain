package org.apache.ignite.internal.processors.tracing.impl;

import io.opencensus.exporter.trace.zipkin.ZipkinExporterConfiguration;
import io.opencensus.exporter.trace.zipkin.ZipkinTraceExporter;
import io.opencensus.trace.Tracing;
import org.apache.ignite.internal.processors.tracing.TraceExporter;

public class OpenCensusZipkinTraceExporter implements TraceExporter {
    private final ZipkinExporterConfiguration cfg;

    public OpenCensusZipkinTraceExporter(ZipkinExporterConfiguration cfg) {
        this.cfg = cfg;
    }

    /** {@inheritDoc} */
    @Override public void start() {
        try {
            ZipkinTraceExporter.createAndRegister(cfg);
        }
        //TODO: This exception handling is needed to allow tracing exporting with multiple nodes in single JVM environment.
        catch (Exception ignored) {}
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        Tracing.getExportComponent().shutdown();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "OpenCensus Zipkin TraceExporter [cfg=" + cfg + "]";
    }
}
