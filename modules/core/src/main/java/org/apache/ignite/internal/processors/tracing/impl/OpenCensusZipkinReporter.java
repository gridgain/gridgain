package org.apache.ignite.internal.processors.tracing.impl;

import io.opencensus.exporter.trace.zipkin.ZipkinExporterConfiguration;
import io.opencensus.exporter.trace.zipkin.ZipkinTraceExporter;
import io.opencensus.trace.Tracing;
import org.apache.ignite.internal.processors.tracing.Reporter;

public class OpenCensusZipkinReporter implements Reporter {
    private final ZipkinExporterConfiguration cfg;

    public OpenCensusZipkinReporter(ZipkinExporterConfiguration cfg) {
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
        return "OpenCensus Zipkin Reporter [cfg=" + cfg + "]";
    }
}
