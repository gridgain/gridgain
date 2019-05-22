package org.apache.ignite.internal.processors.tracing.impl;

import io.opencensus.exporter.trace.zipkin.ZipkinTraceExporter;
import io.opencensus.trace.Tracing;
import org.apache.ignite.internal.processors.tracing.Reporter;

public class OpenCensusZipkinReporter implements Reporter {
    private final String url;

    private final String serviceName;

    public OpenCensusZipkinReporter(String url, String serviceName) {
        this.url = url;
        this.serviceName = serviceName;
    }

    /** {@inheritDoc} */
    @Override public void start() {
        try {
            ZipkinTraceExporter.createAndRegister(url, serviceName);
        }
        catch (Exception ignored) {}
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        Tracing.getExportComponent().shutdown();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "OpenCensusZipkinReporter{" +
            "url='" + url + '\'' +
            ", serviceName='" + serviceName + '\'' +
            '}';
    }
}
