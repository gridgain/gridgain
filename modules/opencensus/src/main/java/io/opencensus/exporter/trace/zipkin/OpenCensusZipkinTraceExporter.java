package io.opencensus.exporter.trace.zipkin;

import io.opencensus.trace.TraceComponent;
import io.opencensus.trace.export.SpanExporter;
import org.apache.ignite.opencensus.spi.tracing.OpenCensusTraceExporter;
import org.apache.ignite.spi.IgniteSpiException;
import zipkin2.reporter.Sender;
import zipkin2.reporter.urlconnection.URLConnectionSender;

/**
 * Zipkin Trace Exporter based on OpenCensus tracing library.
 *
 * Moved to io.opencensus package to get access to package-private {@link ZipkinExporterHandler}.
 */
public class OpenCensusZipkinTraceExporter implements OpenCensusTraceExporter {
    private final ZipkinExporterConfiguration cfg;

    public OpenCensusZipkinTraceExporter(ZipkinExporterConfiguration cfg) {
        this.cfg = cfg;
    }

    /** {@inheritDoc} */
    public void start(TraceComponent traceComponent, String igniteInstanceName) {
        try {
            Sender snd = cfg.getSender();
            if (snd == null)
                snd = URLConnectionSender.create(cfg.getV2Url());

            SpanExporter.Handler exporterHnd = new ZipkinExporterHandler(
                cfg.getEncoder(),
                snd,
                cfg.getServiceName() + "-" + igniteInstanceName,
                cfg.getDeadline()
            );

            traceComponent.getExportComponent().getSpanExporter().registerHandler(
                ZipkinTraceExporter.class.getName(),
                exporterHnd
            );
        }
        catch (Exception e) {
            throw new IgniteSpiException("Failed to start trace exporter", e);
        }
    }

    /** {@inheritDoc} */
    public void stop(TraceComponent traceComponent) {
        traceComponent.getExportComponent().shutdown();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "OpenCensus Zipkin TraceExporter [cfg=" + cfg + "]";
    }
}
