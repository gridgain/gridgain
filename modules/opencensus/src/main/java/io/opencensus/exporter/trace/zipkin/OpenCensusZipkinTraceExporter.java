/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.opencensus.exporter.trace.zipkin;

import io.opencensus.trace.TraceComponent;
import io.opencensus.trace.export.SpanExporter;
import org.apache.ignite.opencensus.spi.tracing.OpenCensusTraceExporter;
import org.apache.ignite.spi.IgniteSpiException;
import zipkin2.reporter.Sender;
import zipkin2.reporter.urlconnection.URLConnectionSender;

/**
 * Zipkin Trace Exporter based on OpenCensus library.
 */
public class OpenCensusZipkinTraceExporter implements OpenCensusTraceExporter {
    /** Config. */
    private final ZipkinExporterConfiguration cfg;

    /**
     * @param cfg Config.
     */
    public OpenCensusZipkinTraceExporter(ZipkinExporterConfiguration cfg) {
        this.cfg = cfg;
    }

    /** {@inheritDoc} */
    @Override public void start(TraceComponent traceComponent, String igniteInstanceName) {
        try {
            Sender snd = cfg.getSender();
            if (snd == null)
                snd = URLConnectionSender.create(cfg.getV2Url());

            SpanExporter.Handler exporterHnd = new ZipkinExporterHandler(
                traceComponent.getTracer(),
                cfg.getEncoder(),
                snd,
                cfg.getServiceName(), //TODO: Maybe additionally attach consistentId to service?
                cfg.getDeadline()
            );

            traceComponent.getExportComponent().getSpanExporter().registerHandler(
                ZipkinTraceExporter.class.getName() + "-" + igniteInstanceName,
                exporterHnd
            );
        }
        catch (Exception e) {
            throw new IgniteSpiException("Failed to start trace exporter", e);
        }
    }

    /** {@inheritDoc} */
    @Override public void stop(TraceComponent traceComponent) {
        traceComponent.getExportComponent().shutdown();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "OpenCensus Zipkin TraceExporter [cfg=" + cfg + "]";
    }
}
