package org.apache.ignite.internal.processors.tracing;

import io.opencensus.exporter.trace.zipkin.ZipkinExporterConfiguration;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.tracing.impl.OpenCensusTracingSpi;
import org.apache.ignite.internal.processors.tracing.impl.OpenCensusZipkinTraceExporter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class TracingProcessor extends GridProcessorAdapter implements Tracing {
    // TODO: Make configurable in Ignite configuration (Tracing section?).
    private final TracingSpi spi;

    private final TraceExporter traceExporter;

    private final TracingMessagesProcessor msgProc;

    /**
     * @param ctx Kernal context.
     */
    public TracingProcessor(GridKernalContext ctx) {
        super(ctx);

        spi = new OpenCensusTracingSpi();

        traceExporter = new OpenCensusZipkinTraceExporter(
            ZipkinExporterConfiguration.builder()
                .setV2Url("http://localhost:9411/api/v2/spans")
                .setServiceName("ignite")
                .build()
        );

        msgProc = new TracingMessagesProcessor(ctx, spi);
    }

    @Override public void start() throws IgniteCheckedException {
        super.start();

        traceExporter.start();

        log.info("Started tracing processor with configured spi " + spi + " and traceExporter " + traceExporter);
    }

    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        super.stop(cancel);

        //traceExporter.stop();
    }

    /** {@inheritDoc} */
    @Override public Span create(@NotNull String name, @Nullable Span parentSpan) {
        return spi.create(name, (SpanEx) parentSpan);
    }

    /** {@inheritDoc} */
    @Override public Span create(@NotNull String name, @Nullable SerializedSpan serializedSpan) {
        return spi.create(name, serializedSpan);
    }

    /** {@inheritDoc} */
    @Override public SerializedSpan serialize(@NotNull Span span) {
        return spi.serialize((SpanEx) span);
    }

    /** {@inheritDoc} */
    @Override public TracingMessagesProcessor messages() {
        return msgProc;
    }
}
