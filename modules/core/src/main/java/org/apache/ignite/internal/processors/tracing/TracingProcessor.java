package org.apache.ignite.internal.processors.tracing;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.tracing.impl.OpenCensusTracingSpi;
import org.apache.ignite.internal.processors.tracing.impl.OpenCensusZipkinReporter;
import org.apache.ignite.internal.processors.tracing.messages.TraceableMessage;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class TracingProcessor extends GridProcessorAdapter implements Tracing {
    // TODO: Make configurable in Ignite configuration (Tracing section?).
    private final TracingSpi spi = new OpenCensusTracingSpi();

    private final Reporter reporter = new OpenCensusZipkinReporter("http://localhost:9411/api/v2/spans", "ignite");

    /**
     * @param ctx Kernal context.
     */
    public TracingProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    @Override public void start() throws IgniteCheckedException {
        super.start();

        reporter.start();

        log.info("Started tracing processor with configured spi " + spi + " and reporter " + reporter);
    }

    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        super.stop(cancel);

        reporter.stop();
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

    @Override public void afterReceive(TraceableMessage msg) {
        if (msg.trace().serializedSpan() != null && msg.trace().span() == null)
            msg.trace().span(
                create(msg.traceName(), msg.trace().serializedSpan())
                    .addTag("node", ctx.localNodeId().toString())
                    .addLog("Received")
            );
    }

    @Override public void beforeSend(TraceableMessage msg) {
        if (msg.trace().span() != null && msg.trace().serializedSpan() == null)
            msg.trace().serializedSpan(serialize(msg.trace().span()));
    }
}
