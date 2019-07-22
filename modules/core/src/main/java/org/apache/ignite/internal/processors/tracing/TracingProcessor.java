package org.apache.ignite.internal.processors.tracing;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.tracing.noop.NoopTracingSpi;
import org.apache.ignite.spi.IgniteSpiException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class TracingProcessor extends GridProcessorAdapter implements Tracing {
    /** Spi. */
    private TracingSpi spi;

    /** Message process. */
    private final TracingMessagesProcessor msgProc;

    /**
     * @param ctx Kernal context.
     */
    public TracingProcessor(GridKernalContext ctx) {
        super(ctx);

        spi = ctx.config().getTracingSpi();

        msgProc = new TracingMessagesProcessor(ctx, spi);
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        super.start();

        try {
            spi.spiStart(ctx.igniteInstanceName());
        }
        catch (IgniteSpiException e) {
            log.warning("Failed to start tracing processor with spi: " + spi.getName()
                + ". Noop implementation will be used instead.", e);

            spi = new NoopTracingSpi();

            spi.spiStart(ctx.igniteInstanceName());
        }

        log.info("Started tracing processor with configured spi: " + spi.getName());
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        super.stop(cancel);

        spi.spiStop();
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
