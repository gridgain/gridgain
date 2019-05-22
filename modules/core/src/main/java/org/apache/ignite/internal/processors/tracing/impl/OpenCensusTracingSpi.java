package org.apache.ignite.internal.processors.tracing.impl;

import io.opencensus.trace.Tracing;
import io.opencensus.trace.propagation.SpanContextParseException;
import io.opencensus.trace.samplers.Samplers;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.tracing.SerializedSpan;
import org.apache.ignite.internal.processors.tracing.SpanEx;
import org.apache.ignite.internal.processors.tracing.TracingSpi;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class OpenCensusTracingSpi implements TracingSpi<io.opencensus.trace.Span> {
    /** {@inheritDoc} */
    @Override public SpanEx<io.opencensus.trace.Span> create(@NotNull String name, @Nullable SpanEx<io.opencensus.trace.Span> parentSpan) {
        return new SpanAdapter(
            Tracing.getTracer().spanBuilderWithExplicitParent(name, parentSpan != null ? parentSpan.impl() : null)
                .setSampler(Samplers.alwaysSample())
                .startSpan()
            );
    }

    /** {@inheritDoc} */
    @Override public SpanEx<io.opencensus.trace.Span> create(@NotNull String name, @Nullable SerializedSpan serializedSpan) {
        try {
            return new SpanAdapter(
                Tracing.getTracer().spanBuilderWithRemoteParent(
                    name,
                    Tracing.getPropagationComponent().getBinaryFormat().fromByteArray(
                        serializedSpan != null ? serializedSpan.value() : null)
                )
                .setSampler(Samplers.alwaysSample())
                .startSpan()
            );
        }
        catch (SpanContextParseException e) {
            throw new IgniteException("Failed to create span from serialized value: " + name, e);
        }
    }

    /** {@inheritDoc} */
    @Override public SerializedSpan serialize(@NotNull SpanEx<io.opencensus.trace.Span> span) {
        return new SerializedSpanAdapter(
            Tracing.getPropagationComponent().getBinaryFormat().toByteArray(span.impl().getContext())
        );
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "OpenCensusTracingSpi";
    }
}
