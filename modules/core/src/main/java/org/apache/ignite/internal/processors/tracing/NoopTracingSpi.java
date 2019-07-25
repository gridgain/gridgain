package org.apache.ignite.internal.processors.tracing;

import java.util.Map;

import io.opencensus.trace.TraceComponent;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class NoopTracingSpi extends IgniteSpiAdapter implements TracingSpi {
    private static final SpanEx NOOP_SPAN = new SpanEx() {
        @Override public Object impl() {
            return null;
        }

        @Override public Span addTag(String tagName, String tagVal) {
            return this;
        }

        @Override public Span addLog(String logDesc) {
            return this;
        }

        @Override public Span addLog(String logDesc, Map<String, String> attributes) {
            return this;
        }

        @Override public Span setStatus(Status status) {
            return this;
        }

        @Override public Span end() {
            return this;
        }
    };

    private static final byte[] NOOP_SERIALIZED_SPAN = new byte[0];

    @Override public SpanEx create(@NotNull String name, @Nullable SpanEx parentSpan) {
        return NOOP_SPAN;
    }

    @Override public SpanEx create(@NotNull String name, @Nullable byte[] serializedSpanBytes) {
        return NOOP_SPAN;
    }

    @Override public byte[] serialize(@NotNull SpanEx span) {
        return NOOP_SERIALIZED_SPAN;
    }

    /** {@inheritDoc} */
    @Override public TraceComponent getTraceComponent() {
        return null;
    }

    @Override public void spiStart(String igniteInstanceName) throws IgniteSpiException {
        // Do nothing.
    }

    @Override public void spiStop() throws IgniteSpiException {
        // Do nothing.
    }
}
