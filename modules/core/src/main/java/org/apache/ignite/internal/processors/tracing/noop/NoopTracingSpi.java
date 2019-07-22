package org.apache.ignite.internal.processors.tracing.noop;

import java.util.Map;
import org.apache.ignite.internal.processors.tracing.SerializedSpan;
import org.apache.ignite.internal.processors.tracing.Span;
import org.apache.ignite.internal.processors.tracing.SpanEx;
import org.apache.ignite.internal.processors.tracing.Status;
import org.apache.ignite.internal.processors.tracing.TracingSpi;
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

    private static final SerializedSpan NOOP_SERIALIZED_SPAN = new SerializedSpan() {
        @Override public byte[] value() {
            return new byte[0];
        }
    };

    @Override public SpanEx create(@NotNull String name, @Nullable SpanEx parentSpan) {
        return NOOP_SPAN;
    }

    @Override public SpanEx create(@NotNull String name, @Nullable SerializedSpan serializedSpan) {
        return NOOP_SPAN;
    }

    @Override public SerializedSpan serialize(@NotNull SpanEx span) {
        return NOOP_SERIALIZED_SPAN;
    }

    @Override public void spiStart(String igniteInstanceName) throws IgniteSpiException {
        log.info("Tracing SPI is not configured. Noop implementation will be used instead.");
    }

    @Override public void spiStop() throws IgniteSpiException {
        // Do nothing.
    }
}
