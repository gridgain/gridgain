package org.apache.ignite.internal.processors.tracing;

import java.util.Map;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class NoopTracingSpi extends IgniteSpiAdapter implements TracingSpi {
    private static final Span NOOP_SPAN = new Span() {
        @Override public Span addTag(String tagName, String tagVal) {
            return this;
        }

        @Override public Span addTag(String tagName, long tagVal) {
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

        @Override public boolean isEnded() {
            return false;
        }
    };

    private static final byte[] NOOP_SERIALIZED_SPAN = new byte[0];

    @Override public Span create(@NotNull String name, @Nullable Span parentSpan) {
        return NOOP_SPAN;
    }

    @Override public Span create(@NotNull String name, @Nullable byte[] serializedSpanBytes) {
        return NOOP_SPAN;
    }

    @Override public byte[] serialize(@NotNull Span span) {
        return NOOP_SERIALIZED_SPAN;
    }

    @Override public void spiStart(String igniteInstanceName) throws IgniteSpiException {
        // Do nothing.
    }

    @Override public void spiStop() throws IgniteSpiException {
        // Do nothing.
    }
}
