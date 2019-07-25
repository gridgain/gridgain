package org.apache.ignite.internal.processors.tracing;

import org.apache.ignite.spi.IgniteSpi;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public interface TracingSpi extends IgniteSpi {
    default Span create(@NotNull String name) {
        return create(name, (Span)null);
    }
    Span create(@NotNull String name, @Nullable Span parentSpan);
    Span create(@NotNull String name, @Nullable byte[] serializedSpanBytes);
    byte[] serialize(@NotNull Span span);
}
