package org.apache.ignite.internal.processors.tracing;

import org.apache.ignite.spi.IgniteSpi;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public interface TracingSpi<T> extends IgniteSpi {
    default SpanEx<T> create(@NotNull String name) {
        return create(name, (SpanEx<T>)null);
    }
    SpanEx<T> create(@NotNull String name, @Nullable SpanEx<T> parentSpan);
    SpanEx<T> create(@NotNull String name, @Nullable byte[] serializedSpanBytes);
    byte[] serialize(@NotNull SpanEx<T> span);
}
