package org.apache.ignite.internal.processors.tracing;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public interface TracingSpi<T> {
    default SpanEx<T> create(@NotNull String name) {
        return create(name, (SpanEx<T>)null);
    }
    SpanEx<T> create(@NotNull String name, @Nullable SpanEx<T> parentSpan);
    SpanEx<T> create(@NotNull String name, @Nullable SerializedSpan serializedSpan);
    SerializedSpan serialize(@NotNull SpanEx<T> span);
}
