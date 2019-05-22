package org.apache.ignite.internal.processors.tracing;

import org.apache.ignite.internal.processors.tracing.messages.Trace;
import org.apache.ignite.internal.processors.tracing.messages.TraceableMessage;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public interface Tracing {
    default Span create(@NotNull String name) {
        return create(name, (Span)null);
    }
    Span create(@NotNull String name, @Nullable Span parentSpan);
    Span create(@NotNull String name, @Nullable SerializedSpan serializedSpan);
    SerializedSpan serialize(@NotNull Span span);

    /**
     * Called when message is received.
     * A span with name {@link TraceableMessage#traceName()} will be created
     * from contained serialized span {@link Trace#serializedSpan()}
     *
     * @param msg Traceable message.
     */
    void afterReceive(TraceableMessage msg);

    /**
     * Called when message is going to be send.
     * A serialized span will be created and attached to {@link TraceableMessage#trace()}.
     *
     * @param msg Traceable message.
     */
    void beforeSend(TraceableMessage msg);
}
