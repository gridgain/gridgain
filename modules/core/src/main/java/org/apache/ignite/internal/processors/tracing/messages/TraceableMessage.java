package org.apache.ignite.internal.processors.tracing.messages;

import org.jetbrains.annotations.NotNull;

public interface TraceableMessage {
    @NotNull TraceContainer trace();
    String traceName();
}
