package org.apache.ignite.internal.processors.tracing.messages;

import java.io.Serializable;
import org.apache.ignite.internal.processors.tracing.SerializedSpan;
import org.apache.ignite.internal.processors.tracing.Span;

public class Trace implements Serializable {
    private SerializedSpan serializedSpan;
    private transient Span span;

    public SerializedSpan serializedSpan() {
        return serializedSpan;
    }

    public void serializedSpan(SerializedSpan serializedSpan) {
        this.serializedSpan = serializedSpan;
    }

    public Span span() {
        return span;
    }

    public void span(Span span) {
        this.span = span;
    }

    @Override
    public String toString() {
        return "Trace{" +
            "serializedSpan=" + serializedSpan +
            ", span=" + span +
            '}';
    }
}
