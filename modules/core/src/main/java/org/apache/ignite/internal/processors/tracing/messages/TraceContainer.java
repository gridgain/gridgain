package org.apache.ignite.internal.processors.tracing.messages;

import java.io.Serializable;
import org.apache.ignite.internal.processors.tracing.Span;

public class TraceContainer implements Serializable {
    private byte[] serializedSpanBytes;
    private transient Span span;

    public byte[] serializedSpanBytes() {
        return serializedSpanBytes;
    }

    public void serializedSpanBytes(byte[] serializedSpan) {
        this.serializedSpanBytes = serializedSpan;
    }

    public Span span() {
        return span;
    }

    public void span(Span span) {
        this.span = span;
    }

    @Override public String toString() {
        return "TraceContainer{" +
            "serializedSpanBytes=" + serializedSpanBytes +
            ", span=" + span +
            '}';
    }
}
