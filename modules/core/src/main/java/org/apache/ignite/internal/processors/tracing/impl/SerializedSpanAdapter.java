package org.apache.ignite.internal.processors.tracing.impl;

import org.apache.ignite.internal.processors.tracing.SerializedSpan;

public class SerializedSpanAdapter implements SerializedSpan {
    /** */
    private static final long serialVersionUID = 0L;

    private final byte[] serializedSpan;

    public SerializedSpanAdapter(byte[] serializedSpan) {
        this.serializedSpan = serializedSpan;
    }

    /** {@inheritDoc} */
    @Override public byte[] value() {
        return serializedSpan;
    }
}
