package org.apache.ignite.internal.processors.tracing;

public interface SpanEx<T> extends Span {
    T impl();
}
