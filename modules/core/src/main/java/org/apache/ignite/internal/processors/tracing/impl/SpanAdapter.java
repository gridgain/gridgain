package org.apache.ignite.internal.processors.tracing.impl;

import io.opencensus.trace.AttributeValue;
import org.apache.ignite.internal.processors.tracing.Span;
import org.apache.ignite.internal.processors.tracing.SpanEx;

public class SpanAdapter implements SpanEx<io.opencensus.trace.Span> {
    private final io.opencensus.trace.Span span;

    public SpanAdapter(io.opencensus.trace.Span span) {
        this.span = span;
    }

    @Override public io.opencensus.trace.Span impl() {
        return span;
    }

    @Override public Span addTag(String tagName, String tagVal) {
        span.putAttribute(tagName, AttributeValue.stringAttributeValue(tagVal));

        return this;
    }

    @Override public Span addLog(String logDesc) {
        span.addAnnotation(logDesc);

        return this;
    }

    @Override public Span end() {
        span.end();

        return this;
    }
}
