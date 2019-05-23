package org.apache.ignite.internal.processors.tracing.impl;

import java.util.Map;
import java.util.stream.Collectors;
import io.opencensus.trace.Annotation;
import io.opencensus.trace.AttributeValue;
import org.apache.ignite.internal.processors.tracing.Span;
import org.apache.ignite.internal.processors.tracing.SpanEx;
import org.apache.ignite.internal.processors.tracing.Status;

public class SpanAdapter implements SpanEx<io.opencensus.trace.Span> {
    private final io.opencensus.trace.Span span;

    public SpanAdapter(io.opencensus.trace.Span span) {
        this.span = span;
    }

    /** {@inheritDoc} */
    @Override public io.opencensus.trace.Span impl() {
        return span;
    }

    /** {@inheritDoc} */
    @Override public Span addTag(String tagName, String tagVal) {
        span.putAttribute(tagName, AttributeValue.stringAttributeValue(tagVal));

        return this;
    }

    /** {@inheritDoc} */
    @Override public Span addLog(String logDesc) {
        span.addAnnotation(logDesc);

        return this;
    }

    /** {@inheritDoc} */
    @Override public Span addLog(String logDesc, Map<String, String> attributes) {
        span.addAnnotation(Annotation.fromDescriptionAndAttributes(
            logDesc,
            attributes.entrySet().stream()
                .collect(Collectors.toMap(
                    e -> e.getKey(),
                    e -> AttributeValue.stringAttributeValue(e.getValue())
                ))
        ));

        return this;
    }

    /** {@inheritDoc} */
    @Override public Span setStatus(Status status) {
        span.setStatus(StatusMatchTable.match(status));

        return this;
    }

    /** {@inheritDoc} */
    @Override public Span end() {
        span.end();

        return this;
    }
}
