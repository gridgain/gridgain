package org.apache.ignite.opencensus.spi.tracing;

import java.util.Map;
import java.util.stream.Collectors;
import io.opencensus.trace.Annotation;
import io.opencensus.trace.AttributeValue;
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
    @Override public SpanAdapter addTag(String tagName, String tagVal) {
        span.putAttribute(tagName, AttributeValue.stringAttributeValue(tagVal));

        return this;
    }

    /** {@inheritDoc} */
    @Override public SpanAdapter addLog(String logDesc) {
        span.addAnnotation(logDesc);

        return this;
    }

    /** {@inheritDoc} */
    @Override public SpanAdapter addLog(String logDesc, Map<String, String> attrs) {
        span.addAnnotation(Annotation.fromDescriptionAndAttributes(
            logDesc,
            attrs.entrySet().stream()
                .collect(Collectors.toMap(
                    Map.Entry::getKey,
                    e -> AttributeValue.stringAttributeValue(e.getValue())
                ))
        ));

        return this;
    }

    /** {@inheritDoc} */
    @Override public SpanAdapter setStatus(Status status) {
        span.setStatus(StatusMatchTable.match(status));

        return this;
    }

    /** {@inheritDoc} */
    @Override public SpanAdapter end() {
        span.end();

        return this;
    }
}
