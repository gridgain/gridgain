/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.agent.service.tracing;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import io.opencensus.common.Duration;
import io.opencensus.common.Function;
import io.opencensus.common.Functions;
import io.opencensus.common.Timestamp;
import io.opencensus.exporter.trace.TimeLimitedHandler;
import io.opencensus.trace.AttributeValue;
import io.opencensus.trace.SpanContext;
import io.opencensus.trace.Status;
import io.opencensus.trace.Tracing;
import io.opencensus.trace.export.SpanData;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.agent.dto.tracing.Annotation;
import org.apache.ignite.agent.dto.tracing.Span;
import org.apache.ignite.agent.service.sender.CoordinatorSender;
import org.apache.ignite.agent.service.sender.RetryableSender;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.tracing.opencensus.OpenCensusTraceExporter;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Span exporter which send spans to coordinator.
 */
public class GmcSpanExporter implements AutoCloseable {
    /** Queue capacity. */
    private static final int QUEUE_CAP = 100;

    /** Status description. */
    public static final String TRACING_TOPIC = "gmc-tracing-topic";

    /** Status code. */
    private static final String STATUS_CODE = "census.status_code";

    /** Status description. */
    private static final String STATUS_DESCRIPTION = "census.status_description";

    /** Context. */
    private GridKernalContext ctx;

    /** Logger. */
    private IgniteLogger log;

    /** Exporter. */
    private OpenCensusTraceExporter exporter;

    /** Worker. */
    private RetryableSender<Span> snd;

    /**
     * @param ctx Context.
     */
    public GmcSpanExporter(GridKernalContext ctx) {
        this.ctx = ctx;
        this.log = ctx.log(GmcSpanExporter.class);

        if (ctx.config().getTracingSpi() != null) {
            try {
                snd = createSender();
                exporter = new OpenCensusTraceExporter(getTraceHandler());
                exporter.start(ctx.igniteInstanceName());
            }
            catch (IgniteSpiException ex) {
                log.error("Trace exporter start failed", ex);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void close() {
        if (exporter != null) {
            U.closeQuiet(snd);
            exporter.stop();
        }
    }

    /**
     * @return Span exporter handler.
     */
    TimeLimitedHandler getTraceHandler() {
        return new TimeLimitedHandler(Tracing.getTracer(), Duration.create(10, 0), "SendGmcSpans") {
            @Override public void timeLimitedExport(Collection<SpanData> spanDataList) {
                List<Span> spans = spanDataList
                        .stream()
                        .map(GmcSpanExporter::fromSpanDataToSpan)
                        .collect(Collectors.toList());

                snd.send(spans);
            }
        };
    }

    /**
     * @return Worker which send messages from queue to topic.
     */
    private RetryableSender<Span> createSender() {
        return new CoordinatorSender<>(ctx, QUEUE_CAP, TRACING_TOPIC);
    }

    /**
     * @param spanData Span data.
     */
    static Span fromSpanDataToSpan(SpanData spanData) {
        SpanContext ctx = spanData.getContext();
        long startTs = toEpochMillis(spanData.getStartTimestamp());
        long endTs = toEpochMillis(spanData.getEndTimestamp());

        Span span = new Span()
                .setTraceId(ctx.getTraceId().toLowerBase16())
                .setSpanId(ctx.getSpanId().toLowerBase16())
                .setName(spanData.getName())
                .setTimestamp(toEpochMillis(spanData.getStartTimestamp()))
                .setDuration(endTs - startTs);

        if (spanData.getParentSpanId() != null && spanData.getParentSpanId().isValid())
            span.setParentId(spanData.getParentSpanId().toLowerBase16());

        for (Map.Entry<String, AttributeValue> label : spanData.getAttributes().getAttributeMap().entrySet())
            span.getTags().put(label.getKey(), attributeValueToString(label.getValue()));

        Status status = spanData.getStatus();
        if (status != null) {
            span.getTags().put(STATUS_CODE, status.getCanonicalCode().toString());
            if (status.getDescription() != null)
                span.getTags().put(STATUS_DESCRIPTION, status.getDescription());
        }

        spanData.getAnnotations().getEvents().stream()
                .map(a -> new Annotation(toEpochMillis(a.getTimestamp()), a.getEvent().getDescription()))
                .forEach(a -> span.getAnnotations().add(a));

        spanData.getMessageEvents().getEvents().stream()
                .map(e -> new Annotation(toEpochMillis(e.getTimestamp()), e.getEvent().getType().name()))
                .forEach(a -> span.getAnnotations().add(a));

        return span;
    }

    /**
     * @param ts Timestamp.
     */
    private static long toEpochMillis(Timestamp ts) {
        return SECONDS.toMillis(ts.getSeconds()) + NANOSECONDS.toMillis(ts.getNanos());
    }

    /** Return to string. */
    private static final Function<Object, String> returnToStr =
            Functions.returnToString();

    /**
     * @param attributeVal Attribute value.
     */
    private static String attributeValueToString(AttributeValue attributeVal) {
        return attributeVal.match(
                returnToStr,
                returnToStr,
                returnToStr,
                returnToStr,
                Functions.returnConstant(""));
    }
}
