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

package org.gridgain.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import com.google.common.collect.Lists;
import io.opencensus.common.Function;
import io.opencensus.common.Functions;
import io.opencensus.common.Timestamp;
import io.opencensus.trace.AttributeValue;
import io.opencensus.trace.SpanContext;
import io.opencensus.trace.Status;
import io.opencensus.trace.export.SpanData;
import io.opencensus.trace.export.SpanExporter;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.gridgain.agent.WebSocketManager;
import org.gridgain.dto.Span;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.gridgain.agent.StompDestinationsUtils.buildSaveSpanDest;

/**
 * Tracing service.
 */
public class TracingService {
    /** Status code. */
    private static final String STATUS_CODE = "census.status_code";

    /** Status description. */
    private static final String STATUS_DESCRIPTION = "census.status_description";

    /** Context. */
    private GridKernalContext ctx;

    /** Manager. */
    private WebSocketManager mgr;

    /** Logger. */
    private IgniteLogger log;

    /** Handler. */
    private SpanExporter.Handler hnd;

    /**
     * @param ctx Context.
     * @param mgr Manager.
     */
    public TracingService(GridKernalContext ctx, WebSocketManager mgr) {
        this.ctx = ctx;
        this.mgr = mgr;
        this.log = ctx.log(TracingService.class);
    }

    /**
     * Register span exporter handler.
     */
    public void registerHandler() {
        if (ctx.config().getTracingSpi().getTraceComponent() == null)
            return;

        hnd = getTraceHandler();
        ctx.config().getTracingSpi().getTraceComponent().getExportComponent().getSpanExporter().registerHandler("gmc", hnd);
    }

    /**
     * Force send buffered spans.
     */
    public void flushBuffer() {
        if (hnd == null)
            return;

        hnd.export(Collections.emptyList());
    }

    /**
     * @return Span exporter handler.
     */
    private SpanExporter.Handler getTraceHandler() {
        return new SpanExporter.Handler() {
            /** Buffer. */
            private final List<Span> buf = Collections.synchronizedList(new ArrayList<>());

            @Override public void export(Collection<SpanData> spanDataList) {
                spanDataList.forEach(s -> buf.add(fromSpanDataToSpan(s)));

                if (log.isDebugEnabled())
                    buf.forEach((s) -> log.debug("Sending span to GMC: " + s));

                if (mgr.getSession() != null && mgr.getSession().isConnected()) {
                    mgr.getSession().send(buildSaveSpanDest(ctx.cluster().get().id()), Lists.newArrayList(buf));
                    buf.clear();
                }
            }
        };
    }

    /**
     * @param spanData Span data.
     */
     Span fromSpanDataToSpan(SpanData spanData) {
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

        return span;
    }

    /**
     * @param ts Timestamp.
     */
    private long toEpochMillis(Timestamp ts) {
        return SECONDS.toMillis(ts.getSeconds()) + NANOSECONDS.toMillis(ts.getNanos());
    }

    /** Return to string. */
    private final Function<Object, String> returnToStr =
            Functions.returnToString();

    /**
     * @param attributeVal Attribute value.
     */
    private String attributeValueToString(AttributeValue attributeVal) {
        return attributeVal.match(
                returnToStr,
                returnToStr,
                returnToStr,
                returnToStr,
                Functions.returnConstant(""));
    }
}
