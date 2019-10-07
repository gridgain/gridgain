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

package org.gridgain.service.tracing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;
import com.google.common.collect.Lists;
import io.opencensus.common.Timestamp;
import io.opencensus.trace.SpanContext;
import io.opencensus.trace.SpanId;
import io.opencensus.trace.TraceId;
import io.opencensus.trace.TraceOptions;
import io.opencensus.trace.Tracestate;
import io.opencensus.trace.export.SpanData;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.tracing.TracingSpi;
import org.gridgain.dto.tracing.Span;
import org.gridgain.service.AbstractServiceTest;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.gridgain.agent.StompDestinationsUtils.buildSaveSpanDest;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tracing service test.
 */
public class TracingServiceTest extends AbstractServiceTest {
    /**
     * Should register handler and export spans.
     */
    @Test
    public void shouldSendSpanList() {
        TracingService srvc = new TracingService(getMockContext(), mgr);

        List<SpanData> spanData = getSpanData();

        List<Span> expSpans = spanData.stream().map(GmcSpanExporter::fromSpanDataToSpan).collect(Collectors.toList());

        srvc.onNodeTraces(UUID.randomUUID(), expSpans);

        ArgumentCaptor<String> destCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Object> payloadCaptor = ArgumentCaptor.forClass(Object.class);
        verify(mgr, timeout(100).times(1)).send(destCaptor.capture(), payloadCaptor.capture());

        List<Span> actualSpans = (List<Span>) payloadCaptor.getValue();

        Assert.assertEquals(buildSaveSpanDest(UUID.fromString("a-a-a-a-a")), destCaptor.getValue());
        Assert.assertEquals(expSpans.size(), actualSpans.size());
    }

    /**
     * @return Span data list.
     */
    private List<SpanData> getSpanData() {
        return Lists.newArrayList(
            SpanData.create(
                SpanContext.create(TraceId.generateRandomId(new Random()), SpanId.generateRandomId(new Random()), TraceOptions.DEFAULT, Tracestate.builder().build()),
                SpanId.generateRandomId(new Random()),
                false,
                "name",
                null,
                Timestamp.create(10, 10),
                SpanData.Attributes.create(new HashMap<>(), 0),
                SpanData.TimedEvents.create(new ArrayList<>(), 0),
                SpanData.TimedEvents.create(new ArrayList<>(), 0),
                SpanData.Links.create(new ArrayList<>(), 0),
                null,
                null,
                Timestamp.create(20, 20)
            )
        );
    }


    /** {@inheritDoc} */
    @Override protected GridKernalContext getMockContext() {
        GridKernalContext ctx = super.getMockContext();

        IgniteConfiguration cfg = mock(IgniteConfiguration.class);
        TracingSpi tracingSpi = mock(TracingSpi.class);

        when(ctx.config()).thenReturn(cfg);
        when(cfg.getTracingSpi()).thenReturn(tracingSpi);

        return ctx;
    }
}
