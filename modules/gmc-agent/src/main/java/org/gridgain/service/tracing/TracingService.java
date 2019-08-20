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
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import com.google.common.collect.Lists;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.gridgain.agent.WebSocketManager;
import org.gridgain.dto.span.Span;
import org.gridgain.dto.span.SpanList;

import static org.gridgain.agent.StompDestinationsUtils.buildSaveSpanDest;
import static org.gridgain.service.tracing.GmcSpanExporter.*;

/**
 * Tracing service.
 */
public class TracingService implements AutoCloseable {
    /** Context. */
    private GridKernalContext ctx;

    /** Manager. */
    private WebSocketManager mgr;

    /** On node traces listener. */
    private IgniteBiPredicate<UUID, Object> onNodeTraces = this::onNodeTraces;

    /** Buffer. */
    private final List<Span> buf = Collections.synchronizedList(new ArrayList<>());

    /**
     * @param ctx Context.
     * @param mgr Manager.
     */
    public TracingService(GridKernalContext ctx, WebSocketManager mgr) {
        this.ctx = ctx;
        this.mgr = mgr;

        ctx.grid().message().localListen(TRACING_TOPIC, onNodeTraces);
    }

    /**
     * Send buffered spans.
     */
    public void sendInitialState() {
        send(new SpanList());
    }

    /** {@inheritDoc} */
    @Override public void close() {
        ctx.grid().message().stopLocalListen(TRACING_TOPIC, onNodeTraces);
    }

    /**
     * @param uuid Uuid.
     * @param spans Spans.
     */
    boolean onNodeTraces(UUID uuid, Object spans) {
        send((SpanList) spans);

        return true;
    }

    /**
     * @param spans Spans.
     */
    private void send(SpanList spans) {
        buf.addAll(spans.getList());

        if (mgr.send(buildSaveSpanDest(ctx.cluster().get().id()), Lists.newArrayList(buf)))
            buf.clear();
    }
}
