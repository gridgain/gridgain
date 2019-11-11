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

import java.util.List;
import java.util.UUID;
import org.apache.ignite.agent.WebSocketManager;
import org.apache.ignite.agent.dto.tracing.Span;
import org.apache.ignite.agent.service.sender.ManagementConsoleSender;
import org.apache.ignite.agent.service.sender.RetryableSender;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;

import static org.apache.ignite.agent.StompDestinationsUtils.buildSaveSpanDest;
import static org.apache.ignite.agent.service.tracing.ManagementConsoleSpanExporter.TOPIC_SPANS;

/**
 * Tracing service.
 */
public class TracingService extends GridProcessorAdapter {
    /** Queue capacity. */
    private static final int QUEUE_CAP = 100;

    /** Manager. */
    private final WebSocketManager mgr;

    /** On node traces listener. */
    private final IgniteBiPredicate<UUID, Object> lsnr = this::processSpans;

    /** Worker. */
    private final RetryableSender<Span> snd;

    /**
     * @param ctx Context.
     * @param mgr Manager.
     */
    public TracingService(GridKernalContext ctx, WebSocketManager mgr) {
        super(ctx);
        this.mgr = mgr;
        this.snd = createSender();

        ctx.grid().message().localListen(TOPIC_SPANS, lsnr);
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) {
        ctx.grid().message().stopLocalListen(TOPIC_SPANS, lsnr);
        U.closeQuiet(snd);
    }

    /**
     * @param uuid Uuid.
     * @param spans Spans.
     */
    boolean processSpans(UUID uuid, Object spans) {
        snd.send((List<Span>) spans);

        return true;
    }

    /**
     * @return Sender which send messages from queue to Management Console.
     */
    private RetryableSender<Span> createSender() {
        return new ManagementConsoleSender<>(
            ctx,
            mgr,
            buildSaveSpanDest(ctx.cluster().get().id()),
            "mgmt-console-tracing-sender-",
            QUEUE_CAP
        );
    }
}
