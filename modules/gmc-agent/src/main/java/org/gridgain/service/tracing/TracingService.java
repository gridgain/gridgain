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

import java.util.List;
import java.util.UUID;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.gridgain.agent.RetryableSender;
import org.gridgain.agent.WebSocketManager;
import org.gridgain.dto.Span;

import static org.gridgain.agent.StompDestinationsUtils.buildSaveSpanDest;
import static org.gridgain.service.tracing.GmcSpanExporter.*;

/**
 * Tracing service.
 */
public class TracingService implements AutoCloseable {
    /** Queue capacity. */
    private static final int QUEUE_CAP = 100;

    /** Logger. */
    private final IgniteLogger log;

    /** Context. */
    private final GridKernalContext ctx;

    /** Manager. */
    private final WebSocketManager mgr;

    /** On node traces listener. */
    private final IgniteBiPredicate<UUID, Object> lsnr = this::onNodeTraces;

    /** Worker. */
    private final RetryableSender<Span> snd;

    /**
     * @param ctx Context.
     * @param mgr Manager.
     */
    public TracingService(GridKernalContext ctx, WebSocketManager mgr) {
        this.ctx = ctx;
        this.mgr = mgr;
        this.log = ctx.log(TracingService.class);
        this.snd = createSenderWorker();

        ctx.grid().message().localListen(TRACING_TOPIC, lsnr);
    }

    /** {@inheritDoc} */
    @Override public void close() {
        ctx.grid().message().stopLocalListen(TRACING_TOPIC, lsnr);
        U.closeQuiet(snd);
    }

    /**
     * @param uuid Uuid.
     * @param spans Spans.
     */
    boolean onNodeTraces(UUID uuid, Object spans) {
        snd.send((List<Span>) spans);

        return true;
    }

    /**
     * @return Sender which send messages from queue to gmc.
     */
    private RetryableSender<Span> createSenderWorker() {
        return new RetryableSender<>(log, QUEUE_CAP, (b) -> {
            if (!mgr.send(buildSaveSpanDest(ctx.cluster().get().id()), b))
                throw new IgniteException("Failed to send message with spans");
        });
    }
}
