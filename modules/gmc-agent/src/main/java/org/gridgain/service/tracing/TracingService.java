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

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.gridgain.agent.WebSocketManager;
import org.gridgain.dto.span.SpanBatch;

import static org.gridgain.agent.StompDestinationsUtils.buildSaveSpanDest;
import static org.gridgain.service.tracing.GmcSpanExporter.*;

/**
 * Tracing service.
 */
public class TracingService implements AutoCloseable {
    /** Queue capacity. */
    private static final int QUEUE_CAP = 100;

    /** Logger. */
    private IgniteLogger log;

    /** Context. */
    private GridKernalContext ctx;

    /** Manager. */
    private WebSocketManager mgr;

    /** On node traces listener. */
    private IgniteBiPredicate<UUID, Object> onNodeTraces = this::onNodeTraces;

    /** Executor service. */
    private final ExecutorService exSrvc = Executors.newSingleThreadExecutor();

    /** Worker. */
    private final RetryableSender<SpanBatch> worker;

    /**
     * @param ctx Context.
     * @param mgr Manager.
     */
    public TracingService(GridKernalContext ctx, WebSocketManager mgr) {
        this.ctx = ctx;
        this.mgr = mgr;
        this.log = ctx.log(TracingService.class);
        this.worker = createSenderWorker();

        ctx.grid().message().localListen(TRACING_TOPIC, onNodeTraces);

        exSrvc.submit(worker);
    }

    /** {@inheritDoc} */
    @Override public void close() {
        ctx.grid().message().stopLocalListen(TRACING_TOPIC, onNodeTraces);
        exSrvc.shutdown();
    }

    /**
     * @param uuid Uuid.
     * @param spans Spans.
     */
    boolean onNodeTraces(UUID uuid, Object spans) {
        worker.addToSendQueue((SpanBatch) spans);

        return true;
    }

    /**
     * @return Sender which send messages from queue to gmc.
     */
    private RetryableSender<SpanBatch> createSenderWorker() {
        return new RetryableSender<>(log, QUEUE_CAP, (b) -> {
            if (!mgr.send(buildSaveSpanDest(ctx.cluster().get().id()), b.getList()))
                throw new IgniteException("Failed to send message with spans");
        });
    }
}
