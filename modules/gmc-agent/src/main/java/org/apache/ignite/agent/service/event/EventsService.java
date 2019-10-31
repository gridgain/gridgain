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

package org.apache.ignite.agent.service.event;

import java.util.List;
import java.util.UUID;
import org.apache.ignite.agent.WebSocketManager;
import org.apache.ignite.agent.service.sender.GmcSender;
import org.apache.ignite.agent.service.sender.RetryableSender;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.event.VisorGridEvent;
import org.apache.ignite.lang.IgniteBiPredicate;

import static org.apache.ignite.agent.StompDestinationsUtils.buildEventsDest;
import static org.apache.ignite.agent.service.event.EventsExporter.EVENTS_TOPIC;

/**
 * Events service.
 */
public class EventsService implements AutoCloseable {
    /** Queue capacity. */
    private static final int QUEUE_CAP = 100;

    /** Context. */
    private GridKernalContext ctx;

    /** Worker. */
    private final RetryableSender<VisorGridEvent> snd;

    /** On node traces listener. */
    private final IgniteBiPredicate<UUID, List<VisorGridEvent>> lsnr = this::onEvents;

    /**
     * @param ctx Context.
     * @param mgr Manager.
     */
    public EventsService(GridKernalContext ctx, WebSocketManager mgr) {
        this.ctx = ctx;

        snd = new GmcSender<>(ctx, mgr, QUEUE_CAP, buildEventsDest(ctx.cluster().get().id()));

        ctx.grid().message().localListen(EVENTS_TOPIC, lsnr);
    }

    /** {@inheritDoc} */
    @Override public void close() {
        ctx.grid().message().stopLocalListen(EVENTS_TOPIC, lsnr);

        U.closeQuiet(snd);
    }

    /**
     * @param nid Node id.
     * @param evts Events.
     */
    boolean onEvents(UUID nid, List<VisorGridEvent> evts) {
        snd.send(evts);

        return true;
    }
}
