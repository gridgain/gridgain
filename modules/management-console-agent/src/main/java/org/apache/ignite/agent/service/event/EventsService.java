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
import org.apache.ignite.agent.service.sender.ManagementConsoleSender;
import org.apache.ignite.agent.service.sender.RetryableSender;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.event.VisorGridEvent;
import org.apache.ignite.lang.IgniteBiPredicate;

import static org.apache.ignite.agent.StompDestinationsUtils.buildEventsDest;
import static org.apache.ignite.agent.service.event.EventsExporter.TOPIC_EVTS;

/**
 * Events service.
 */
public class EventsService extends GridProcessorAdapter {
    /** Queue capacity. */
    private static final int QUEUE_CAP = 100;

    /** Worker. */
    private final RetryableSender<VisorGridEvent> snd;

    /** On node traces listener. */
    private final IgniteBiPredicate<UUID, List<VisorGridEvent>> lsnr = this::processEvents;

    /**
     * @param ctx Context.
     * @param mgr Manager.
     */
    public EventsService(GridKernalContext ctx, WebSocketManager mgr) {
        super(ctx);

        snd = new ManagementConsoleSender<>(
            ctx,
            mgr,
            buildEventsDest(ctx.cluster().get().id()),
            "mgmt-console-events-sender-",
            QUEUE_CAP
        );

        ctx.grid().message().localListen(TOPIC_EVTS, lsnr);
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) {
        ctx.grid().message().stopLocalListen(TOPIC_EVTS, lsnr);

        U.closeQuiet(snd);
    }

    /**
     * @param nid Node id.
     * @param evts Events.
     */
    boolean processEvents(UUID nid, List<VisorGridEvent> evts) {
        snd.send(evts);

        return true;
    }
}
