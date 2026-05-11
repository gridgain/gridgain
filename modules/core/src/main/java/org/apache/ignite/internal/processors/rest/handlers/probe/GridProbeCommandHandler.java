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

package org.apache.ignite.internal.processors.rest.handlers.probe;

import java.util.Collection;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCachePreloader;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPreloader;
import org.apache.ignite.internal.processors.rest.GridRestCommand;
import org.apache.ignite.internal.processors.rest.GridRestResponse;
import org.apache.ignite.internal.processors.rest.handlers.GridRestCommandHandler;
import org.apache.ignite.internal.processors.rest.handlers.GridRestCommandHandlerAdapter;
import org.apache.ignite.internal.processors.rest.handlers.drain.GridDrainCommandHandler;
import org.apache.ignite.internal.processors.rest.request.GridRestRequest;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.processors.rest.GridRestCommand.NODE_STATE_BEFORE_START;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.PROBE;

/**
 * Handler for {@link GridRestCommand#PROBE}.
 *
 * <p>Per HLD v14 §2 ({@code 14-probe-rework.md}), {@code cmd=probe} is a
 * <b>readiness gate</b>. It returns {@code 200} only when ALL of the
 * following hold:</p>
 * <ol>
 *     <li>The kernel has started ({@code IgnitionEx.hasKernalStarted(...)}).</li>
 *     <li>The drain flag is not set
 *         ({@link GridDrainCommandHandler#drainingFlag()} {@code == false}).</li>
 *     <li>No PME is in progress
 *         ({@code ctx.cache().context().exchange().lastTopologyFuture().isDone()}).</li>
 *     <li>No non-system cache group has active outbound supply
 *         ({@code ((GridDhtPreloader) grpCtx.preloader()).supplier().isSupply() == false}).</li>
 * </ol>
 *
 * <p>If any condition fails, returns
 * {@link GridRestResponse#SERVICE_UNAVAILABLE} (HTTP 503) with the reason in
 * the {@code error} field.</p>
 *
 * <p>The {@code NODE_STATE_BEFORE_START} branch is unchanged.</p>
 */
public class GridProbeCommandHandler extends GridRestCommandHandlerAdapter {
    /**
     * @param ctx Context.
     */
    public GridProbeCommandHandler(GridKernalContext ctx) {
        super(ctx);
    }

    /** Supported commands. */
    private static final Collection<GridRestCommand> SUPPORTED_COMMANDS = U.sealList(PROBE, NODE_STATE_BEFORE_START);

    /** {@inheritDoc} */
    @Override public Collection<GridRestCommand> supportedCommands() {
        return SUPPORTED_COMMANDS;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<GridRestResponse> handleAsync(GridRestRequest req) {
        assert req != null;

        assert SUPPORTED_COMMANDS.contains(req.command());

        switch (req.command()) {
            case PROBE: {
                if (log.isDebugEnabled())
                    log.debug("probe command handler invoked.");

                // Step 1 — kernel started.
                if (!IgnitionEx.hasKernalStarted(ctx.igniteInstanceName()))
                    return notReady("grid has not started");

                // Step 2 — drain flag not set.
                if (isDraining())
                    return notReady("draining");

                // Step 3 — no PME in progress.
                if (!isPmeIdle())
                    return notReady("PME in progress");

                // Step 4 — no active outbound supply.
                String supplyingGrp = anySupplyingCacheGroup();

                if (supplyingGrp != null)
                    return notReady("supplying: " + supplyingGrp);

                return new GridFinishedFuture<>(new GridRestResponse("grid has started"));
            }

            case NODE_STATE_BEFORE_START: {
                if (log.isDebugEnabled())
                    log.debug(NODE_STATE_BEFORE_START.key() + " command handler invoked.");

                return new GridFinishedFuture<>(new GridRestResponse());
            }
        }

        return new GridFinishedFuture<>();
    }

    /**
     * @param reason 503 reason.
     * @return Future carrying a SERVICE_UNAVAILABLE response.
     */
    private static IgniteInternalFuture<GridRestResponse> notReady(String reason) {
        return new GridFinishedFuture<>(new GridRestResponse(GridRestResponse.SERVICE_UNAVAILABLE, reason));
    }

    /**
     * Read the drain flag from the registered drain handler instance. If REST
     * is not started yet (handlers map empty) or the drain handler is not
     * registered, treat as not draining.
     *
     * @return {@code true} if drain is active.
     */
    private boolean isDraining() {
        if (ctx.rest() == null)
            return false;

        GridRestCommandHandler hnd = ctx.rest().getHandler(GridRestCommand.DRAIN);

        return hnd instanceof GridDrainCommandHandler && ((GridDrainCommandHandler)hnd).drainingFlag();
    }

    /**
     * @return {@code true} if the last topology future is done (no PME in
     *     progress). Returns {@code true} defensively if the cache subsystem
     *     is not available yet.
     */
    private boolean isPmeIdle() {
        if (ctx.cache() == null || ctx.cache().context() == null || ctx.cache().context().exchange() == null)
            return true;

        return ctx.cache().context().exchange().lastTopologyFuture() == null
            || ctx.cache().context().exchange().lastTopologyFuture().isDone();
    }

    /**
     * Iterate non-system, non-local cache groups and return the name of the
     * first group whose DHT preloader's supplier reports {@code isSupply() ==
     * true}. Returns {@code null} if no group is supplying.
     *
     * @return Name of a supplying cache group, or {@code null}.
     */
    private String anySupplyingCacheGroup() {
        if (ctx.cache() == null)
            return null;

        for (CacheGroupContext grpCtx : ctx.cache().cacheGroups()) {
            if (grpCtx.isLocal() || grpCtx.systemCache())
                continue;

            GridCachePreloader preloader = grpCtx.preloader();

            if (preloader instanceof GridDhtPreloader) {
                GridDhtPreloader dht = (GridDhtPreloader)preloader;

                if (dht.supplier() != null && dht.supplier().isSupply())
                    return grpCtx.cacheOrGroupName();
            }
        }

        return null;
    }
}
