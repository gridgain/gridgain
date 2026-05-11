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

package org.apache.ignite.internal.processors.rest.handlers.drain;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCachePreloader;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPreloader;
import org.apache.ignite.internal.processors.rest.GridRestCommand;
import org.apache.ignite.internal.processors.rest.GridRestResponse;
import org.apache.ignite.internal.processors.rest.handlers.GridRestCommandHandlerAdapter;
import org.apache.ignite.internal.processors.rest.request.GridRestRequest;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.processors.rest.GridRestCommand.SUPPLY_STATUS;

/**
 * Handler for {@link GridRestCommand#SUPPLY_STATUS}. Reports whether this node
 * is currently supplying partition data to peers.
 *
 * <p>Always returns {@code STATUS_SUCCESS} (HTTP 200). The supply state is
 * informational only — the design forbids using it as a
 * readiness gate. The endpoint is consumed by the helm preStop hook (to wait
 * for outbound supply completion before returning) and by the operator (as an
 * advisory pre-delete signal).</p>
 *
 * <p>This handler MUST NOT influence readiness, MUST NOT trigger PME, and
 * MUST NOT pause/cancel supply.</p>
 */
public class GridSupplyStatusCommandHandler extends GridRestCommandHandlerAdapter {
    /** Supported commands. */
    private static final Collection<GridRestCommand> SUPPORTED_COMMANDS = U.sealList(SUPPLY_STATUS);

    /**
     * @param ctx Kernal context.
     */
    public GridSupplyStatusCommandHandler(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public Collection<GridRestCommand> supportedCommands() {
        return SUPPORTED_COMMANDS;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<GridRestResponse> handleAsync(GridRestRequest req) {
        assert req != null;
        assert SUPPORTED_COMMANDS.contains(req.command());

        if (log.isDebugEnabled())
            log.debug("supply-status command handler invoked.");

        boolean supplying = false;
        List<String> supplyingGroups = new ArrayList<>();

        // ctx.cache() may be null very early during start; guard.
        if (ctx.cache() != null) {
            for (CacheGroupContext grpCtx : ctx.cache().cacheGroups()) {
                if (grpCtx.isLocal() || grpCtx.systemCache())
                    continue;

                GridCachePreloader preloader = grpCtx.preloader();

                if (preloader instanceof GridDhtPreloader) {
                    GridDhtPreloader dht = (GridDhtPreloader)preloader;

                    // Defensive null-check: supplier() may not be set during very early lifecycle.
                    if (dht.supplier() != null && dht.supplier().isSupply()) {
                        supplying = true;
                        supplyingGroups.add(grpCtx.cacheOrGroupName());
                    }
                }
            }
        }

        return new GridFinishedFuture<>(new GridRestResponse(new SupplyStatusResponse(supplying, supplyingGroups)));
    }
}
