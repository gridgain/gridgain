/*
 * Copyright 2025 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.rest.handlers.warmup;

import java.util.Collection;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.rest.GridRestCommand;
import org.apache.ignite.internal.processors.rest.GridRestResponse;
import org.apache.ignite.internal.processors.rest.handlers.GridRestCommandHandlerAdapter;
import org.apache.ignite.internal.processors.rest.request.GridRestRequest;
import org.apache.ignite.internal.processors.rest.request.GridRestWarmUpRequest;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Command handler for managing state of a node before it starts and getting information about it.
 */
public class NodeWarmupCommandHandler extends GridRestCommandHandlerAdapter {
    /** Supported commands. */
    private static final List<GridRestCommand> SUPPORTED_COMMANDS = U.sealList(GridRestCommand.WARM_UP);

    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     */
    public NodeWarmupCommandHandler(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public Collection<GridRestCommand> supportedCommands() {
        return SUPPORTED_COMMANDS;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<GridRestResponse> handleAsync(GridRestRequest req) {
        if (log.isDebugEnabled())
            log.debug("Handling REST request: " + req);

        try {
            if (req.command() == GridRestCommand.WARM_UP) {
                GridRestWarmUpRequest warmUpReq = (GridRestWarmUpRequest)req;

                if (warmUpReq.stopWarmUp())
                    ctx.cache().stopWarmUp();

                return new GridFinishedFuture<>(new GridRestResponse());
            }
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to execute cache command: " + req, e);

            return new GridFinishedFuture<>(e);
        }

        return new GridFinishedFuture<>();
    }
}
