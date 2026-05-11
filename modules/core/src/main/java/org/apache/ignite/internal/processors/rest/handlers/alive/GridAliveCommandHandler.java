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

package org.apache.ignite.internal.processors.rest.handlers.alive;

import java.util.Collection;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.processors.rest.GridRestCommand;
import org.apache.ignite.internal.processors.rest.GridRestResponse;
import org.apache.ignite.internal.processors.rest.handlers.GridRestCommandHandlerAdapter;
import org.apache.ignite.internal.processors.rest.request.GridRestRequest;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.processors.rest.GridRestCommand.ALIVE;

/**
 * Handler for {@link GridRestCommand#ALIVE}. Liveness/startup gate — returns
 * {@code 200} when the JVM kernel has started, {@code 503} otherwise. No
 * partition, supply, or drain logic.
 *
 * <p>Used for the Kubernetes {@code livenessProbe} and {@code startupProbe}.
 * Liveness must NEVER return 503 because the node is rebalancing, supplying,
 * or being drained — those are all healthy states. Killing the pod in any of
 * these states triggers a PME, cancels the rebalance, and restarts the cycle.
 * {@code cmd=alive} answers only "is the node running?"</p>
 */
public class GridAliveCommandHandler extends GridRestCommandHandlerAdapter {
    /** Supported commands. */
    private static final Collection<GridRestCommand> SUPPORTED_COMMANDS = U.sealList(ALIVE);

    /**
     * @param ctx Kernal context.
     */
    public GridAliveCommandHandler(GridKernalContext ctx) {
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
            log.debug("alive command handler invoked.");

        return new GridFinishedFuture<>(IgnitionEx.hasKernalStarted(ctx.igniteInstanceName())
            ? new GridRestResponse("alive")
            : new GridRestResponse(GridRestResponse.SERVICE_UNAVAILABLE, "not started"));
    }
}
