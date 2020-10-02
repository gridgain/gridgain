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
import org.apache.ignite.internal.processors.rest.GridRestCommand;
import org.apache.ignite.internal.processors.rest.GridRestResponse;
import org.apache.ignite.internal.processors.rest.handlers.GridRestCommandHandlerAdapter;
import org.apache.ignite.internal.processors.rest.request.GridRestRequest;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.processors.rest.GridRestCommand.PROBE;

/**
 * Handler for {@link GridRestCommand#VERSION} and {@link GridRestCommand#NAME} command.
 */
public class GridProbeCommandHandler extends GridRestCommandHandlerAdapter {
    /**
     * @param ctx Context.
     */
    public GridProbeCommandHandler(GridKernalContext ctx) {
        super(ctx);
    }

    /** Supported commands. */
    private static final Collection<GridRestCommand> SUPPORTED_COMMANDS = U.sealList(PROBE);

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

                while (IgnitionEx.grid(ctx.igniteInstanceName()) == null) {
                    try {
                        Thread.sleep(1000L);
                    }
                    catch (InterruptedException e) {
                        U.error(log, "", e);
                    }
                }

                return new GridFinishedFuture<>(new GridRestResponse("true"));

            }

        }

        return new GridFinishedFuture<>();
    }
}