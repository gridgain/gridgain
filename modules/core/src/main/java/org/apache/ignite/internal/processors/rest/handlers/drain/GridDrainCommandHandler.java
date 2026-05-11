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

import java.util.Collection;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.rest.GridRestCommand;
import org.apache.ignite.internal.processors.rest.GridRestResponse;
import org.apache.ignite.internal.processors.rest.handlers.GridRestCommandHandlerAdapter;
import org.apache.ignite.internal.processors.rest.request.GridRestDrainRequest;
import org.apache.ignite.internal.processors.rest.request.GridRestRequest;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.processors.rest.GridRestCommand.DRAIN;

/**
 * Handler for {@link GridRestCommand#DRAIN}. Per HLD v14 §3, exposes three
 * operations:
 * <ul>
 *     <li>{@code action=start} — sets a process-scoped {@code drainingFlag}
 *         (auth required at the processor layer).</li>
 *     <li>{@code action=stop} — clears the flag (auth required). Operator
 *         use case: abort a rolling update mid-drain and restore the pod to
 *         service without restarting it.</li>
 *     <li>{@code action=status} — returns the current drain flag and the
 *         active thin-client connection count (auth-exempt).</li>
 * </ul>
 *
 * <p>Readiness is consumed via {@code cmd=probe} (see
 * {@link org.apache.ignite.internal.processors.rest.handlers.probe.GridProbeCommandHandler}).
 * {@code cmd=drain} no longer has a bare-GET readiness ladder.</p>
 *
 * <p>The drain flag is JVM-scoped — automatically cleared on pod restart by
 * virtue of being a fresh field on a fresh handler instance. There is no
 * metastore round-trip or init container.</p>
 *
 * <p>This handler MUST NOT touch {@code TcpDiscoverySpi}, MUST NOT trigger
 * PME, and MUST NOT pause/cancel in-flight supply messages on the drain
 * path. The drain plane operates in the Kubernetes Service-endpoints plane;
 * cluster-membership transitions are deferred to SIGTERM and
 * {@code onKernalStop()}.</p>
 */
public class GridDrainCommandHandler extends GridRestCommandHandlerAdapter {
    /** Supported commands. */
    private static final Collection<GridRestCommand> SUPPORTED_COMMANDS = U.sealList(DRAIN);

    /** Action constant for the {@code start} sub-command. */
    public static final String ACTION_START = "start";

    /** Action constant for the {@code stop} sub-command. */
    public static final String ACTION_STOP = "stop";

    /** Action constant for the {@code status} sub-command. */
    public static final String ACTION_STATUS = "status";

    /**
     * Process-scoped drain flag. {@code volatile} for visibility across REST
     * worker threads and the {@code cmd=probe} handler that reads it. No
     * metastore round-trip — automatically reset on JVM restart.
     */
    private volatile boolean drainingFlag = false;

    /**
     * @param ctx Kernal context.
     */
    public GridDrainCommandHandler(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public Collection<GridRestCommand> supportedCommands() {
        return SUPPORTED_COMMANDS;
    }

    /**
     * Public accessor for the drain flag. Read by
     * {@code GridProbeCommandHandler} via {@code ctx.rest().getHandler(DRAIN)}
     * and by tests.
     *
     * @return Current drain flag value.
     */
    public boolean drainingFlag() {
        return drainingFlag;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<GridRestResponse> handleAsync(GridRestRequest req) {
        assert req != null;
        assert SUPPORTED_COMMANDS.contains(req.command());

        if (log.isDebugEnabled())
            log.debug("drain command handler invoked: " + req);

        String action = null;

        if (req instanceof GridRestDrainRequest)
            action = ((GridRestDrainRequest)req).action();

        if (ACTION_START.equalsIgnoreCase(action)) {
            drainingFlag = true;

            if (log.isInfoEnabled())
                log.info("Drain flag set; cmd=probe will report 503 (draining) until cmd=drain&action=stop or pod restart.");

            return new GridFinishedFuture<>(new GridRestResponse("draining"));
        }

        if (ACTION_STOP.equalsIgnoreCase(action)) {
            drainingFlag = false;

            if (log.isInfoEnabled())
                log.info("Drain flag cleared; cmd=probe will resume normal readiness behaviour.");

            return new GridFinishedFuture<>(new GridRestResponse("ready"));
        }

        if (ACTION_STATUS.equalsIgnoreCase(action)) {
            int connCnt = ctx.sqlListener() != null ? ctx.sqlListener().connectionsCount() : 0;

            return new GridFinishedFuture<>(new GridRestResponse(new DrainStatusResponse(drainingFlag, connCnt)));
        }

        // Unknown / missing action. Per v14 §3, cmd=drain has no bare-GET path.
        return new GridFinishedFuture<>(new GridRestResponse(GridRestResponse.STATUS_FAILED,
            "Unknown drain action: " + action + " (expected one of: start, stop, status)"));
    }
}
