/*
 * Copyright 2026 GridGain Systems, Inc. and Contributors.
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
import java.util.List;
import org.apache.ignite.ShutdownPolicy;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.ShutdownPolicyHandler;
import org.apache.ignite.internal.processors.rest.GridRestCommand;
import org.apache.ignite.internal.processors.rest.GridRestResponse;
import org.apache.ignite.internal.processors.rest.handlers.GridRestCommandHandlerAdapter;
import org.apache.ignite.internal.processors.rest.request.GridRestDrainRequest;
import org.apache.ignite.internal.processors.rest.request.GridRestRequest;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.rest.GridRestCommand.DRAIN;

/**
 * Handler for {@link GridRestCommand#DRAIN} — the k8s pod-lifecycle flag for rolling restarts.
 * {@code action=start} sets a process-scoped {@code drainingFlag} (read by {@code cmd=probe&kind=readiness},
 * which then 503s and removes the pod from Service endpoints); under {@code ShutdownPolicy.GRACEFUL} the
 * unique-data guard refuses it (503, {@link DrainUniqueDataResponse}) when this node is the sole owner of a
 * partition unless {@code force=true}. {@code action=stop} clears the flag (operator rollback);
 * {@code action=status} reports it (auth-exempt). The flag is volatile JVM-heap state, reset on restart.
 * Setting it MUST NOT touch discovery / PME / supply; the guard only reads ownership.
 */
public class GridDrainCommandHandler extends GridRestCommandHandlerAdapter {
    /** Supported commands. */
    private static final Collection<GridRestCommand> SUPPORTED_COMMANDS = U.sealList(DRAIN);

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
     * Public accessor for the drain flag. Read by {@code GridProbeCommandHandler}
     * via {@code ctx.rest().getHandler(DRAIN)} and by tests.
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

        GridRestDrainRequest.Action action = req instanceof GridRestDrainRequest
            ? ((GridRestDrainRequest)req).action() : null;

        if (action == null) {
            return new GridFinishedFuture<>(new GridRestResponse(GridRestResponse.STATUS_FAILED,
                "drain action is required (expected one of: start, stop, status)"));
        }

        switch (action) {
            case START:
                GridRestResponse guard = uniqueDataGuard(req);

                if (guard != null)
                    return new GridFinishedFuture<>(guard);

                drainingFlag = true;

                if (log.isInfoEnabled())
                    log.info("Drain flag set; cmd=probe&kind=readiness will report 503 (draining) until " +
                        "cmd=drain&action=stop or pod restart.");

                return new GridFinishedFuture<>(new GridRestResponse("draining"));

            case STOP:
                drainingFlag = false;

                if (log.isInfoEnabled())
                    log.info("Drain flag cleared; cmd=probe&kind=readiness will resume normal readiness behaviour.");

                return new GridFinishedFuture<>(new GridRestResponse("ready"));

            case STATUS:
                return new GridFinishedFuture<>(new GridRestResponse(new DrainStatusResponse(drainingFlag)));

            default:
                throw new AssertionError("Unhandled drain action: " + action);
        }
    }

    /**
     * Unique-data guard for {@code action=start}. Under
     * {@link ShutdownPolicy#GRACEFUL}, if this node is the only owner of any
     * non-system cache group's partitions, refuses the drain with {@code 503}
     * (so SIGTERM would not make that data unavailable) unless {@code force=true}.
     * Under {@link ShutdownPolicy#IMMEDIATE} the guard is not applied (data loss
     * is accepted by design). Reuses
     * {@link ShutdownPolicyHandler#uniqueDataCacheGroups(GridKernalContext)},
     * the same ownership logic GRACEFUL uses.
     *
     * @param req Drain request.
     * @return A {@code 503} response if the drain is blocked, or {@code null} to proceed.
     */
    @Nullable private GridRestResponse uniqueDataGuard(GridRestRequest req) {
        boolean force = req instanceof GridRestDrainRequest && ((GridRestDrainRequest)req).force();

        if (force || ctx.config().getShutdownPolicy() != ShutdownPolicy.GRACEFUL)
            return null;

        List<String> grps = ShutdownPolicyHandler.uniqueDataCacheGroups(ctx);

        if (grps.isEmpty())
            return null;

        if (log.isInfoEnabled())
            log.info("Refusing cmd=drain&action=start: this node is the only owner of partitions for cache groups "
                + grps + " under ShutdownPolicy.GRACEFUL; pass force=true to override (accepts data loss).");

        GridRestResponse resp = new GridRestResponse(GridRestResponse.SERVICE_UNAVAILABLE,
            "unique data held on cache groups: " + grps);

        resp.setResponse(new DrainUniqueDataResponse(grps));

        return resp;
    }
}
