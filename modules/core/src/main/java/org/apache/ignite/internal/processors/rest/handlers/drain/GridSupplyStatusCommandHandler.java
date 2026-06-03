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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import org.apache.ignite.Ignition;
import org.apache.ignite.ShutdownPolicy;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.ShutdownPolicyHandler;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCachePreloader;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPreloader;
import org.apache.ignite.internal.processors.cache.dr.IgniteReplicationStateProvider;
import org.apache.ignite.internal.processors.rest.GridRestCommand;
import org.apache.ignite.internal.processors.rest.GridRestResponse;
import org.apache.ignite.internal.processors.rest.handlers.GridRestCommandHandlerAdapter;
import org.apache.ignite.internal.processors.rest.request.GridRestRequest;
import org.apache.ignite.internal.processors.rest.request.GridRestSupplyStatusRequest;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.processors.rest.GridRestCommand.SUPPLY_STATUS;

/**
 * Handler for {@link GridRestCommand#SUPPLY_STATUS}. Bare / {@code shutdown=false} is an auth-exempt
 * informational read (200, {@link SupplyStatusResponse} body). {@code shutdown=true} is the auth-required atomic
 * check-and-shutdown gate, evaluated in fixed order with NO shutdown on any 503 path: (1) GRACEFUL +
 * {@code force=false} + {@code uniqueDataHeld} → 503 (IMMEDIATE skips); (2) PME in flight → 503;
 * (3) demander → 503; (4) supplying → 503; (5) replication active → 503; else 200, then
 * {@code Ignition.stop(name, true)} on a dedicated daemon thread AFTER the response is flushed
 * (after-write hook). Idempotent via the {@code shutdownInitiated} CAS.
 */
public class GridSupplyStatusCommandHandler extends GridRestCommandHandlerAdapter {
    /** Supported commands. */
    private static final Collection<GridRestCommand> SUPPORTED_COMMANDS = U.sealList(SUPPLY_STATUS);

    /**
     * True once a successful {@code shutdown=true} call has fired the graceful shutdown.
     * {@code volatile} for cross-thread visibility; {@link #shutdownLock} guards the CAS.
     */
    private volatile boolean shutdownInitiated;

    /** Coarse guard for the {@code shutdownInitiated} check + body capture. */
    private final Object shutdownLock = new Object();

    /**
     * Replication-state provider — the GG-side override if registered, else the OSS stub
     * (returns {@code isReplicationActive() == false}). Resolved once at handler init.
     */
    private final IgniteReplicationStateProvider replicationProvider;

    /**
     * @param ctx Kernal context.
     */
    public GridSupplyStatusCommandHandler(GridKernalContext ctx) {
        super(ctx);

        IgniteReplicationStateProvider rp = ctx.plugins() != null
            ? ctx.plugins().createComponent(IgniteReplicationStateProvider.class)
            : null;

        this.replicationProvider = rp != null ? rp : new IgniteReplicationStateProvider();
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
            log.debug("supply-status command handler invoked: " + req);

        boolean shutdown = req instanceof GridRestSupplyStatusRequest
            && ((GridRestSupplyStatusRequest)req).shutdown();
        boolean force = req instanceof GridRestSupplyStatusRequest
            && ((GridRestSupplyStatusRequest)req).force();

        // Capture per-call state shared by all branches.
        List<String> supplyingGroups = supplyingCacheGroups();
        boolean supplying = !supplyingGroups.isEmpty();

        int activeClients = ctx.sqlListener() != null ? ctx.sqlListener().activeThinClientConnections() : 0;

        List<String> uniqueGroups = ShutdownPolicyHandler.uniqueDataCacheGroups(ctx);
        boolean uniqueDataHeld = !uniqueGroups.isEmpty();

        SupplyStatusResponse body = new SupplyStatusResponse(supplying, supplyingGroups, activeClients, uniqueDataHeld, uniqueGroups);

        // Informational variant — no gate, no side-effect.
        if (!shutdown)
            return new GridFinishedFuture<>(new GridRestResponse(body));

        // shutdown=true variant — atomic gate + trigger.
        synchronized (shutdownLock) {
            if (shutdownInitiated) {
                // Idempotent return — same body shape, NO re-trigger.
                if (log.isInfoEnabled())
                    log.info("cmd=supply-status&shutdown=true: idempotent (shutdown already initiated)");

                return new GridFinishedFuture<>(new GridRestResponse(body));
            }

            // Gate, evaluated in fixed order; no shutdown on any 503 path.

            // 1. Unique-data guard (GRACEFUL only; force bypasses).
            if (!force && uniqueDataHeld && ctx.config().getShutdownPolicy() == ShutdownPolicy.GRACEFUL)
                return new GridFinishedFuture<>(notSafe("unique data held on cache groups: " + uniqueGroups, body));

            // 2. PME in flight.
            if (!isPmeIdle())
                return new GridFinishedFuture<>(notSafe("pme in progress", body));

            // 3. Inbound rebalance — this node is a demander.
            List<String> demandingGroups = inboundRebalanceCacheGroups();

            if (!demandingGroups.isEmpty())
                return new GridFinishedFuture<>(notSafe("rebalancing: " + demandingGroups, body));

            // 4. Outbound supply.
            if (supplying)
                return new GridFinishedFuture<>(notSafe("supplying: " + supplyingGroups, body));

            // 5. Replication active.
            if (replicationProvider.isReplicationActive())
                return new GridFinishedFuture<>(notSafe("replication active", body));

            // Gate passed — set the flag inside the lock.
            shutdownInitiated = true;
        }

        // Gate passed. Defer the actual stop until AFTER the HTTP 200 has been flushed to the
        // caller: register an after-write action that the protocol layer runs once the response is
        // on the wire (see GridRestResponse.afterWrite + GridJettyRestHandler). This avoids the
        // connection-reset the caller would otherwise see if Ignition.stop tore the REST protocol down mid-flush.
        final String instanceName = ctx.igniteInstanceName();

        GridRestResponse resp = new GridRestResponse(body);

        resp.afterWrite(() -> {
            // Run Ignition.stop on a dedicated daemon thread — NOT a ctx.pools() executor
            // (node stop tears those pools down via U.shutdownNow during IgniteKernal.stop0,
            // which would interrupt the very thread running the stop) and NOT the protocol/Jetty
            // thread (stop can block). Mirrors the failure-handler self-stop pattern.
            Thread stopThread = new Thread(() -> {
                try {
                    if (log.isInfoEnabled())
                        log.info("cmd=supply-status&shutdown=true: triggering Ignition.stop(true) for " + instanceName);

                    // cancel=true cancels in-flight compute jobs on the way down (the node
                    // is already drained out of the Service plane, so no client traffic is
                    // lost). It does NOT skip the configured ShutdownPolicy — the GRACEFUL
                    // backup-wait runs inside IgniteKernal.stop0 regardless of this flag.
                    Ignition.stop(instanceName, true);
                }
                catch (Throwable t) {
                    U.error(log, "Graceful shutdown trigger failed for instance: " + instanceName, t);
                }
            }, "supply-status-shutdown-trigger");

            stopThread.setDaemon(true);
            stopThread.start();
        });

        return new GridFinishedFuture<>(resp);
    }

    /**
     * Builds a 503 (SERVICE_UNAVAILABLE) response carrying the informational body plus the
     * blocking reason in the {@code error} field.
     *
     * @param reason Blocking reason.
     * @param body Informational body.
     * @return 503 response.
     */
    private static GridRestResponse notSafe(String reason, SupplyStatusResponse body) {
        GridRestResponse resp = new GridRestResponse(GridRestResponse.SERVICE_UNAVAILABLE, reason);

        resp.setResponse(body);

        return resp;
    }

    /**
     * @return {@code true} if the last topology future is done (no PME in progress).
     *     Returns {@code true} defensively if the cache subsystem is not available yet.
     */
    private boolean isPmeIdle() {
        if (ctx.cache() == null || ctx.cache().context() == null || ctx.cache().context().exchange() == null)
            return true;

        GridDhtPartitionsExchangeFuture lastFut = ctx.cache().context().exchange().lastTopologyFuture();

        return lastFut == null || lastFut.isDone();
    }

    /**
     * @return Names of all non-system, non-local cache groups currently supplying
     *     ({@code GridDhtPreloader.supplier().isSupply()}); empty if none.
     */
    private List<String> supplyingCacheGroups() {
        return cacheGroupsMatching(p -> p instanceof GridDhtPreloader
            && ((GridDhtPreloader)p).supplier() != null && ((GridDhtPreloader)p).supplier().isSupply());
    }

    /**
     * @return Names of all non-system, non-local cache groups whose inbound rebalance (this node as
     *     a demander) has not completed — i.e. the node does not yet own a consistent copy of the
     *     partitions it is pulling; empty if none.
     */
    private List<String> inboundRebalanceCacheGroups() {
        return cacheGroupsMatching(p -> {
            IgniteInternalFuture<Boolean> rebFut = p.rebalanceFuture();

            return rebFut != null && !rebFut.isDone();
        });
    }

    /**
     * @param pred Predicate over a cache group's preloader.
     * @return Names of the non-system, non-local cache groups whose preloader matches {@code pred}; empty if none.
     */
    private List<String> cacheGroupsMatching(Predicate<GridCachePreloader> pred) {
        if (ctx.cache() == null)
            return Collections.emptyList();

        List<String> res = new ArrayList<>();

        for (CacheGroupContext grpCtx : ctx.cache().cacheGroups()) {
            if (grpCtx.isLocal() || grpCtx.systemCache())
                continue;

            GridCachePreloader preloader = grpCtx.preloader();

            if (preloader != null && pred.test(preloader))
                res.add(grpCtx.cacheOrGroupName());
        }

        return res;
    }
}
