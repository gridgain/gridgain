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
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCachePartitionExchangeManager;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cluster.DiscoveryDataClusterState;
import org.apache.ignite.internal.processors.rest.GridRestCommand;
import org.apache.ignite.internal.processors.rest.GridRestResponse;
import org.apache.ignite.internal.processors.rest.handlers.GridRestCommandHandlerAdapter;
import org.apache.ignite.internal.processors.rest.request.GridRestProbeRequest;
import org.apache.ignite.internal.processors.rest.request.GridRestRequest;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.rest.GridRestCommand.NODE_STATE_BEFORE_START;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.PROBE;

/**
 * Handler for {@link GridRestCommand#PROBE}, dispatching on the {@code kind} query parameter
 * for Kubernetes probes:
 * <ul>
 *     <li>bare {@code cmd=probe} (no {@code kind}) and {@code kind=liveness} — kernel-started
 *         check only: 200 {@code "grid has started"} / 503 {@code "grid has not started"}.
 *         Never blocks on rebalance, PME, cluster state, or maintenance (a rebalancing node is
 *         alive). Bare is preserved for backward compatibility; {@code kind=liveness} backs the
 *         {@code livenessProbe}.</li>
 *     <li>{@code kind=readiness} — the readiness/startup gate; see {@link #readinessLadder()}.
 *         Backs the {@code readinessProbe} and {@code startupProbe}.</li>
 * </ul>
 *
 * <p>{@code kind=readiness} reports a (re)joining node not-ready until it has finished pulling
 * its partitions (inbound rebalance), so a rolling restart does not bounce the next pod (a
 * supplier) mid-rebalance and lose partitions. It gates on this node holding no MOVING partitions
 * (full or historical rebalance), latched once the node's initial rebalance completes so later
 * topology rebalances don't flap an already-loaded node out of the Service endpoints
 * (see {@link #isInitialRebalanceComplete()}). An unknown {@code kind} returns
 * {@link GridRestResponse#STATUS_FAILED}; the {@code NODE_STATE_BEFORE_START} branch is unchanged.</p>
 */
public class GridProbeCommandHandler extends GridRestCommandHandlerAdapter {
    /**
     * Sub-command of {@code cmd=probe} expressed via the {@code kind} query
     * parameter. {@code null} {@code kind} routes to the bare-GET BC path.
     */
    public enum Kind {
        /** Liveness check — kernel-started gate ONLY. */
        LIVENESS("liveness"),

        /** Readiness/startup check — latched initial-rebalance gate. */
        READINESS("readiness");

        /** Wire-level key as it appears in the {@code kind} query parameter. */
        private final String key;

        /**
         * @param key Wire-level key.
         */
        Kind(String key) {
            this.key = key;
        }

        /**
         * @return Wire-level key.
         */
        public String key() {
            return key;
        }

        /**
         * Case-insensitive lookup of a {@link Kind} by its wire-level key.
         *
         * @param key Wire-level key from the request (may be {@code null}).
         * @return Matching kind, or {@code null} if {@code key} is {@code null}
         *     or doesn't match any defined kind.
         */
        @Nullable public static Kind of(@Nullable String key) {
            if (key == null)
                return null;

            for (Kind k : values())
                if (k.key.equalsIgnoreCase(key))
                    return k;

            return null;
        }
    }

    /**
     * Set-once readiness latch. Once the node observes a completed initial
     * rebalance while the cluster is active, readiness stays {@code true} for the
     * lifetime of this JVM regardless of later topology-change rebalances.
     * {@code volatile} for visibility across REST worker threads.
     */
    private volatile boolean rebalanceLatched;

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

                String rawKind = req instanceof GridRestProbeRequest ? ((GridRestProbeRequest)req).kind() : null;
                Kind kind = Kind.of(rawKind);

                if (rawKind != null && kind == null) {
                    return new GridFinishedFuture<>(new GridRestResponse(GridRestResponse.STATUS_FAILED,
                        "Unknown probe kind: " + rawKind + " (expected one of: liveness, readiness)"));
                }

                if (kind == Kind.READINESS)
                    return readinessLadder();

                // Liveness probe is the same as the bare GET which is kept for compatibility.
                return new GridFinishedFuture<>(IgnitionEx.hasKernalStarted(ctx.igniteInstanceName())
                    ? new GridRestResponse("grid has started")
                    : new GridRestResponse(GridRestResponse.SERVICE_UNAVAILABLE, "grid has not started"));
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
     * k8s readiness/startup ladder. Evaluated in fixed order; the first matching
     * condition determines the response.
     *
     * @return Future carrying 200 {@code "ready"} or a 503 with the specific
     *     blocker as reason.
     */
    private IgniteInternalFuture<GridRestResponse> readinessLadder() {
        // 1. JVM kernel started.
        if (!IgnitionEx.hasKernalStarted(ctx.igniteInstanceName()))
            return notReady("kernel not started");

        // 2. Maintenance mode — alive but must not receive traffic (liveness stays 200).
        if (isMaintenanceMode())
            return notReady("maintenance mode");

        // 3. Inactive cluster — nothing to rebalance; report ready so a k8s rolling
        //    update can proceed (the rebalanced flag is always false while INACTIVE,
        //    so this short-circuit MUST precede the rebalance check).
        if (isClusterInactive())
            return new GridFinishedFuture<>(new GridRestResponse("ready"));

        // 4 & 5. Initial rebalance complete (latched) -> ready; else still pulling.
        if (isInitialRebalanceComplete())
            return new GridFinishedFuture<>(new GridRestResponse("ready"));

        return notReady("rebalance in progress");
    }

    /**
     * Latched "this node has completed its initial post-join rebalance" check.
     * Once {@code true} while the cluster is active, stays {@code true} for the
     * JVM lifetime (later topology-change rebalances do not flip it back).
     *
     * @return {@code true} if readiness should report ready on the rebalance rung.
     */
    private boolean isInitialRebalanceComplete() {
        if (rebalanceLatched)
            return true;

        if (isNodeRebalanceComplete()) {
            rebalanceLatched = true;

            return true;
        }

        return false;
    }

    /**
     * @param reason 503 reason.
     * @return Future carrying a SERVICE_UNAVAILABLE response.
     */
    private static IgniteInternalFuture<GridRestResponse> notReady(String reason) {
        return new GridFinishedFuture<>(new GridRestResponse(GridRestResponse.SERVICE_UNAVAILABLE, reason));
    }

    /**
     * @return {@code true} if the cluster is INACTIVE. Non-blocking, null-safe.
     */
    private boolean isClusterInactive() {
        DiscoveryDataClusterState st = ctx.state() == null ? null : ctx.state().clusterState();

        return st != null && st.state() == ClusterState.INACTIVE;
    }

    /**
     * @return {@code true} if maintenance mode is active. Non-blocking. The maintenance
     *     registry is created and started before the REST processor, so it is never
     *     {@code null} by the time a probe request is handled.
     */
    private boolean isMaintenanceMode() {
        return ctx.maintenanceRegistry().isMaintenanceMode();
    }

    /**
     * Instantaneous "this node has finished its (initial) inbound rebalance" predicate - the value
     * the readiness latch captures. The node is settled once none of its local partitions in any
     * affinity cache group is {@link GridDhtPartitionState#MOVING} (full and historical/WAL rebalance
     * both hold a partition MOVING until it is pulled). This is a per-node signal: a node reports
     * ready as soon as its own partitions are loaded, regardless of other (re)joining nodes still
     * rebalancing.
     *
     * <p>Partition state is read rather than the per-group rebalance future because MOVING is set
     * in-band during the exchange (before {@link GridCachePartitionExchangeManager#readyAffinityVersion()}
     * advances), whereas the rebalance future is (re)created only afterwards in the exchange worker -
     * so reading it in that window could see a stale done future and report ready prematurely.</p>
     *
     * <p>Non-blocking, null-safe; {@link #isInitialPmeComplete()} guards the pre-affinity window so
     * readiness stays 503 until the node is genuinely settled.</p>
     *
     * @return {@code true} if the node's initial PME is complete and it holds no MOVING partition.
     */
    private boolean isNodeRebalanceComplete() {
        if (ctx.clientNode())
            return true; // Clients/daemons never own partitions and never rebalance.

        if (!isInitialPmeComplete())
            return false;

        for (CacheGroupContext grp : ctx.cache().cacheGroups()) {
            if (!grp.affinityNode())
                continue; // Node holds no data for this group.

            for (GridDhtLocalPartition part : grp.topology().localPartitions()) {
                if (part.state() == GridDhtPartitionState.MOVING)
                    return false; // Still demanding this partition.
            }
        }

        return true;
    }

    /**
     * Initial/local-join PME finished and affinity is ready (server nodes only -
     * the sole caller short-circuits client/daemon nodes). Non-blocking, null-safe;
     * returns {@code false} while the cache subsystem is still initializing or no
     * exchange has finished yet.
     *
     * @return {@code true} if at least one exchange has finished and affinity is ready.
     */
    private boolean isInitialPmeComplete() {
        if (ctx.cache() == null || ctx.cache().context() == null || ctx.cache().context().exchange() == null)
            return false;

        GridCachePartitionExchangeManager<?, ?> exch = ctx.cache().context().exchange();

        // readyAffinityVersion() is never null; NONE (0,0) means no exchange has completed yet.
        return exch.readyAffinityVersion().compareTo(AffinityTopologyVersion.NONE) > 0;
    }
}
