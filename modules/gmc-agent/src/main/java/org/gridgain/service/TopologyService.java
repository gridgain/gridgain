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

package org.gridgain.service;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.cluster.IgniteClusterEx;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.eventstorage.GridEventStorageManager;
import org.gridgain.agent.WebSocketManager;
import org.gridgain.dto.topology.TopologySnapshot;

import static org.apache.ignite.events.EventType.EVT_CLUSTER_ACTIVATED;
import static org.apache.ignite.events.EventType.EVT_CLUSTER_DEACTIVATED;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.gridgain.agent.StompDestinationsUtils.buildBaselineTopologyDest;
import static org.gridgain.agent.StompDestinationsUtils.buildClusterActiveStateDest;
import static org.gridgain.agent.StompDestinationsUtils.buildClusterTopologyDest;

/**
 * Topology service.
 */
public class TopologyService implements AutoCloseable {
    /** Discovery event on restart agent. */
    private static final int[] EVTS_DISCOVERY = new int[] {EVT_NODE_JOINED, EVT_NODE_FAILED, EVT_NODE_LEFT};

    /** Discovery event on restart agent. */
    private static final int[] EVTS_ACTIVATION = new int[] {EVT_CLUSTER_ACTIVATED, EVT_CLUSTER_DEACTIVATED};

    /** TODO GG-21449 this code emulates EVT_BASELINE_CHANGED */
    private volatile Set<String> curBaseline;

    /** Context. */
    private GridKernalContext ctx;

    /** Manager. */
    private WebSocketManager mgr;

    /** Logger. */
    private IgniteLogger log;

    /** Cluster ID. */
    private UUID clusterId;

    /** Executor service. */
    private ScheduledExecutorService baselineExecSrvc = Executors.newScheduledThreadPool(1);

    /**
     * @param ctx Context.
     * @param mgr Manager.
     */
    public TopologyService(GridKernalContext ctx, WebSocketManager mgr) {
        this.ctx = ctx;
        this.mgr = mgr;

        clusterId = ctx.grid().cluster().id();

        GridEventStorageManager evtMgr = ctx.event();

        log = ctx.log(TopologyService.class);

        // Listener for topology changes.
        evtMgr.addDiscoveryEventListener(this::sendTopologyUpdate, EVTS_DISCOVERY);

        // Listen for activation/deactivation.
        evtMgr.addLocalEventListener(this::sendClusterActiveState, EVTS_ACTIVATION);

        // TODO GG-21449 this code emulates EVT_BASELINE_CHANGED
        baselineExecSrvc.scheduleWithFixedDelay(() -> {
            Set<String> baseline = ctx
                .grid()
                .cluster()
                .currentBaselineTopology()
                .stream()
                .map(BaselineNode::consistentId)
                .map(Object::toString)
                .collect(Collectors.toSet());

            if (curBaseline == null)
                curBaseline = baseline;
            else if (!curBaseline.equals(baseline)) {
                curBaseline = baseline;

                sendBaseline();
            }
        }, 2, 5, TimeUnit.SECONDS);
    }

    /**
     * Send full topology to GMC.
     */
    void sendTopologyUpdate(DiscoveryEvent evt, DiscoCache discoCache) {
        // TODO GG-22191 change on debug level.
        log.info("Sending full topology to GMC");

        mgr.send(
            buildClusterTopologyDest(clusterId),
            TopologySnapshot.topology(ctx.grid().cluster().topologyVersion(), ctx.grid().cluster().nodes())
        );
    }

    /**
     * Send cluster active state to GMC.
     */
    void sendClusterActiveState(Event evt) {
        // TODO GG-22191 change on debug level.
        log.info("Sending cluster active state to GMC");

        mgr.send(buildClusterActiveStateDest(clusterId), ctx.grid().cluster().active());
    }

    /**
     * Send cluster baseline to GMC.
     */
    void sendBaseline() {
        // TODO GG-22191 change on debug level.
        log.info("Sending cluster baseline to GMC");

        IgniteClusterEx cluster = ctx.grid().cluster();

        mgr.send(
            buildBaselineTopologyDest(cluster.id()),
            TopologySnapshot.baseline(cluster.topologyVersion(), cluster.currentBaselineTopology())
        );
    }

    /**
     * Start topology service.
     */
    public void sendInitialState() {
        sendTopologyUpdate(null, ctx.discovery().discoCache());
        sendClusterActiveState(null);
        sendBaseline();
    }

    /** {@inheritDoc} */
    @Override public void close() {
        ctx.event().removeDiscoveryEventListener(this::sendTopologyUpdate, EVTS_DISCOVERY);
        ctx.event().removeLocalEventListener(this::sendClusterActiveState, EVTS_ACTIVATION);

        baselineExecSrvc.shutdown();
    }
}
