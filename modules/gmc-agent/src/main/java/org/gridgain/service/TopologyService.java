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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.cluster.IgniteClusterEx;
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
    /** TODO GG-21449 this code emulates EVT_BASELINE_CHANGED */
    private volatile Set<String> curBaseline;

    /** Context. */
    private GridKernalContext ctx;

    /** Manager. */
    private WebSocketManager mgr;

    /** Logger. */
    private IgniteLogger log;

    /** Executor service. */
    private ScheduledExecutorService execSrvc = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors());

    /**
     * @param ctx Context.
     * @param mgr Manager.
     */
    public TopologyService(GridKernalContext ctx, WebSocketManager mgr) {
        this.ctx = ctx;
        this.mgr = mgr;

        GridEventStorageManager evtMgr = ctx.event();

        log = ctx.log(TopologyService.class);

        // Listener for topology changes.
        evtMgr.addDiscoveryEventListener(
            (evt, dc) -> execSrvc.submit(this::sendTopologyUpdate),
            EVT_NODE_JOINED, EVT_NODE_FAILED, EVT_NODE_LEFT
        );

        // Listen for activation/deactivation.
        evtMgr.addLocalEventListener(
            (evt) -> execSrvc.submit(this::sendClusterActiveState),
            EVT_CLUSTER_ACTIVATED, EVT_CLUSTER_DEACTIVATED
        );

        // TODO GG-21449 this code emulates EVT_BASELINE_CHANGED
        execSrvc.scheduleWithFixedDelay(() -> {
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

    /** {@inheritDoc} */
    @Override public void close() {
        execSrvc.shutdown();
    }

    /**
     * Send full topology to GMC.
     */
    public void sendTopologyUpdate() {
        // TODO GG-22191 change on debug level.
        log.info("Sending full topology to GMC");

        IgniteClusterEx cluster = ctx.grid().cluster();
        mgr.getSession().send(
            buildClusterTopologyDest(cluster.id()),
            TopologySnapshot.topology(cluster.topologyVersion(), cluster.nodes())
        );
    }

    /**
     * Send cluster active state to GMC.
     */
    public void sendClusterActiveState() {
        // TODO GG-22191 change on debug level.
        log.info("Sending cluster active state to GMC");

        IgniteClusterEx cluster = ctx.grid().cluster();
        mgr.getSession().send(buildClusterActiveStateDest(cluster.id()), cluster.active());
    }

    /**
     * Send cluster baseline to GMC.
     */
    public void sendBaseline() {
        // TODO GG-22191 change on debug level.
        log.info("Sending cluster baseline to GMC");

        IgniteClusterEx cluster = ctx.grid().cluster();

        mgr.getSession().send(
            buildBaselineTopologyDest(cluster.id()),
            TopologySnapshot.baseline(cluster.topologyVersion(), cluster.currentBaselineTopology())
        );
    }
}
