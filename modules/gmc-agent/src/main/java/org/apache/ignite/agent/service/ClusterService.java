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

package org.apache.ignite.agent.service;

import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.agent.dto.cluster.BaselineInfo;
import org.apache.ignite.agent.dto.cluster.ClusterInfo;
import org.apache.ignite.agent.dto.topology.TopologySnapshot;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.cluster.IgniteClusterEx;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.eventstorage.GridEventStorageManager;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.agent.utils.AgentUtils;
import org.apache.ignite.agent.WebSocketManager;

import static org.apache.ignite.events.EventType.EVT_CLUSTER_ACTIVATED;
import static org.apache.ignite.events.EventType.EVT_CLUSTER_DEACTIVATED;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.agent.StompDestinationsUtils.buildClusterDest;
import static org.apache.ignite.agent.StompDestinationsUtils.buildClusterTopologyDest;

/**
 * Topology service.
 */
public class ClusterService implements AutoCloseable {
    /** Discovery event on restart agent. */
    private static final int[] EVTS_DISCOVERY = new int[] {EVT_NODE_JOINED, EVT_NODE_FAILED, EVT_NODE_LEFT};

    /** Discovery event on restart agent. */
    private static final int[] EVTS_ACTIVATION = new int[] {EVT_CLUSTER_ACTIVATED, EVT_CLUSTER_DEACTIVATED};

    /** TODO GG-21449 this code emulates EVT_BASELINE_CHANGED */
    private volatile Set<String> curBaseline;

    /** Baseline parameters. */
    private volatile BaselineInfo curBaselineParameters;

    /** Context. */
    private GridKernalContext ctx;

    /** Cluster. */
    private IgniteClusterEx cluster;

    /** Manager. */
    private WebSocketManager mgr;

    /** Logger. */
    private IgniteLogger log;

    /** Executor service. */
    private ScheduledExecutorService baselineExecSrvc = Executors.newScheduledThreadPool(1);

    /**
     * @param ctx Context.
     * @param mgr Manager.
     */
    public ClusterService(GridKernalContext ctx, WebSocketManager mgr) {
        this.ctx = ctx;
        this.mgr = mgr;
        cluster = ctx.grid().cluster();
        log = ctx.log(ClusterService.class);

        GridEventStorageManager evtMgr = ctx.event();

        // Listener for topology changes.
        evtMgr.addDiscoveryEventListener(this::sendTopologyUpdate, EVTS_DISCOVERY);

        // Listen for activation/deactivation.
        evtMgr.addLocalEventListener(this::sendClusterInfo, EVTS_ACTIVATION);

        // TODO GG-21449 this code emulates EVT_BASELINE_CHANGED and EVT_BASELINE_AUTO_*
        baselineExecSrvc.scheduleWithFixedDelay(() -> {
            Stream<BaselineNode> stream = AgentUtils.fromNullableCollection(ctx.grid().cluster().currentBaselineTopology());

            Set<String> baseline = stream
                .map(BaselineNode::consistentId)
                .map(Object::toString)
                .collect(Collectors.toSet());

            if (curBaseline == null)
                curBaseline = baseline;
            else if (!curBaseline.equals(baseline)) {
                curBaseline = baseline;

                sendTopologyUpdate(null, ctx.discovery().discoCache());
            }

            BaselineInfo baselineParameters = new BaselineInfo(
                cluster.isBaselineAutoAdjustEnabled(),
                cluster.baselineAutoAdjustTimeout()
            );

            if (curBaselineParameters == null)
                curBaselineParameters = baselineParameters;
            else if (!curBaselineParameters.equals(baselineParameters)) {
                curBaselineParameters = baselineParameters;

                sendClusterInfo(null);
            }
        }, 2, 5, TimeUnit.SECONDS);
    }

    /**
     * Send initial states.
     */
    public void sendInitialState() {
        sendClusterInfo(null);
        sendTopologyUpdate(null, ctx.discovery().discoCache());
    }

    /**
     * Send full topology to GMC.
     */
    void sendTopologyUpdate(DiscoveryEvent evt, DiscoCache discoCache) {
        // TODO GG-22191 change on debug level.
        log.info("Sending full topology to GMC");

        Object crdId = cluster.localNode().consistentId();
        mgr.send(
            buildClusterTopologyDest(cluster.id()),
            TopologySnapshot.topology(cluster.topologyVersion(), crdId, cluster.nodes(), cluster.currentBaselineTopology())
        );

        if (evt != null)
            sendClusterInfo(null);
    }

    /**
     * Send cluster info.
     */
    void sendClusterInfo(Event evt) {
        // TODO GG-22191 change on debug level.
        log.info("Sending cluster info to GMC");

        ClusterInfo clusterInfo = new ClusterInfo(cluster.id(), cluster.tag())
            .setActive(cluster.active())
            .setPersistenceEnabled(CU.isPersistenceEnabled(ctx.config()))
            .setBaselineParameters(
                new BaselineInfo(
                    cluster.isBaselineAutoAdjustEnabled(),
                    cluster.baselineAutoAdjustTimeout()
                )
            )
            .setFeatures(AgentUtils.getClusterFeatures(ctx, ctx.cluster().get().nodes()));

        mgr.send(buildClusterDest(cluster.id()), clusterInfo);
    }

    /** {@inheritDoc} */
    @Override public void close() {
        ctx.event().removeDiscoveryEventListener(this::sendTopologyUpdate, EVTS_DISCOVERY);
        ctx.event().removeLocalEventListener(this::sendClusterInfo, EVTS_ACTIVATION);

        baselineExecSrvc.shutdown();
    }
}
