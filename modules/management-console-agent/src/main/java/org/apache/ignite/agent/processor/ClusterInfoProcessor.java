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

package org.apache.ignite.agent.processor;

import org.apache.ignite.agent.dto.cluster.BaselineInfo;
import org.apache.ignite.agent.dto.cluster.ClusterInfo;
import org.apache.ignite.agent.dto.topology.TopologySnapshot;
import org.apache.ignite.agent.ws.WebSocketManager;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.cluster.IgniteClusterEx;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.eventstorage.GridEventStorageManager;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.util.typedef.internal.CU;

import static org.apache.ignite.agent.StompDestinationsUtils.buildClusterDest;
import static org.apache.ignite.agent.StompDestinationsUtils.buildClusterTopologyDest;
import static org.apache.ignite.agent.utils.AgentUtils.getClusterFeatures;
import static org.apache.ignite.events.EventType.EVTS_CLUSTER_ACTIVATION;
import static org.apache.ignite.events.EventType.EVT_BASELINE_AUTO_ADJUST_AWAITING_TIME_CHANGED;
import static org.apache.ignite.events.EventType.EVT_BASELINE_AUTO_ADJUST_ENABLED_CHANGED;
import static org.apache.ignite.events.EventType.EVT_BASELINE_CHANGED;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;

/**
 * Cluster processor.
 */
public class ClusterInfoProcessor extends GridProcessorAdapter {
    /** Discovery event on restart agent. */
    private static final int[] EVTS_DISCOVERY = new int[] {EVT_NODE_JOINED, EVT_NODE_FAILED, EVT_NODE_LEFT};

    /** Baseline events. */
    private static final int[] EVTS_BASELINE = new int[] {
        EVT_BASELINE_CHANGED,
        EVT_BASELINE_AUTO_ADJUST_ENABLED_CHANGED,
        EVT_BASELINE_AUTO_ADJUST_AWAITING_TIME_CHANGED
    };

    /** Baseline params changed events. */
    private static final int[] EVTS_BASELINE_PARAMS_CHANGED = new int[] {
        EVT_BASELINE_AUTO_ADJUST_ENABLED_CHANGED,
        EVT_BASELINE_AUTO_ADJUST_AWAITING_TIME_CHANGED
    };

    /** Cluster. */
    protected IgniteClusterEx cluster;

    /** Manager. */
    private WebSocketManager mgr;

    /** Send full topology to Control Center on baseline change. */
    private GridLocalEventListener sndTopUpdateLsnr = (event -> sendTopologyUpdate(null, ctx.discovery().discoCache()));

    /**
     * @param ctx Context.
     * @param mgr Manager.
     */
    public ClusterInfoProcessor(GridKernalContext ctx, WebSocketManager mgr) {
        super(ctx);
        this.mgr = mgr;
        cluster = ctx.grid().cluster();

        GridEventStorageManager evtMgr = ctx.event();

        // Listener for topology changes.
        evtMgr.addDiscoveryEventListener(this::sendTopologyUpdate, EVTS_DISCOVERY);

        // Listen for activation/deactivation.
        evtMgr.enableEvents(EVTS_CLUSTER_ACTIVATION);
        evtMgr.addLocalEventListener(this::sendClusterInfo, EVTS_CLUSTER_ACTIVATION);

        evtMgr.enableEvents(EVTS_BASELINE);
        evtMgr.addLocalEventListener(this::sendClusterInfo, EVTS_BASELINE_PARAMS_CHANGED);
        evtMgr.addLocalEventListener(sndTopUpdateLsnr, EVT_BASELINE_CHANGED);
    }

    /**
     * Send initial states.
     */
    public void sendInitialState() {
        sendClusterInfo(null);
        sendTopologyUpdate(null, ctx.discovery().discoCache());
    }

    /**
     * Send full topology to Control Center.
     *
     * @param evt Discovery event.
     * @param discoCache Disco cache.
     */
    void sendTopologyUpdate(DiscoveryEvent evt, DiscoCache discoCache) {
        if (log.isDebugEnabled())
            log.debug("Sending full topology to Control Center");

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
        if (log.isDebugEnabled())
            log.debug("Sending cluster info to Control Center");

        ClusterInfo clusterInfo = createClusterInfo();

        populateClusterInfo(clusterInfo);

        mgr.send(buildClusterDest(cluster.id()), clusterInfo);
    }

    /**
     * @return Create cluster info.
     */
    protected ClusterInfo createClusterInfo() {
        return new ClusterInfo(cluster.id(), cluster.tag());
    }

    /**
     * @param clusterInfo Cluster info to populate with data.
     */
    protected void populateClusterInfo(ClusterInfo clusterInfo) {
        clusterInfo
            .setActive(cluster.active())
            .setPersistenceEnabled(CU.isPersistenceEnabled(ctx.config()))
            .setBaselineParameters(
                new BaselineInfo(
                    cluster.isBaselineAutoAdjustEnabled(),
                    cluster.baselineAutoAdjustTimeout()
                )
            )
            .setSecure(ctx.authentication().enabled() || ctx.security().enabled())
            .setFeatures(getClusterFeatures(ctx, ctx.cluster().get().nodes()));

    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) {
        ctx.event().removeDiscoveryEventListener(this::sendTopologyUpdate, EVTS_DISCOVERY);

        ctx.event().removeLocalEventListener(this::sendClusterInfo, EVTS_CLUSTER_ACTIVATION);

        ctx.event().removeLocalEventListener(this::sendClusterInfo, EVTS_BASELINE_PARAMS_CHANGED);
        ctx.event().removeLocalEventListener(sndTopUpdateLsnr, EVT_BASELINE_CHANGED);
    }
}
