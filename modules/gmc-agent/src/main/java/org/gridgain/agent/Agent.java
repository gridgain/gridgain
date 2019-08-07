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

package org.gridgain.agent;

import java.lang.reflect.Type;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.cluster.IgniteClusterImpl;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.eventstorage.GridEventStorageManager;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetaStorage;
import org.apache.ignite.internal.processors.gmc.ManagementConsoleProcessor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.gridgain.dto.ClusterInfo;
import org.gridgain.service.MetricsService;
import org.gridgain.service.TopologyService;
import org.gridgain.service.TracingService;
import org.springframework.messaging.simp.stomp.ConnectionLostException;
import org.springframework.messaging.simp.stomp.StompFrameHandler;
import org.springframework.messaging.simp.stomp.StompHeaders;
import org.springframework.messaging.simp.stomp.StompSession;
import org.springframework.messaging.simp.stomp.StompSessionHandlerAdapter;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.gridgain.agent.AgentUtils.getWsUri;
import static org.gridgain.agent.AgentUtils.monitoringUri;
import static org.gridgain.agent.StompDestinationsUtils.buildClusterAddDest;
import static org.gridgain.agent.StompDestinationsUtils.buildMetricsPullTopic;

/**
 * Management Agent.
 */
public class Agent extends ManagementConsoleProcessor {
    /** Gmc url meta storage prefix. */
    private static final String MANAGMENT_CFG_META_STORAGE_PREFIX = "gmc-cfg";

    /** Agent configuration. */
    private AgentConfiguration cfg;

    /** Websocket manager. */
    private WebSocketManager mgr;

    /** Topology service. */
    private TopologyService topSrvc;

    /** Tracing service. */
    private TracingService tracingSrvc;

    /** Metric service. */
    private MetricsService metricSrvc;

    /** Execute service. */
    private ScheduledExecutorService execSrvc = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors());

    /** Connection execute service. */
    private ExecutorService connExecSrvc = Executors.newSingleThreadExecutor();

    /** Meta storage. */
    private MetaStorage metaStorage;

    /**
     * @param ctx Kernal context.
     */
    public Agent(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart(boolean active) {
        GridEventStorageManager evtMgr = ctx.event();
        DiscoCache discoCache = ctx.discovery().discoCache();
        metaStorage = ctx.cache().context().database().metaStorage();

        startAgentOnCoordinator(discoCache);

        // Listener for coordinator changed.
        evtMgr.addDiscoveryEventListener(
            (evt, dc) -> execSrvc.submit(() -> startAgentOnCoordinator(dc)),
            EVT_NODE_FAILED, EVT_NODE_LEFT
        );
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        U.quietAndInfo(log, "Stopping GMC agent...");

        connExecSrvc.shutdown();
        U.closeQuiet(mgr);
        U.closeQuiet(topSrvc);
        execSrvc.shutdown();
    }

    /**
     * @param discoCache Discovery cache.
     */
    private void startAgentOnCoordinator(DiscoCache discoCache) {
        if (ctx.clientNode())
            return;

        List<ClusterNode> srvNodes = discoCache.serverNodes();
        ClusterNode crdNode = F.isEmpty(srvNodes) ? null : srvNodes.get(0);

        if (crdNode != null && crdNode.isLocal()) {
            log.info("Starting GMC agent on coordinator");

            cfg = loadAgentConfiguration();

            // TODO GG-21824 Use control script to change GMC url.
            String srvUri = System.getenv().get("GMC_ADDRESS");
            if (!F.isEmpty(srvUri)) {
                cfg.serverUri(srvUri);
                return;
            }

            mgr = new WebSocketManager(ctx, cfg);
            topSrvc = new TopologyService(ctx, mgr);
            tracingSrvc = new TracingService(ctx, mgr);
            metricSrvc = new MetricsService(ctx, mgr);

            tracingSrvc.registerHandler();

            connect();
        }
    }

    /**
     * Connect to backend.
     */
    private void connect() {
        connExecSrvc.submit(() -> {
            try {
                String srvUri = cfg.lastSuccessConnectedServerUri() != null
                        ? cfg.lastSuccessConnectedServerUri()
                        : cfg.serverUri();

                mgr.connect(getWsUri(srvUri), new AfterConnectedSessionHandler());

                if (cfg.lastSuccessConnectedServerUri() == null)
                    saveAgentConfiguration(cfg.lastSuccessConnectedServerUri(srvUri));
            }
            catch (InterruptedException | IgniteCheckedException ex) {
                log.error("Can't connect to GMC server!", ex);
            }
        });
    }

    // TODO: GG-21357 implement CLUSTER_ACTION.


    /**
     * @return Agent configuration.
     */
    private AgentConfiguration loadAgentConfiguration() {
        AgentConfiguration cfg = null;

        try {
            cfg = (AgentConfiguration) metaStorage.read(MANAGMENT_CFG_META_STORAGE_PREFIX);
        }
        catch (IgniteCheckedException e) {
            log.warning("Can't read agent configuration from meta storage!");
        }

        return cfg != null ? cfg : new AgentConfiguration();
    }

    /**
     * @param cfg Agent configuration.
     */
    private void saveAgentConfiguration(AgentConfiguration cfg) {
        try {
            metaStorage.write(MANAGMENT_CFG_META_STORAGE_PREFIX, cfg);
        }
        catch (IgniteCheckedException e) {
            log.warning("Can't save agent configuration to meta storage!");
        }
    }

    /**
     * Session handler for sending cluster info to backend.
     */
    private class AfterConnectedSessionHandler extends StompSessionHandlerAdapter {
        /** {@inheritDoc} */
        @Override public void afterConnected(StompSession stompSes, StompHeaders stompHeaders) {
            IgniteClusterImpl cluster = ctx.cluster().get();

            U.quietAndInfo(log, "");
            U.quietAndInfo(log, "Found GMC server that can be used to monitor your cluster: " + cfg.serverUri());

            U.quietAndInfo(log, "");
            U.quietAndInfo(log, "Open link in browser to monitor your cluster in GMC: " +
                    monitoringUri(cfg.serverUri(), cluster.id()));

            U.quietAndInfo(log, "If you already using GMC, you can add cluster manually by it's ID: " + cluster.id());

            stompSes.send(buildClusterAddDest(), new ClusterInfo(cluster.id(), cluster.tag()));

            topSrvc.sendTopologyUpdate();
            topSrvc.sendClusterActiveState();
            topSrvc.sendBaseline();

            tracingSrvc.flushBuffer();

            stompSes.subscribe(buildMetricsPullTopic(), new StompFrameHandler() {
                @Override public Type getPayloadType(StompHeaders headers) {
                    return String.class;
                }

                @Override public void handleFrame(StompHeaders headers, Object payload) {
                    metricSrvc.broadcastPullMetrics();
                }
            });
        }

        /** {@inheritDoc} */
        @Override public void handleTransportError(StompSession stompSes, Throwable e) {
            if (e instanceof ConnectionLostException) {
                log.error("Lost websocket connection with server: " + cfg.serverUri());

                try {
                    mgr.reconnect();
                }
                catch (InterruptedException | IgniteCheckedException ex) {
                    log.error("Can't recconect to GMC server!", ex);
                }
            }
        }
    }
}
