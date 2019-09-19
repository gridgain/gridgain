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
import java.net.ConnectException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.cluster.IgniteClusterImpl;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetaStorage;
import org.apache.ignite.internal.processors.gmc.ManagementConfiguration;
import org.apache.ignite.internal.processors.gmc.ManagementConsoleProcessor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.eclipse.jetty.io.EofException;
import org.eclipse.jetty.websocket.api.UpgradeException;
import org.gridgain.dto.action.Request;
import org.gridgain.dto.ClusterInfo;
import org.gridgain.service.ActionService;
import org.gridgain.service.config.NodeConfigurationExporter;
import org.gridgain.service.MetricsService;
import org.gridgain.service.TopologyService;
import org.gridgain.service.config.NodeConfigurationService;
import org.gridgain.service.tracing.TracingService;
import org.gridgain.service.tracing.GmcSpanExporter;
import org.springframework.messaging.simp.stomp.ConnectionLostException;
import org.springframework.messaging.simp.stomp.StompFrameHandler;
import org.springframework.messaging.simp.stomp.StompHeaders;
import org.springframework.messaging.simp.stomp.StompSession;
import org.springframework.messaging.simp.stomp.StompSessionHandlerAdapter;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.gridgain.agent.AgentUtils.monitoringUri;
import static org.gridgain.agent.AgentUtils.toWsUri;
import static org.gridgain.agent.StompDestinationsUtils.buildActionRequestTopic;
import static org.gridgain.agent.StompDestinationsUtils.buildClusterAddDest;
import static org.gridgain.agent.StompDestinationsUtils.buildMetricsPullTopic;

/**
 * Management Agent.
 */
public class Agent extends ManagementConsoleProcessor {
    /** GMC configuration meta storage prefix. */
    private static final String MANAGEMENT_CFG_META_STORAGE_PREFIX = "gmc-cfg";

    /** Discovery event on restart agent. */
    private static final int[] EVTS_DISCOVERY = new int[] {EVT_NODE_FAILED, EVT_NODE_LEFT};

    /** Websocket manager. */
    private WebSocketManager mgr;

    /** Topology service. */
    private TopologyService topSrvc;

    /** Tracing service. */
    private TracingService tracingSrvc;

    /** Span exporter. */
    private GmcSpanExporter spanExporter;

    /** Node configuration exporter. */
    private NodeConfigurationExporter nodeConfigurationExporter;

    /** Metric service. */
    private MetricsService metricSrvc;

    /** Action service. */
    private ActionService actionSrvc;

    /** Node configuration service. */
    private NodeConfigurationService nodeConfigurationSrvc;

    /** Execute service. */
    private ThreadPoolExecutor execSrvc;

    /** Meta storage. */
    private MetaStorage metaStorage;

    /** Active server uri. */
    private String curSrvUri;

    /** If first connection error after successful connection. */
    private AtomicBoolean disconnected = new AtomicBoolean();

    /**
     * @param ctx Kernal context.
     */
    public Agent(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart(boolean active) {
        spanExporter = new GmcSpanExporter(ctx);
        nodeConfigurationExporter = new NodeConfigurationExporter(ctx);
        metaStorage = ctx.cache().context().database().metaStorage();

        launchAgentListener(null, ctx.discovery().discoCache());

        // Listener for coordinator changed.
        ctx.event().addDiscoveryEventListener(this::launchAgentListener, EVTS_DISCOVERY);
        nodeConfigurationExporter.export();
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        log.info("Stopping GMC agent.");

        ctx.event().removeDiscoveryEventListener(this::launchAgentListener, EVTS_DISCOVERY);

        U.shutdownNow(this.getClass(), execSrvc, log);

        U.closeQuiet(actionSrvc);
        U.closeQuiet(nodeConfigurationExporter);
        U.closeQuiet(metricSrvc);
        U.closeQuiet(spanExporter);
        U.closeQuiet(tracingSrvc);
        U.closeQuiet(topSrvc);
        U.closeQuiet(mgr);

        disconnected.set(false);

        U.quietAndInfo(log, "GMC agent stopped.");
    }

    /** {@inheritDoc} */
    @Override public void configuration(ManagementConfiguration cfg) {
        if (cfg.hasServerUris()) {
            writeToMetaStorage(cfg);

            super.configuration(cfg);
        }

        ManagementConfiguration oldCfg = configuration();

        if (oldCfg.isEnable() != cfg.isEnable())
            toggleAgentState(cfg.isEnable());
        else
            submitConnectTask();
    }

    /**
     * Toggle agent state.
     *
     * @param enable Enable flag.
     */
    private void toggleAgentState(boolean enable) {
        ManagementConfiguration cfg = configuration()
            .setEnable(enable);

        super.configuration(cfg);

        writeToMetaStorage(cfg);

        if (enable)
            onKernalStart(ctx.cluster().get().active());
        else
            onKernalStop(true);
    }
    
    /**
     * Start agent on local node if this is coordinator node.
     */
    private void launchAgentListener(DiscoveryEvent evt, DiscoCache discoCache) {
        if (ctx.clientNode())
            return;

        ClusterNode crdNode = F.first(discoCache.serverNodes());

        if (crdNode != null && crdNode.isLocal()) {
            cfg = readFromMetaStorage();

            connect();
        }
    }

    /**
     * @param uris GMC Server URIs.
     */
    private String nextUri(List<String> uris, String cur) {
        int idx = uris.indexOf(cur);

        return uris.get((idx + 1) % uris.size());
    }

    // TODO: GG-21357 implement CLUSTER_ACTION.

    /**
     * Connect to backend in same thread.
     */
    private void connect0() {
        curSrvUri = nextUri(cfg.getServerUris(), curSrvUri);

        try {
            mgr.connect(toWsUri(curSrvUri), cfg, new AfterConnectedSessionHandler());

            disconnected.set(false);
        }
        catch (InterruptedException ignored) {
            // No-op.
        }
        catch (TimeoutException ignored) {
            connect0();
        }
        catch (ExecutionException e) {
            if (X.hasCause(e, ConnectException.class, UpgradeException.class, EofException.class)) {
                if (disconnected.compareAndSet(false, true))
                    log.error("Failed to establish websocket connection with GMC server: " + curSrvUri);

                connect0();
            }
            else
                log.error("Failed to establish websocket connection with GMC server: " + curSrvUri, e);
        }
        catch (Exception e) {
            log.error("Failed to establish websocket connection with GMC server: " + curSrvUri, e);
        }
    }

    /**
     * Connect to backend.
     */
    private void connect() {
        log.info("Starting GMC agent on coordinator");

        U.closeQuiet(actionSrvc);
        U.closeQuiet(nodeConfigurationSrvc);
        U.closeQuiet(metricSrvc);
        U.closeQuiet(tracingSrvc);
        U.closeQuiet(topSrvc);
        U.closeQuiet(mgr);

        if (!cfg.isEnable()) {
            log.info("Skip start GMC agent on coordinator, because it was disabled in configuration");

            return;
        }

        mgr = new WebSocketManager(ctx);
        topSrvc = new TopologyService(ctx, mgr);
        tracingSrvc = new TracingService(ctx, mgr);
        metricSrvc = new MetricsService(ctx, mgr);
        nodeConfigurationSrvc = new NodeConfigurationService(ctx, mgr);
        actionSrvc = new ActionService(ctx, mgr);

        execSrvc = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);

        submitConnectTask();
    }

    /**
     * @return Agent configuration.
     */
    private ManagementConfiguration readFromMetaStorage() {
        if (metaStorage == null)
            return new ManagementConfiguration();

        ManagementConfiguration cfg = null;

        ctx.cache().context().database().checkpointReadLock();

        try {
            cfg = (ManagementConfiguration) metaStorage.read(MANAGEMENT_CFG_META_STORAGE_PREFIX);
        }
        catch (IgniteCheckedException e) {
            log.warning("Can't read agent configuration from meta storage!");
        }
        finally {
            ctx.cache().context().database().checkpointReadUnlock();
        }

        return cfg != null ? cfg : new ManagementConfiguration();
    }

    /**
     * @param cfg Agent configuration.
     */
    private void writeToMetaStorage(ManagementConfiguration cfg) {
        ctx.cache().context().database().checkpointReadLock();

        try {
            metaStorage.write(MANAGEMENT_CFG_META_STORAGE_PREFIX, cfg);
        }
        catch (IgniteCheckedException e) {
            log.warning("Can't save management configuration to meta storage!");

            throw U.convertException(e);
        }
        finally {
            ctx.cache().context().database().checkpointReadUnlock();
        }
    }

    /**
     * Session handler for sending cluster info to backend.
     */
    private class AfterConnectedSessionHandler extends StompSessionHandlerAdapter {
        /** {@inheritDoc} */
        @Override public void afterConnected(StompSession ses, StompHeaders stompHeaders) {
            IgniteClusterImpl cluster = ctx.cluster().get();

            U.quietAndInfo(log, "");
            U.quietAndInfo(log, "Found GMC server that can be used to monitor your cluster: " + curSrvUri);

            U.quietAndInfo(log, "");
            U.quietAndInfo(log, "Open link in browser to monitor your cluster in GMC: " +
                    monitoringUri(curSrvUri, cluster.id()));

            U.quietAndInfo(log, "If you already using GMC, you can add cluster manually by it's ID: " + cluster.id());

            ses.send(buildClusterAddDest(), new ClusterInfo(cluster.id(), cluster.tag()));

            topSrvc.sendInitialState();

            ses.subscribe(buildMetricsPullTopic(), new StompFrameHandler() {
                @Override public Type getPayloadType(StompHeaders headers) {
                    return String.class;
                }

                @Override public void handleFrame(StompHeaders headers, Object payload) {
                    metricSrvc.broadcastPullMetrics();
                }
            });

            ses.subscribe(buildActionRequestTopic(cluster.id()), new StompFrameHandler() {
                @Override public Type getPayloadType(StompHeaders headers) {
                    return Request.class;
                }

                @Override public void handleFrame(StompHeaders headers, Object payload) {
                    actionSrvc.onActionRequest((Request) payload);
                }
            });
        }

        /** {@inheritDoc} */
        @Override public void handleTransportError(StompSession stompSes, Throwable e) {
            if (e instanceof ConnectionLostException) {
                if (disconnected.compareAndSet(false, true)) {
                    log.error("Lost websocket connection with server: " + curSrvUri);

                    submitConnectTask();
                }
            }
        }
    }

    /**
     * Send the connect task to executor service.
     */
    private void submitConnectTask() {
        // Submit a reconnection task only if we are no longer trying to reconnect
        if (execSrvc.getActiveCount() == 0)
            execSrvc.submit(this::connect0);
    }
}
