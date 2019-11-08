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

package org.apache.ignite.agent;

import java.lang.reflect.Type;
import java.net.ConnectException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.agent.dto.action.Request;
import org.apache.ignite.agent.service.ActionService;
import org.apache.ignite.agent.service.CacheService;
import org.apache.ignite.agent.service.ClusterService;
import org.apache.ignite.agent.service.config.NodeConfigurationExporter;
import org.apache.ignite.agent.service.config.NodeConfigurationService;
import org.apache.ignite.agent.service.event.EventsExporter;
import org.apache.ignite.agent.service.event.EventsService;
import org.apache.ignite.agent.service.metrics.MetricExporter;
import org.apache.ignite.agent.service.metrics.MetricsService;
import org.apache.ignite.agent.service.tracing.ManagementConsoleSpanExporter;
import org.apache.ignite.agent.service.tracing.TracingService;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.cluster.IgniteClusterImpl;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.processors.management.ManagementConfiguration;
import org.apache.ignite.internal.processors.management.ManagementConsoleProcessor;
import org.apache.ignite.internal.processors.metastorage.DistributedMetaStorage;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.eclipse.jetty.io.EofException;
import org.eclipse.jetty.websocket.api.UpgradeException;
import org.springframework.messaging.simp.stomp.ConnectionLostException;
import org.springframework.messaging.simp.stomp.StompCommand;
import org.springframework.messaging.simp.stomp.StompFrameHandler;
import org.springframework.messaging.simp.stomp.StompHeaders;
import org.springframework.messaging.simp.stomp.StompSession;
import org.springframework.messaging.simp.stomp.StompSessionHandlerAdapter;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;

import static java.util.Collections.singletonList;
import static org.apache.ignite.agent.StompDestinationsUtils.buildActionRequestTopic;
import static org.apache.ignite.agent.StompDestinationsUtils.buildMetricsPullTopic;
import static org.apache.ignite.agent.utils.AgentUtils.monitoringUri;
import static org.apache.ignite.agent.utils.AgentUtils.toWsUri;
import static org.apache.ignite.events.EventType.EVT_CACHE_STARTED;
import static org.apache.ignite.events.EventType.EVT_CACHE_STOPPED;
import static org.apache.ignite.events.EventType.EVT_CLUSTER_ACTIVATED;
import static org.apache.ignite.events.EventType.EVT_CLUSTER_DEACTIVATED;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.events.EventType.EVT_NODE_SEGMENTED;

/**
 * Management Agent.
 */
public class Agent extends ManagementConsoleProcessor {
    /** Management Console configuration meta storage prefix. */
    private static final String MANAGEMENT_CFG_META_STORAGE_PREFIX = "mgmt-console-cfg";

    /** Discovery event on restart agent. */
    private static final int[] EVTS_DISCOVERY = new int[] {EVT_NODE_FAILED, EVT_NODE_LEFT, EVT_NODE_SEGMENTED};

    /** Not enabled events by default. */
    private static final int[] NOT_ENABLED_EVTS = new int[] {
        EVT_CLUSTER_ACTIVATED,
        EVT_CLUSTER_DEACTIVATED,
        EVT_CACHE_STARTED,
        EVT_CACHE_STOPPED
    };

    /** Websocket manager. */
    private WebSocketManager mgr;

    /** Cluster service. */
    private ClusterService clusterSrvc;

    /** Tracing service. */
    private TracingService tracingSrvc;

    /** Span exporter. */
    private ManagementConsoleSpanExporter spanExporter;

    /** Events exporter. */
    private EventsExporter evtsExporter;

    /** Metric exporter. */
    private MetricExporter metricExporter;

    /** Metric service. */
    private MetricsService metricSrvc;

    /** Action service. */
    private ActionService actSrvc;

    /** Event service. */
    private EventsService evtSrvc;

    /** Node configuration service. */
    private NodeConfigurationService nodeConfigurationSrvc;

    /** Cache service. */
    private CacheService cacheSrvc;

    /** Execute service. */
    private ThreadPoolExecutor connectPool;

    /** Meta storage. */
    private DistributedMetaStorage metaStorage;

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
        metaStorage = ctx.distributedMetastorage();

        evtsExporter = new EventsExporter(ctx);
        spanExporter = new ManagementConsoleSpanExporter(ctx);
        metricExporter = new MetricExporter(ctx);

        // Connect to backend if local node is a coordinator or await coordinator change event.
        if (isCoordinator(ctx.discovery().discoCache()))
            connect();
        else
            ctx.event().addDiscoveryEventListener(this::launchAgentListener, EVTS_DISCOVERY);

        evtsExporter.addLocalEventListener();
        metricExporter.addMetricListener();

        try (NodeConfigurationExporter exporter = new NodeConfigurationExporter(ctx)) {
            exporter.export();
        }
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        ctx.event().removeDiscoveryEventListener(this::launchAgentListener, EVTS_DISCOVERY);

        U.closeQuiet(metricExporter);
        U.closeQuiet(evtsExporter);
        U.closeQuiet(spanExporter);

        disconnect();
    }

    /**
     *  Stop agent.
     */
    private void disconnect() {
        log.info("Stopping Management Console agent.");

        U.shutdownNow(getClass(), connectPool, log);

        U.closeQuiet(cacheSrvc);
        U.closeQuiet(actSrvc);
        U.closeQuiet(metricSrvc);
        U.closeQuiet(nodeConfigurationSrvc);
        U.closeQuiet(evtSrvc);
        U.closeQuiet(tracingSrvc);
        U.closeQuiet(clusterSrvc);
        U.closeQuiet(mgr);

        disconnected.set(false);

        U.quietAndInfo(log, "Management console agent stopped.");
    }

    /** {@inheritDoc} */
    @Override public void configuration(ManagementConfiguration cfg) {
        ManagementConfiguration oldCfg = configuration();

        if (oldCfg.isEnabled() != cfg.isEnabled())
            cfg = oldCfg.setEnabled(cfg.isEnabled());

        super.configuration(cfg);

        writeToMetaStorage(cfg);

        disconnect();

        launchAgentListener(null, ctx.discovery().discoCache());
    }

    /**
     * Start agent on local node if this is coordinator node.
     */
    private void launchAgentListener(DiscoveryEvent evt, DiscoCache discoCache) {
        if (isCoordinator(discoCache)) {
            cfg = readFromMetaStorage();

            connect();
        }
    }

    /**
     * @param uris Management Console Server URIs.
     */
    private String nextUri(List<String> uris, String cur) {
        int idx = uris.indexOf(cur);

        return uris.get((idx + 1) % uris.size());
    }

    /**
     * Connect to backend in same thread.
     */
    private void connect0() {
        curSrvUri = nextUri(cfg.getConsoleUris(), curSrvUri);

        try {
            mgr.connect(toWsUri(curSrvUri), cfg, new AfterConnectedSessionHandler());

            disconnected.set(false);
        }
        catch (IgniteInterruptedCheckedException | IgniteInterruptedException e) {
            if (log.isDebugEnabled())
                log.debug("Caught interrupted exception: " + e);

            mgr.close();
        }
        catch (InterruptedException e) {
            if (log.isDebugEnabled())
                log.debug("Caught interrupted exception: " + e);

            mgr.close();

            Thread.currentThread().interrupt();
        }
        catch (TimeoutException ignored) {
            connect0();
        }
        catch (ExecutionException e) {
            if (X.hasCause(e, ConnectException.class, UpgradeException.class, EofException.class, ConnectionLostException.class)) {
                if (disconnected.compareAndSet(false, true))
                    log.error("Failed to establish websocket connection with Management Console: " + curSrvUri);

                connect0();
            }
            else {
                log.error("Failed to establish websocket connection with Management Console: " + curSrvUri, e);

                mgr.close();
            }
        }
        catch (Exception e) {
            log.error("Failed to establish websocket connection with Management Console: " + curSrvUri, e);

            mgr.close();
        }
    }

    /**
     * Connect to backend.
     */
    private void connect() {
        if (!cfg.isEnabled()) {
            log.info("Skip start Management Console agent on coordinator, because it was disabled in configuration");

            return;
        }

        log.info("Starting Management Console agent on coordinator");

        mgr = new WebSocketManager(ctx);
        clusterSrvc = new ClusterService(ctx, mgr);
        tracingSrvc = new TracingService(ctx, mgr);
        metricSrvc = new MetricsService(ctx, mgr);
        evtSrvc = new EventsService(ctx, mgr);
        nodeConfigurationSrvc = new NodeConfigurationService(ctx, mgr);
        actSrvc = new ActionService(ctx, mgr);
        cacheSrvc = new CacheService(ctx, mgr);

        evtsExporter.addGlobalEventListener();

        connectPool =
            (ThreadPoolExecutor) Executors.newFixedThreadPool(1, new CustomizableThreadFactory("mgmt-console-connection-"));

        connectPool.submit(this::connect0);

        ctx.event().enableEvents(NOT_ENABLED_EVTS);
    }

    /**
     * @return Agent configuration.
     */
    private ManagementConfiguration readFromMetaStorage() {
        if (metaStorage == null)
            return new ManagementConfiguration();

        ctx.cache().context().database().checkpointReadLock();

        ManagementConfiguration cfg = null;

        try {
            cfg = metaStorage.read(MANAGEMENT_CFG_META_STORAGE_PREFIX);
        }
        catch (IgniteCheckedException e) {
            log.warning("Failed to read management configuration from meta storage!");
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
            log.warning("Failed to save management configuration to meta storage!");

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
            U.quietAndInfo(log, "Found Management Console that can be used to monitor your cluster:: " + curSrvUri);

            U.quietAndInfo(log, "");
            U.quietAndInfo(log, "Open link in browser to monitor your cluster: " +
                    monitoringUri(curSrvUri, cluster.id()));

            U.quietAndInfo(log, "If you already using Management Console, you can add cluster manually by it's ID: " + cluster.id());

            clusterSrvc.sendInitialState();
            cacheSrvc.sendInitialState();

            ses.subscribe(buildMetricsPullTopic(), new StompFrameHandler() {
                /** {@inheritDoc} */
                @Override public Type getPayloadType(StompHeaders headers) {
                    return String.class;
                }

                /** {@inheritDoc} */
                @Override public void handleFrame(StompHeaders headers, Object payload) {
                    metricSrvc.broadcastPullMetrics();
                }
            });

            ses.subscribe(buildActionRequestTopic(cluster.id()), new StompFrameHandler() {
                /** {@inheritDoc} */
                @Override public Type getPayloadType(StompHeaders headers) {
                    return Request.class;
                }

                /** {@inheritDoc} */
                @Override public void handleFrame(StompHeaders headers, Object payload) {
                    actSrvc.onActionRequest((Request) payload);
                }
            });

            cfg.setConsoleUris(singletonList(curSrvUri));

            writeToMetaStorage(cfg);
        }

        /** {@inheritDoc} */
        @Override public void handleException(StompSession ses, StompCommand cmd, StompHeaders headers, byte[] payload, Throwable e) {
            log.warning("Failed to process a STOMP frame", e);
        }

        /** {@inheritDoc} */
        @Override public void handleTransportError(StompSession stompSes, Throwable e) {
            if (e instanceof ConnectionLostException) {
                if (disconnected.compareAndSet(false, true)) {
                    log.error("Lost websocket connection with server: " + curSrvUri);

                    reconnect();
                }
            }
        }
    }

    /**
     * Submit a reconnection task only if there no active connect in progress.
     */
    private void reconnect() {
        if (connectPool.getActiveCount() == 0)
            connectPool.submit(this::connect0);
    }

    /**
     * @param discoCache Disco cache.
     */
    private boolean isCoordinator(DiscoCache discoCache) {
        ClusterNode crdNode = F.first(discoCache.serverNodes());

        return crdNode != null && crdNode.isLocal();
    }
}
