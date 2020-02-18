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

import java.io.EOFException;
import java.lang.reflect.Type;
import java.net.ConnectException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.websocket.DeploymentException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.agent.action.ActionDispatcher;
import org.apache.ignite.agent.action.SessionRegistry;
import org.apache.ignite.agent.dto.action.Request;
import org.apache.ignite.agent.processor.CacheChangesProcessor;
import org.apache.ignite.agent.processor.ClusterInfoProcessor;
import org.apache.ignite.agent.processor.ManagementConsoleMessagesProcessor;
import org.apache.ignite.agent.processor.action.DistributedActionProcessor;
import org.apache.ignite.agent.processor.export.EventsExporter;
import org.apache.ignite.agent.processor.export.MetricsExporter;
import org.apache.ignite.agent.processor.export.NodesConfigurationExporter;
import org.apache.ignite.agent.processor.export.SpanExporter;
import org.apache.ignite.agent.processor.metrics.MetricsProcessor;
import org.apache.ignite.agent.ws.WebSocketManager;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.cluster.IgniteClusterImpl;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.management.ManagementConfiguration;
import org.apache.ignite.internal.processors.management.ManagementConsoleProcessor;
import org.apache.ignite.internal.processors.metastorage.DistributedMetaStorage;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
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
import static org.apache.ignite.agent.utils.AgentUtils.quiteStop;
import static org.apache.ignite.agent.utils.AgentUtils.toWsUri;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.events.EventType.EVT_NODE_SEGMENTED;
import static org.apache.ignite.internal.IgniteFeatures.TRACING;
import static org.apache.ignite.internal.IgniteFeatures.nodeSupports;
import static org.apache.ignite.internal.util.IgniteUtils.isLocalNodeCoordinator;

/**
 * Control Center agent.
 */
public class ManagementConsoleAgent extends GridProcessorAdapter implements ManagementConsoleProcessor {
    /** Control Center configuration meta storage prefix. */
    private static final String MANAGEMENT_CFG_META_STORAGE_PREFIX = "mgmt-console-cfg";

    /** Topic of Control Center configuration. */
    public static final String TOPIC_MANAGEMENT_CONSOLE = "mgmt-console-topic";

    /** Discovery event on restart agent. */
    private static final int[] EVTS_DISCOVERY = new int[] {EVT_NODE_FAILED, EVT_NODE_LEFT, EVT_NODE_SEGMENTED};

    /** Management configuration instance. */
    private ManagementConfiguration cfg = new ManagementConfiguration();

    /** Websocket manager. */
    protected WebSocketManager mgr;

    /** Cluster processor. */
    private ClusterInfoProcessor clusterProc;

    /** Span exporter. */
    private SpanExporter spanExporter;

    /** Events exporter. */
    private EventsExporter evtsExporter;

    /** Metric exporter. */
    private MetricsExporter metricExporter;

    /** Metric processor. */
    private MetricsProcessor metricProc;

    /** Actions dispatcher. */
    private ActionDispatcher actDispatcher;

    /** Distributed action processor. */
    private DistributedActionProcessor distributedActProc;

    /** Topic processor. */
    private ManagementConsoleMessagesProcessor messagesProc;

    /** Cache processor. */
    private CacheChangesProcessor cacheProc;

    /** Execute service. */
    private ExecutorService connectPool;

    /** Meta storage. */
    private DistributedMetaStorage metaStorage;

    /** Session registry. */
    private SessionRegistry sesRegistry;

    /** Active server URI. */
    private String curSrvUri;

    /** If first connection error after successful connection. */
    private AtomicBoolean disconnected = new AtomicBoolean();

    /** Agent started. */
    private final AtomicBoolean agentStarted = new AtomicBoolean();

    /**
     * @param ctx Kernal context.
     */
    public ManagementConsoleAgent(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart(boolean active) {
        metaStorage = ctx.distributedMetastorage();
        evtsExporter = new EventsExporter(ctx);
        metricExporter = new MetricsExporter(ctx);
        actDispatcher = new ActionDispatcher(ctx);

        if (isTracingEnabled())
            spanExporter = new SpanExporter(ctx);
        else
            U.quietAndWarn(log, "Current Ignite configuration does not support tracing functionality" +
                " and control center agent will not collect traces" +
                " (consider adding ignite-opencensus module to classpath).");

        // Connect to backend if local node is a coordinator or await coordinator change event.
        if (isLocalNodeCoordinator(ctx.discovery())) {
            messagesProc = new ManagementConsoleMessagesProcessor(ctx);

            connect();
        }
        else
            ctx.event().addDiscoveryEventListener(this::launchAgentListener, EVTS_DISCOVERY);

        evtsExporter.addLocalEventListener();

        metricExporter.addMetricListener();

        NodesConfigurationExporter exporter = new NodesConfigurationExporter(ctx);

        exporter.export();

        quiteStop(exporter);
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        ctx.event().removeDiscoveryEventListener(this::launchAgentListener, EVTS_DISCOVERY);

        quiteStop(actDispatcher);
        quiteStop(messagesProc);
        quiteStop(metricExporter);
        quiteStop(evtsExporter);
        quiteStop(spanExporter);

        disconnect();
    }

    /**
     *  Stop agent.
     */
    private void disconnect() {
        log.info("Stopping Control Center agent.");

        U.shutdownNow(getClass(), connectPool, log);

        quiteStop(cacheProc);
        quiteStop(distributedActProc);
        quiteStop(metricProc);
        quiteStop(clusterProc);
        quiteStop(mgr);

        disconnected.set(false);

        U.quietAndInfo(log, "Control Center agent stopped.");
    }

    /** {@inheritDoc} */
    @Override public ManagementConfiguration configuration() {
        return cfg;
    }

    /** {@inheritDoc} */
    @Override public void configuration(ManagementConfiguration cfg) {
        ManagementConfiguration oldCfg = configuration();

        if (oldCfg.isEnabled() != cfg.isEnabled())
            cfg = oldCfg.setEnabled(cfg.isEnabled());

        this.cfg = cfg;

        writeToMetaStorage(cfg);

        disconnect();

        connect();
    }

    /**
     * @return Session registry.
     */
    public SessionRegistry sessionRegistry() {
        return sesRegistry;
    }

    /**
     * @return Weboscket manager.
     */
    public WebSocketManager webSocketManager() {
        return mgr;
    }

    /**
     * @return Action dispatcher.
     */
    public ActionDispatcher actionDispatcher() {
        return actDispatcher;
    }

    /**
     * @return Destributed actions processor.
     */
    public DistributedActionProcessor distributedActionProcessor() {
        return distributedActProc;
    }

    /**
     * @return {@code True} if tracing is enable.
     */
    boolean isTracingEnabled() {
        return nodeSupports(ctx, ctx.grid().localNode(), TRACING);
    }

    /**
     * Start agent on a local node if it is a coordinator node.
     */
    private void launchAgentListener(DiscoveryEvent evt, DiscoCache discoCache) {
        if (isLocalNodeCoordinator(ctx.discovery()) && agentStarted.compareAndSet(false, true)) {
            ctx.event().removeDiscoveryEventListener(this::launchAgentListener, EVTS_DISCOVERY);

            cfg = readFromMetaStorage();

            connect();
        }
    }

    /**
     * @param uris Control Center Server URIs.
     */
    private String nextUri(List<String> uris, String cur) {
        int idx = uris.indexOf(cur);

        return uris.get((idx + 1) % uris.size());
    }

    /**
     * Connect to backend in same thread.
     */
    private void connect0() {
        while (!ctx.isStopping()) {
            try {
                mgr.stop(true);

                curSrvUri = nextUri(cfg.getConsoleUris(), curSrvUri);

                mgr.connect(toWsUri(curSrvUri), cfg, new AfterConnectedSessionHandler());

                disconnected.set(false);

                break;
            }
            catch (Exception e) {
                mgr.stop(true);

                if (X.hasCause(e, InterruptedException.class)) {
                    U.quiet(true, "Caught interrupted exception: " + e);

                    Thread.currentThread().interrupt();

                    break;
                }
                else if (
                    X.hasCause(
                        e,
                        TimeoutException.class,
                        ConnectException.class,
                        EOFException.class,
                        ConnectionLostException.class,
                        DeploymentException.class
                    )
                ) {
                    if (disconnected.compareAndSet(false, true))
                        log.error("Failed to establish websocket connection with Control Center: " + curSrvUri);
                }
                else
                    log.error("Failed to establish websocket connection with Control Center: " + curSrvUri, e);
            }
        }
    }

    /**
     * Connect to backend.
     */
    private void connect() {
        if (!cfg.isEnabled()) {
            log.info("Control Center agent was not started on coordinator, because it was disabled in configuration");
            log.info("You can use control script to enable Control Center agent");

            return;
        }

        if (F.isEmpty(cfg.getConsoleUris())) {
            log.info("Control Center agent  was not started on coordinator, because the server URI was not set");
            log.info("You can use control script to setup server URI");

            return;
        }

        log.info("Starting Control Center agent on coordinator");

        mgr = new WebSocketManager(ctx);
        sesRegistry = new SessionRegistry(ctx);
        clusterProc =  createClusterInfoProcessor();
        metricProc = new MetricsProcessor(ctx, mgr);
        distributedActProc = new DistributedActionProcessor(ctx);
        cacheProc = new CacheChangesProcessor(ctx, mgr);

        evtsExporter.addGlobalEventListener();

        connectPool = Executors.newSingleThreadExecutor(new CustomizableThreadFactory("mgmt-console-connection-"));

        connectPool.submit(this::connect0);
    }

    /**
     * @return Agent configuration.
     */
    private ManagementConfiguration readFromMetaStorage() {
        if (metaStorage == null)
            return new ManagementConfiguration();

        ctx.cache().context().database().checkpointReadLock();

        ManagementConfiguration cfg;

        try {
            cfg = metaStorage.read(MANAGEMENT_CFG_META_STORAGE_PREFIX);
        }
        catch (IgniteCheckedException e) {
            log.warning("Failed to read management configuration from meta storage!");

            throw U.convertException(e);
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
     * @return Cluster info processor.
     */
    protected ClusterInfoProcessor createClusterInfoProcessor() {
        return new ClusterInfoProcessor(ctx, mgr);
    }

    /**
     * Session handler for sending cluster info to backend.
     */
    private class AfterConnectedSessionHandler extends StompSessionHandlerAdapter {
        /** {@inheritDoc} */
        @Override public void afterConnected(StompSession ses, StompHeaders stompHeaders) {
            IgniteClusterImpl cluster = ctx.cluster().get();

            U.quietAndInfo(log, "");

            U.quietAndInfo(log, "Found Control Center that can be used to monitor your cluster: " + curSrvUri);

            U.quietAndInfo(log, "");

            U.quietAndInfo(log, "Open link in browser to monitor your cluster: " +
                    monitoringUri(curSrvUri, cluster.id()));

            U.quietAndInfo(log, "If you are already using Control Center, you can add the cluster manually" +
                " by its ID: " + cluster.id());

            clusterProc.sendInitialState();

            cacheProc.sendInitialState();

            ses.subscribe(buildMetricsPullTopic(), new StompFrameHandler() {
                /** {@inheritDoc} */
                @Override public Type getPayloadType(StompHeaders headers) {
                    return String.class;
                }

                /** {@inheritDoc} */
                @Override public void handleFrame(StompHeaders headers, Object payload) {
                    metricProc.broadcastPullMetrics();
                }
            });

            ses.subscribe(buildActionRequestTopic(cluster.id()), new StompFrameHandler() {
                /** {@inheritDoc} */
                @Override public Type getPayloadType(StompHeaders headers) {
                    return Request.class;
                }

                /** {@inheritDoc} */
                @Override public void handleFrame(StompHeaders headers, Object payload) {
                    distributedActProc.onActionRequest((Request)payload);
                }
            });

            cfg.setConsoleUris(singletonList(curSrvUri));

            writeToMetaStorage(cfg);
        }

        /** {@inheritDoc} */
        @Override public void handleException(
            StompSession ses,
            StompCommand cmd,
            StompHeaders headers,
            byte[] payload,
            Throwable e
        ) {
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
     * Submit a reconnection task.
     */
    private void reconnect() {
        connectPool.submit(this::connect0);
    }
}
