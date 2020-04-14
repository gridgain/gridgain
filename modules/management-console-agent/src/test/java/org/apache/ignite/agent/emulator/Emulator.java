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

package org.apache.ignite.agent.emulator;

import java.io.EOFException;
import java.lang.reflect.Type;
import java.net.ConnectException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;
import javax.websocket.DeploymentException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.agent.dto.action.Request;
import org.apache.ignite.agent.dto.action.Status;
import org.apache.ignite.agent.dto.action.TaskResponse;
import org.apache.ignite.agent.dto.cluster.BaselineInfo;
import org.apache.ignite.agent.dto.cluster.ClusterInfo;
import org.apache.ignite.agent.dto.topology.Node;
import org.apache.ignite.agent.dto.topology.TopologySnapshot;
import org.apache.ignite.agent.ws.WebSocketManager;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteFeatures;
import org.apache.ignite.internal.cluster.IgniteClusterImpl;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.StandaloneGridKernalContext;
import org.apache.ignite.internal.processors.cluster.ClusterProcessor;
import org.apache.ignite.internal.processors.management.ManagementConfiguration;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.logger.NullLogger;
import org.jetbrains.annotations.Nullable;
import org.springframework.messaging.simp.stomp.ConnectionLostException;
import org.springframework.messaging.simp.stomp.StompCommand;
import org.springframework.messaging.simp.stomp.StompFrameHandler;
import org.springframework.messaging.simp.stomp.StompHeaders;
import org.springframework.messaging.simp.stomp.StompSession;
import org.springframework.messaging.simp.stomp.StompSessionHandlerAdapter;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.agent.StompDestinationsUtils.buildActionRequestTopic;
import static org.apache.ignite.agent.StompDestinationsUtils.buildActionTaskResponseDest;
import static org.apache.ignite.agent.StompDestinationsUtils.buildClusterDest;
import static org.apache.ignite.agent.StompDestinationsUtils.buildClusterTopologyDest;
import static org.apache.ignite.agent.StompDestinationsUtils.buildMetricsPullTopic;
import static org.apache.ignite.agent.utils.AgentUtils.monitoringUri;
import static org.apache.ignite.agent.utils.AgentUtils.toWsUri;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_BUILD_VER;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_IPS;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_JVM_PID;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_MACS;

/**
 * Emulator launcher.
 */
public class Emulator {
    /** */
    private static final UUID CLUSTER_ID = UUID.fromString("603871d5-86f8-4993-a3f3-692aaad11899");

    /** */
    private static final String CLUSTER_TAG = "emulator";

    /** */
    private final List<Node> nodes;

    /** */
    private final IgniteLogger log;

    /** Context. */
    private final GridKernalContext ctx;

    /** Config. */
    private final ManagementConfiguration cfg;

    /** Execute service. */
    private final ExecutorService connectPool;

    /** Websocket manager. */
    private final WebSocketManager mgr;

    /** If first connection error after successful connection. */
    private AtomicBoolean disconnected = new AtomicBoolean();

    /** Active server URI. */
    private String curSrvUri;

    /** */
    private final AtomicBoolean clusterState = new AtomicBoolean();

    /** */
    private final ClusterInfo clusterInfo;

    /**
     * Constructor.
     *
     * @param clusterSz Cluster size.
     */
    public Emulator(int clusterSz) throws IgniteCheckedException {
        log = new TempLogger();
        ctx = new EmulatorContext(log);
        cfg = new ManagementConfiguration();
        mgr = new WebSocketManager(ctx);
        connectPool = Executors.newSingleThreadExecutor(new CustomizableThreadFactory("emulator-connection-"));

        clusterInfo = new ClusterInfo(CLUSTER_ID, CLUSTER_TAG)
            .setPersistenceEnabled(true)
            .setBaselineParameters(new BaselineInfo(false, 100000))
            .setSecure(false)
            .setFeatures(features());

        Map<String, Object> attrs = new HashMap<>();
        attrs.put(ATTR_IPS, "192.168.0.1");
        attrs.put(ATTR_MACS, "de:ad:be:ef");
        attrs.put(ATTR_JVM_PID, 42);
        attrs.put(ATTR_BUILD_VER, "9.9.9");

        nodes = IntStream.range(0, clusterSz)
            .mapToObj(i -> new Node(
                UUID.randomUUID(),
                UUID.randomUUID(),
                i,
                false,
                true,
                attrs
            ))
            .collect(toList());
    }

    /**
     * Start emulator.
     */
    public void start() {
        log.info("Starting cluster emulator...");

        reconnect();
    }

    /**
     * Submit a reconnection task.
     */
    private void reconnect() {
        connectPool.submit(this::connect);
    }

    /**
     * Connect to backend in same thread.
     */
    private void connect() {
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
     * @param uris Control Center Server URIs.
     */
    private String nextUri(List<String> uris, String cur) {
        int idx = uris.indexOf(cur);

        return uris.get((idx + 1) % uris.size());
    }

    /**
     * Send initial cluster state.
     */
    private void sendInitialClusterState() {
        sendClusterInfo();
        sendTopologyUpdate();
    }

    /**
     * Send cluster info.
     */
    private void sendClusterInfo() {
        log.info("sendClusterInfo");

        clusterInfo.setActive(clusterState.get());

        mgr.send(buildClusterDest(CLUSTER_ID), clusterInfo);
    }

    /**
     * @return Features suported by emulator.
     */
    private Set<String> features() {
        IgniteFeatures[] enums = IgniteFeatures.values();

        Set<String> features = U.newHashSet(enums.length);

        for (IgniteFeatures val : enums)
                features.add(val.name());

        return features;
    }

    /**
     * Send full topology to Control Center.
     */
    void sendTopologyUpdate() {
        String crdConsistentId = nodes.get(0).getConsistentId();

        TopologySnapshot top = new TopologySnapshot(1, crdConsistentId, nodes);

        mgr.send(
            buildClusterTopologyDest(CLUSTER_ID),
            top
        );
    }

    /**
     * Send initial caches states.
     */
    private void sendInitialCachesState() {
        log.info("TODO sendInitialCachesState");

        // sendCacheInfo();
    }

    /**
     *
     */
    private void broadcastPullMetrics() {
        log.info("TODO broadcastPullMetrics");
    }

    /**
     * @param req Request to process.
     */
    private void processDistributedActionRequest(Request req) {
        log.info("Process action request: " + req);

        String act = req.getAction();

        if ("ClusterActions.activate".equalsIgnoreCase(act)) {
            clusterState.set(true);

            TaskResponse res = new TaskResponse()
                .setId(req.getId())
                .setStatus(Status.COMPLETED);

            mgr.send(buildActionTaskResponseDest(CLUSTER_ID), res);

            sendClusterInfo();
        }
    }

    /**
     * Temporary logger.
     *
     * TODO GG-28545 Implement log4j support later.
     */
    private static class TempLogger extends NullLogger {
        /** {@inheritDoc} */
        @Override public void info(String msg) {
            U.quiet(false, "[emulator] " + msg);
        }

        /** {@inheritDoc} */
        @Override public void error(String msg) {
            U.error(null, msg);
        }

        /** {@inheritDoc} */
        @Override public void error(String msg, @Nullable Throwable e) {
            U.error(null, msg, e);
        }
    }

    /**
     * Context for emulator.
     */
    private static class EmulatorContext extends StandaloneGridKernalContext {
        /** */
        private ClusterProcessor cluster;

        /**
         * @param log Logger.
         */
        EmulatorContext(IgniteLogger log) throws IgniteCheckedException {
            super(log, null, null);

            cluster = new ClusterProcessor(this);
            IgniteClusterImpl impl = cluster.get();
            impl.setId(CLUSTER_ID);
            impl.setTag(CLUSTER_TAG);
        }

        /** {@inheritDoc} */
        @Override public ClusterProcessor cluster() {
            return cluster;
        }
    }

    /**
     * Session handler for sending cluster info to backend.
     */
    private class AfterConnectedSessionHandler extends StompSessionHandlerAdapter {
        /** {@inheritDoc} */
        @Override public void afterConnected(StompSession ses, StompHeaders stompHeaders) {
            log.info("Found Control Center that can be used to monitor your cluster: " + curSrvUri);
            log.info("Open link in browser to monitor your cluster: " + monitoringUri(curSrvUri, CLUSTER_ID));
            log.info("If you are already using Control Center, you can add the cluster manually by its ID: " + CLUSTER_ID);

            sendInitialClusterState();

            sendInitialCachesState();

            ses.subscribe(buildMetricsPullTopic(), new StompFrameHandler() {
                /** {@inheritDoc} */
                @Override public Type getPayloadType(StompHeaders headers) {
                    return String.class;
                }

                /** {@inheritDoc} */
                @Override public void handleFrame(StompHeaders headers, Object payload) {
                    broadcastPullMetrics();
                }
            });

            ses.subscribe(buildActionRequestTopic(CLUSTER_ID), new StompFrameHandler() {
                /** {@inheritDoc} */
                @Override public Type getPayloadType(StompHeaders headers) {
                    return Request.class;
                }

                /** {@inheritDoc} */
                @Override public void handleFrame(StompHeaders headers, Object payload) {
                    processDistributedActionRequest((Request)payload);
                }
            });
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
     * @param args Command line arguments.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        Emulator emulator = new Emulator(10);

        emulator.start();
    }
}
