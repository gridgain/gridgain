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

package org.apache.ignite.internal.processors.cluster;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.ClusterIdUpdatedEvent;
import org.apache.ignite.events.ClusterTagUpdatedEvent;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.ClusterMetricsSnapshot;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridKernalGatewayImpl;
import org.apache.ignite.internal.IgniteDiagnosticInfo;
import org.apache.ignite.internal.IgniteDiagnosticMessage;
import org.apache.ignite.internal.IgniteFeatures;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.cluster.IgniteClusterImpl;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.IgniteClusterNode;
import org.apache.ignite.internal.managers.discovery.IgniteDiscoverySpi;
import org.apache.ignite.internal.managers.eventstorage.DiscoveryEventListener;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedConfigurationLifecycleListener;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedPropertyDispatcher;
import org.apache.ignite.internal.processors.configuration.distributed.SimpleDistributedProperty;
import org.apache.ignite.internal.processors.metastorage.DistributedMetaStorage;
import org.apache.ignite.internal.processors.metastorage.DistributedMetastorageLifecycleListener;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObject;
import org.apache.ignite.internal.util.GridTimerTask;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.future.IgniteFinishedFutureImpl;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.mxbean.IgniteClusterMXBean;
import org.apache.ignite.spi.discovery.DiscoveryDataBag;
import org.apache.ignite.spi.discovery.DiscoveryDataBag.GridDiscoveryData;
import org.apache.ignite.spi.discovery.DiscoveryMetricsProvider;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.jetbrains.annotations.Nullable;

import javax.management.JMException;
import javax.management.ObjectName;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Timer;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.ignite.IgniteSystemProperties.*;
import static org.apache.ignite.events.EventType.*;
import static org.apache.ignite.internal.GridComponent.DiscoveryDataExchangeType.CLUSTER_PROC;
import static org.apache.ignite.internal.GridTopic.TOPIC_INTERNAL_DIAGNOSTIC;
import static org.apache.ignite.internal.GridTopic.TOPIC_METRICS;
import static org.apache.ignite.internal.IgniteVersionUtils.VER_STR;
import static org.apache.ignite.internal.SupportFeaturesUtils.IGNITE_CLUSTER_ID_AND_TAG_FEATURE;
import static org.apache.ignite.internal.SupportFeaturesUtils.isFeatureEnabled;
import static org.apache.ignite.internal.cluster.DistributedConfigurationUtils.makeUpdateListener;
import static org.apache.ignite.internal.cluster.DistributedConfigurationUtils.setDefaultValue;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.CLUSTER_METRICS;

/**
 *
 */
public class ClusterProcessor extends GridProcessorAdapter implements DistributedMetastorageLifecycleListener {
    /** */
    private static final String ATTR_UPDATE_NOTIFIER_STATUS = "UPDATE_NOTIFIER_STATUS";

    /** */
    private static final String CLUSTER_ID_TAG_KEY =
        DistributedMetaStorage.IGNITE_INTERNAL_KEY_PREFIX + "cluster.id.tag";

    /** */
    private static final String M_BEAN_NAME = "IgniteCluster";

    /** Periodic version check delay. */
    private static final long PERIODIC_VER_CHECK_DELAY = 1000 * 60 * 60; // Every hour.

    /** Periodic version check delay. */
    private static final long PERIODIC_VER_CHECK_CONN_TIMEOUT = 10 * 1000; // 10 seconds.

    /** Total server nodes count metric name. */
    public static final String TOTAL_SERVER_NODES = "TotalServerNodes";

    /** Total client nodes count metric name. */
    public static final String TOTAL_CLIENT_NODES = "TotalClientNodes";

    /** Total baseline nodes count metric name. */
    public static final String TOTAL_BASELINE_NODES = "TotalBaselineNodes";

    /** Active baseline nodes count metric name. */
    public static final String ACTIVE_BASELINE_NODES = "ActiveBaselineNodes";

    /** @see IgniteSystemProperties#IGNITE_UPDATE_NOTIFIER */
    public static final boolean DFLT_UPDATE_NOTIFIER = true;

    /** @see IgniteSystemProperties#IGNITE_DIAGNOSTIC_ENABLED */
    public static final boolean DFLT_DIAGNOSTIC_ENABLED = true;

    /** */
    private final IgniteClusterImpl cluster;

    /** */
    private final AtomicBoolean notifyEnabled = new AtomicBoolean();

    /** */
    private final AtomicReference<String> updateNotifierUrl = new AtomicReference<>();

    /** */
    @GridToStringExclude
    private Timer updateNtfTimer;

    /** Version checker. */
    @GridToStringExclude
    private GridUpdateNotifier verChecker;

    /** */
    private final AtomicReference<ConcurrentHashMap<Long, InternalDiagnosticFuture>> diagnosticFutMap =
        new AtomicReference<>();

    /** */
    private final AtomicLong diagFutId = new AtomicLong();

    /** */
    private final Map<UUID, byte[]> allNodesMetrics = new ConcurrentHashMap<>();

    /** */
    private final JdkMarshaller marsh = new JdkMarshaller();

    /** */
    private DiscoveryMetricsProvider metricsProvider;

    /** */
    private final boolean sndMetrics;

    /** Flag is used to detect and manage case when new node (this one) joins old cluster. */
    private volatile boolean compatibilityMode;

    /**
     * Flag indicates that the feature is disabled.
     * No values should be stored in metastorage nor passed in joining node discovery data.
     */
    private final boolean clusterIdAndTagSupport = isFeatureEnabled(IGNITE_CLUSTER_ID_AND_TAG_FEATURE);

    private final SimpleDistributedProperty<ClusterIdAndTag> clusterIdAndTagProperty =
        new SimpleDistributedProperty<>(CLUSTER_ID_TAG_KEY, null);

    /** */
    private ObjectName mBean;

    /**
     * @param ctx Kernal context.
     */
    public ClusterProcessor(GridKernalContext ctx) {
        super(ctx);

        notifyEnabled.set(IgniteSystemProperties.getBoolean(IGNITE_UPDATE_NOTIFIER, DFLT_UPDATE_NOTIFIER));

        updateNotifierUrl.set(IgniteSystemProperties.getString(GRIDGAIN_UPDATE_URL,
            GridUpdateNotifier.DEFAULT_GRIDGAIN_UPDATES_URL));

        cluster = new IgniteClusterImpl(ctx);

        sndMetrics = !(ctx.config().getDiscoverySpi() instanceof TcpDiscoverySpi);

        if (clusterIdAndTagSupport) {
            ctx.internalSubscriptionProcessor().registerDistributedConfigurationListener(
                new DistributedConfigurationLifecycleListener() {
                    @Override public void onReadyToRegister(DistributedPropertyDispatcher dispatcher) {
                        String logMsgFmt = "Cluster ID and tag changed [property=%s, oldVal=%s, newVal=%s]";

                        clusterIdAndTagProperty.addListener(makeUpdateListener(logMsgFmt, log));
                        clusterIdAndTagProperty.addListener((name, oldVal, newVal) -> {
                            // User-requested updates always have both old and new val set.
                            if (oldVal != null && newVal != null) {
                                // Record ID update event.
                                if (!Objects.equals(oldVal.id(), newVal.id())
                                    && ctx.event().isRecordable(EVT_CLUSTER_ID_UPDATED)) {
                                    String msg = "ID has been updated to new value: " +
                                        newVal.id() +
                                        ", previous value was " +
                                        oldVal.id();

                                    ctx.closure().runLocalSafe(() -> ctx.event().record(
                                        new ClusterIdUpdatedEvent(
                                            ctx.discovery().localNode(),
                                            msg,
                                            oldVal.id(),
                                            newVal.id()
                                        )
                                    ));
                                }

                                // Record tag update event.
                                if (!Objects.equals(oldVal.tag(), newVal.tag())
                                    && ctx.event().isRecordable(EVT_CLUSTER_TAG_UPDATED)) {
                                    String msg = "Tag has been updated to new value: " +
                                        newVal.tag() +
                                        ", previous value was " +
                                        oldVal.tag();

                                    ctx.closure().runLocalSafe(() -> ctx.event().record(
                                        new ClusterTagUpdatedEvent(
                                            ctx.discovery().localNode(),
                                            msg,
                                            oldVal.id(),
                                            oldVal.tag(),
                                            newVal.tag()
                                        )
                                    ));
                                }
                            }
                        });

                        dispatcher.registerProperty(clusterIdAndTagProperty);
                    }

                    @Override public void onReadyToWrite() {
                        if (!compatibilityMode)
                            initializeClusterIdAndTagIfNeeded();
                    }
                });
        }
    }

    /**
     * @return Diagnostic flag.
     */
    public boolean diagnosticEnabled() {
        return getBoolean(IGNITE_DIAGNOSTIC_ENABLED, DFLT_DIAGNOSTIC_ENABLED);
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        if (ctx.isDaemon())
            return;

        cluster.start();
    }

    /**
     * @return Returns cluster ID.
     */
    public UUID getId() {
        // For compatibility, we avoid throwing exception on read and return null instead.
        // Prior to GG-33727, this used to return an actual non-null value propagated using a complex algorithm.
        // It was abandoned in favor of simple metastore property.
        if (compatibilityMode)
            return null;

        ClusterIdAndTag clusterIdAndTag = clusterIdAndTagProperty.get();
        return clusterIdAndTag != null ? clusterIdAndTag.id() : null;
    }

    /**
     * Method is called when user requests updating cluster ID through public API.
     *
     * @param newId New ID.
     */
    public void updateId(UUID newId) {
        if (compatibilityMode)
            throw new IllegalStateException("Not all nodes in the cluster support cluster ID and tag.");

        ClusterIdAndTag old = clusterIdAndTagProperty.get();
        try {
            clusterIdAndTagProperty.propagate(new ClusterIdAndTag(newId, old.tag()));
        } catch (IgniteCheckedException e) {
            throw new IgniteException("Unexpectedly failed to update cluster ID and tag", e);
        }
    }

    /**
     * @return Returns cluster tag.
     */
    public String getTag() {
        // For compatibility, we avoid throwing exception on read and return null instead.
        // Prior to GG-33727, this used to return an actual non-null value propagated using a complex algorithm.
        // It was abandoned in favor of simple metastore property.
        if (compatibilityMode)
            return null;

        ClusterIdAndTag clusterIdAndTag = clusterIdAndTagProperty.get();
        return clusterIdAndTag != null ? clusterIdAndTag.tag() : null;
    }

    /**
     * Method is called when user requests updating tag through public API.
     *
     * @param newTag New tag.
     */
    public void updateTag(String newTag) {
        if (compatibilityMode)
            throw new IllegalStateException("Not all nodes in the cluster support cluster ID and tag.");

        ClusterIdAndTag old = clusterIdAndTagProperty.get();
        try {
            clusterIdAndTagProperty.propagate(new ClusterIdAndTag(old.id(), newTag));
        } catch (IgniteCheckedException e) {
            throw new IgniteException("Unexpectedly failed to update cluster ID and tag", e);
        }
    }

    /**
     * Node makes ID and tag available through public API on local join event.
     *
     * Effectively no-op if working in compatibility mode (see javadoc for {@link ClusterProcessor#compatibilityMode})
     *
     * Two cases.
     * <ul>
     *     <li>In in-memory scenario very first node of the cluster generates ID and tag,
     *     other nodes receives them on join.</li>
     *     <li>When persistence is enabled each node reads ID and tag from metastorage
     *     when it becomes ready for read.</li>
     * </ul>
     */
    public void onLocalJoin() {
        if (!clusterIdAndTagSupport)
            return;

        if (ctx.discovery().localNode().isClient() || ctx.discovery().localNode().isDaemon())
            return;

        compatibilityMode = !IgniteFeatures.allNodesSupports(ctx, F.view(ctx.discovery().remoteNodes(), IgniteDiscoverySpi.SRV_NODES),
            IgniteFeatures.CLUSTER_ID_AND_TAG);

        DiscoveryEventListener discoveryEventListener = new DiscoveryEventListener() {
            @Override public void onEvent(DiscoveryEvent evt, DiscoCache discoCache) {
                // Initialize cluster ID and tag if all old servers have left.
                if (compatibilityMode
                    && IgniteFeatures.allNodesSupports(ctx, F.view(discoCache.remoteNodes(), IgniteDiscoverySpi.SRV_NODES),
                    IgniteFeatures.CLUSTER_ID_AND_TAG)
                ) {
                    compatibilityMode = false;
                    ClusterProcessor.this.initializeClusterIdAndTagIfNeeded();
                }

                if (!compatibilityMode)
                    ctx.event().removeDiscoveryEventListener(this);
            }
        };
        ctx.event().addDiscoveryEventListener(discoveryEventListener, EVT_NODE_LEFT, EVT_NODE_FAILED);
    }

    /**
     * Initialize cluster ID and tag in the metastore if not present.
     */
    private void initializeClusterIdAndTagIfNeeded() {
        // We can't write the value to metastore before all nodes are upgraded
        // to avoid NoClassDefFoundError on metastore scan.
        if (compatibilityMode)
            return;

        ClusterIdAndTag newIdAndTag = new ClusterIdAndTag(UUID.randomUUID(), ClusterTagGenerator.generateTag());
        boolean defaultValueWasSet = setDefaultValue(clusterIdAndTagProperty, newIdAndTag, log);
        if (defaultValueWasSet) {
            if (log.isInfoEnabled())
                log.info("New cluster ID and tag have been generated: " + newIdAndTag);
        }
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void initDiagnosticListeners() throws IgniteCheckedException {
        ctx.event().addLocalEventListener(evt -> {
            assert evt instanceof DiscoveryEvent;
            assert evt.type() == EVT_NODE_FAILED || evt.type() == EVT_NODE_LEFT;

            DiscoveryEvent discoEvt = (DiscoveryEvent)evt;

            UUID nodeId = discoEvt.eventNode().id();

            ConcurrentHashMap<Long, InternalDiagnosticFuture> futs = diagnosticFutMap.get();

            if (futs != null) {
                for (InternalDiagnosticFuture fut : futs.values()) {
                    if (fut.nodeId.equals(nodeId))
                        fut.onDone(new IgniteDiagnosticInfo("Target node failed: " + nodeId));
                }
            }

            allNodesMetrics.remove(nodeId);
        }, EVT_NODE_FAILED, EVT_NODE_LEFT);

        ctx.io().addMessageListener(TOPIC_INTERNAL_DIAGNOSTIC, (nodeId, msg, plc) -> {
            if (msg instanceof IgniteDiagnosticMessage) {
                IgniteDiagnosticMessage msg0 = (IgniteDiagnosticMessage)msg;

                if (msg0.request()) {
                    ClusterNode node = ctx.discovery().node(nodeId);

                    if (node == null) {
                        if (diagnosticLog.isDebugEnabled()) {
                            diagnosticLog.debug("Skip diagnostic request, sender node left " +
                                "[node=" + nodeId + ", msg=" + msg + ']');
                        }

                        return;
                    }

                    byte[] diagRes;

                    IgniteClosure<GridKernalContext, IgniteDiagnosticInfo> c;

                    try {
                        c = msg0.unmarshal(marsh);

                        Objects.requireNonNull(c);

                        diagRes = marsh.marshal(c.apply(ctx));
                    }
                    catch (Exception e) {
                        U.error(diagnosticLog, "Failed to run diagnostic closure: " + e, e);

                        try {
                            IgniteDiagnosticInfo errInfo =
                                new IgniteDiagnosticInfo("Failed to run diagnostic closure: " + e);

                            diagRes = marsh.marshal(errInfo);
                        }
                        catch (Exception e0) {
                            U.error(diagnosticLog, "Failed to marshal diagnostic closure result: " + e, e);

                            diagRes = null;
                        }
                    }

                    IgniteDiagnosticMessage res = IgniteDiagnosticMessage.createResponse(diagRes, msg0.futureId());

                    try {
                        ctx.io().sendToGridTopic(node, TOPIC_INTERNAL_DIAGNOSTIC, res, GridIoPolicy.SYSTEM_POOL);
                    }
                    catch (ClusterTopologyCheckedException e) {
                        if (diagnosticLog.isDebugEnabled()) {
                            diagnosticLog.debug("Failed to send diagnostic response, node left " +
                                "[node=" + nodeId + ", msg=" + msg + ']');
                        }
                    }
                    catch (IgniteCheckedException e) {
                        U.error(diagnosticLog, "Failed to send diagnostic response [msg=" + msg0 + "]", e);
                    }
                }
                else {
                    InternalDiagnosticFuture fut = diagnosticFuturesMap().get(msg0.futureId());

                    if (fut != null) {
                        IgniteDiagnosticInfo res;

                        try {
                            res = msg0.unmarshal(marsh);

                            if (res == null)
                                res = new IgniteDiagnosticInfo("Remote node failed to marshal response.");
                        }
                        catch (Exception e) {
                            U.error(diagnosticLog, "Failed to unmarshal diagnostic response: " + e, e);

                            res = new IgniteDiagnosticInfo("Failed to unmarshal diagnostic response: " + e);
                        }

                        fut.onResponse(res);
                    }
                    else
                        U.warn(diagnosticLog, "Failed to find diagnostic message future [msg=" + msg0 + ']');
                }
            }
            else
                U.warn(diagnosticLog, "Received unexpected message: " + msg);
        });

        if (sndMetrics) {
            ctx.io().addMessageListener(TOPIC_METRICS, (nodeId, msg, plc) -> {
                if (msg instanceof ClusterMetricsUpdateMessage)
                    processMetricsUpdateMessage(nodeId, (ClusterMetricsUpdateMessage)msg);
                else
                    U.warn(log, "Received unexpected message for TOPIC_METRICS: " + msg);
            });
        }
    }

    /**
     * @return Logger for diagnostic category.
     */
    public IgniteLogger diagnosticLog() {
        return diagnosticLog;
    }

    /**
     * @return Cluster.
     */
    public IgniteClusterImpl get() {
        return cluster;
    }

    /**
     * @return Client reconnect future.
     */
    public IgniteFuture<?> clientReconnectFuture() {
        IgniteFuture<?> fut = cluster.clientReconnectFuture();

        return fut != null ? fut : new IgniteFinishedFutureImpl<>();
    }

    /** {@inheritDoc} */
    @Nullable @Override public DiscoveryDataExchangeType discoveryDataType() {
        return CLUSTER_PROC;
    }

    /** {@inheritDoc} */
    @Override public void collectJoiningNodeData(DiscoveryDataBag dataBag) {
        dataBag.addJoiningNodeData(CLUSTER_PROC.ordinal(), getDiscoveryData());
    }

    /** {@inheritDoc} */
    @Override public void collectGridNodeData(DiscoveryDataBag dataBag) {
        dataBag.addNodeSpecificData(CLUSTER_PROC.ordinal(), getDiscoveryData());

        if (!clusterIdAndTagSupport)
            return;

        // Left for compatibility with old versions that get cluster ID and tag from disco data
        // instead of from metastore.
        if (!compatibilityMode && !dataBag.isJoiningNodeClient())
            dataBag.addGridCommonData(CLUSTER_PROC.ordinal(), clusterIdAndTagProperty.get());
    }

    /**
     * @return Discovery data.
     */
    private Serializable getDiscoveryData() {
        HashMap<String, Object> map = new HashMap<>(2);

        map.put(ATTR_UPDATE_NOTIFIER_STATUS, notifyEnabled.get());

        return map;
    }

    /** {@inheritDoc} */
    @Override public void onGridDataReceived(GridDiscoveryData data) {
        Map<UUID, Serializable> nodeSpecData = data.nodeSpecificData();

        if (nodeSpecData != null) {
            Boolean lstFlag = findLastFlag(nodeSpecData.values());

            if (lstFlag != null)
                notifyEnabled.set(lstFlag);
        }
    }

    /**
     * @param vals collection to seek through.
     */
    private Boolean findLastFlag(Collection<Serializable> vals) {
        Boolean flag = null;

        for (Serializable ser : vals) {
            if (ser != null) {
                @SuppressWarnings("unchecked") Map<String, Object> map = (Map<String, Object>)ser;

                if (map.containsKey(ATTR_UPDATE_NOTIFIER_STATUS))
                    flag = (Boolean)map.get(ATTR_UPDATE_NOTIFIER_STATUS);
            }
        }

        return flag;
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart(boolean active) throws IgniteCheckedException {
        if (notifyEnabled.get()) {
            try {
                verChecker = new GridUpdateNotifier(ctx.igniteInstanceName(),
                    VER_STR,
                    new GridKernalGatewayImpl(ctx.igniteInstanceName()),
                    ctx.discovery(),
                    U.allPluginProviders(),
                    false,
                    new HttpIgniteUpdatesChecker(updateNotifierUrl.get(), GridUpdateNotifier.CHARSET));

                updateNtfTimer = new Timer("ignite-update-notifier-timer", true);

                // Setup periodic version check.
                updateNtfTimer.scheduleAtFixedRate(
                    new UpdateNotifierTimerTask((IgniteKernal)ctx.grid(), verChecker, notifyEnabled),
                    0, PERIODIC_VER_CHECK_DELAY);
            }
            catch (IgniteCheckedException e) {
                if (log.isDebugEnabled())
                    log.debug("Failed to create GridUpdateNotifier: " + e);
            }
        }

        if (sndMetrics) {
            metricsProvider = ctx.discovery().createMetricsProvider();

            long updateFreq = ctx.config().getMetricsUpdateFrequency();

            ctx.timeout().addTimeoutObject(new MetricsUpdateTimeoutObject(updateFreq));
        }

        if (!clusterIdAndTagSupport)
            return;

        IgniteClusterMXBeanImpl mxBeanImpl = new IgniteClusterMXBeanImpl(cluster);

        if (!U.IGNITE_MBEANS_DISABLED) {
            try {
                mBean = U.registerMBean(
                    ctx.config().getMBeanServer(),
                    ctx.igniteInstanceName(),
                    M_BEAN_NAME,
                    mxBeanImpl.getClass().getSimpleName(),
                    mxBeanImpl,
                    IgniteClusterMXBean.class);

                if (log.isDebugEnabled())
                    log.debug("Registered " + M_BEAN_NAME + " MBean: " + mBean);
            }
            catch (Throwable e) {
                U.error(log, "Failed to register MBean for cluster: ", e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        unregisterMBean();
    }

    /**
     * Unregister IgniteCluster MBean.
     */
    private void unregisterMBean() {
        ObjectName mBeanName = mBean;

        if (mBeanName == null)
            return;

        assert !U.IGNITE_MBEANS_DISABLED;

        try {
            ctx.config().getMBeanServer().unregisterMBean(mBeanName);

            mBean = null;

            if (log.isDebugEnabled())
                log.debug("Unregistered " + M_BEAN_NAME + " MBean: " + mBeanName);
        }
        catch (JMException e) {
            U.error(log, "Failed to unregister " + M_BEAN_NAME + " MBean: " + mBeanName, e);
        }
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        // Cancel update notification timer.
        if (updateNtfTimer != null)
            updateNtfTimer.cancel();

        if (verChecker != null)
            verChecker.stop();

        // Io manager can be null, if invoke stop before create io manager, for example
        // exception on start.
        if (ctx.io() != null)
            ctx.io().removeMessageListener(TOPIC_INTERNAL_DIAGNOSTIC);
    }

    /**  Registers cluster metrics. */
    public void registerMetrics() {
        MetricRegistry reg = ctx.metric().registry(CLUSTER_METRICS);

        reg.register(TOTAL_SERVER_NODES,
            () -> ctx.isStopping() || ctx.clientDisconnected() ? -1 : cluster.forServers().nodes().size(),
            "Server nodes count.");

        reg.register(TOTAL_CLIENT_NODES,
            () -> ctx.isStopping() || ctx.clientDisconnected() ? -1 : cluster.forClients().nodes().size(),
            "Client nodes count.");

        reg.register(TOTAL_BASELINE_NODES,
            () -> ctx.isStopping() || ctx.clientDisconnected() ? -1 : F.size(cluster.currentBaselineTopology()),
            "Total baseline nodes count.");

        reg.register(ACTIVE_BASELINE_NODES, () -> {
            if (ctx.isStopping() || ctx.clientDisconnected())
                return -1;

            Collection<Object> srvIds = F.nodeConsistentIds(cluster.forServers().nodes());

            return F.size(cluster.currentBaselineTopology(), node -> srvIds.contains(node.consistentId()));
        }, "Active baseline nodes count.");
    }

    /**
     * @param sndNodeId Sender node ID.
     * @param msg Message.
     */
    private void processMetricsUpdateMessage(UUID sndNodeId, ClusterMetricsUpdateMessage msg) {
        byte[] nodeMetrics = msg.nodeMetrics();

        if (nodeMetrics != null) {
            assert msg.allNodesMetrics() == null;

            allNodesMetrics.put(sndNodeId, nodeMetrics);

            updateNodeMetrics(ctx.discovery().discoCache(), sndNodeId, nodeMetrics);
        }
        else {
            Map<UUID, byte[]> allNodesMetrics = msg.allNodesMetrics();

            assert allNodesMetrics != null;

            DiscoCache discoCache = ctx.discovery().discoCache();

            for (Map.Entry<UUID, byte[]> e : allNodesMetrics.entrySet()) {
                if (!ctx.localNodeId().equals(e.getKey()))
                    updateNodeMetrics(discoCache, e.getKey(), e.getValue());
            }
        }
    }

    /**
     * @param discoCache Discovery data cache.
     * @param nodeId Node ID.
     * @param metricsBytes Marshalled metrics.
     */
    private void updateNodeMetrics(DiscoCache discoCache, UUID nodeId, byte[] metricsBytes) {
        ClusterNode node = discoCache.node(nodeId);

        if (node == null || !discoCache.alive(nodeId))
            return;

        try {
            ClusterNodeMetrics metrics = U.unmarshalZip(ctx.config().getMarshaller(), metricsBytes, null);

            assert node instanceof IgniteClusterNode : node;

            IgniteClusterNode node0 = (IgniteClusterNode)node;

            node0.setMetrics(ClusterMetricsSnapshot.deserialize(metrics.metrics(), 0));
            node0.setCacheMetrics(metrics.cacheMetrics());

            ctx.discovery().metricsUpdateEvent(discoCache, node0);
        }
        catch (IgniteCheckedException e) {
            U.warn(log, "Failed to unmarshal node metrics: " + e);
        }
    }

    /**
     *
     */
    private void updateMetrics() {
        if (ctx.isStopping() || ctx.clientDisconnected())
            return;

        ClusterNode oldest = ctx.discovery().oldestAliveServerNode(AffinityTopologyVersion.NONE);

        if (oldest == null)
            return;

        if (ctx.localNodeId().equals(oldest.id())) {
            IgniteClusterNode locNode = (IgniteClusterNode)ctx.discovery().localNode();

            locNode.setMetrics(metricsProvider.metrics());
            locNode.setCacheMetrics(metricsProvider.cacheMetrics());

            ClusterNodeMetrics metrics = new ClusterNodeMetrics(locNode.metrics(), locNode.cacheMetrics());

            try {
                byte[] metricsBytes = U.zip(U.marshal(ctx.config().getMarshaller(), metrics));

                allNodesMetrics.put(ctx.localNodeId(), metricsBytes);
            }
            catch (IgniteCheckedException e) {
                U.warn(log, "Failed to marshal local node metrics: " + e, e);
            }

            ctx.discovery().metricsUpdateEvent(ctx.discovery().discoCache(), locNode);

            Collection<ClusterNode> allNodes = ctx.discovery().allNodes();

            ClusterMetricsUpdateMessage msg = new ClusterMetricsUpdateMessage(new HashMap<>(allNodesMetrics));

            for (ClusterNode node : allNodes) {
                if (ctx.localNodeId().equals(node.id()) || !ctx.discovery().alive(node.id()))
                    continue;

                try {
                    ctx.io().sendToGridTopic(node, TOPIC_METRICS, msg, GridIoPolicy.SYSTEM_POOL);
                }
                catch (ClusterTopologyCheckedException e) {
                    if (log.isDebugEnabled())
                        log.debug("Failed to send metrics update, node failed: " + e);
                }
                catch (IgniteCheckedException e) {
                    U.warn(log, "Failed to send metrics update: " + e, e);
                }
            }
        }
        else {
            ClusterNodeMetrics metrics = new ClusterNodeMetrics(metricsProvider.metrics(), metricsProvider.cacheMetrics());

            try {
                byte[] metricsBytes = U.zip(U.marshal(ctx.config().getMarshaller(), metrics));

                ClusterMetricsUpdateMessage msg = new ClusterMetricsUpdateMessage(metricsBytes);

                ctx.io().sendToGridTopic(oldest, TOPIC_METRICS, msg, GridIoPolicy.SYSTEM_POOL);
            }
            catch (ClusterTopologyCheckedException e) {
                if (log.isDebugEnabled())
                    log.debug("Failed to send metrics update to oldest, node failed: " + e);
            }
            catch (IgniteCheckedException e) {
                LT.warn(log, e, "Failed to send metrics update to oldest: " + e, false, false);
            }
        }
    }

    /**
     * Disables update notifier.
     */
    public void disableUpdateNotifier() {
        notifyEnabled.set(false);
    }

    /**
     * @return Update notifier status.
     */
    public boolean updateNotifierEnabled() {
        return notifyEnabled.get();
    }

    /**
     * @return Latest version string.
     */
    public String latestVersion() {
        return verChecker != null ? verChecker.latestVersion() : null;
    }

    /**
     * Get cluster name.
     *
     * @return Cluster name.
     * */
    public String clusterName() {
        try {
            ctx.cache().awaitStarted();
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }

        return IgniteSystemProperties.getString(
            IGNITE_CLUSTER_NAME,
            ctx.cache().utilityCache().context().dynamicDeploymentId().toString()
        );
    }

    /**
     * Sends diagnostic message closure to remote node. When response received dumps remote message and local
     * communication info about connection(s) with remote node.
     *
     * @param nodeId Target node ID.
     * @param c Closure to send.
     * @param baseMsg Local message to log.
     * @return Message future.
     */
    public IgniteInternalFuture<String> requestDiagnosticInfo(final UUID nodeId,
        IgniteClosure<GridKernalContext, IgniteDiagnosticInfo> c,
        final String baseMsg) {
        final GridFutureAdapter<String> infoFut = new GridFutureAdapter<>();

        final IgniteInternalFuture<IgniteDiagnosticInfo> rmtFut = sendDiagnosticMessage(nodeId, c);

        rmtFut.listen((CI1<IgniteInternalFuture<IgniteDiagnosticInfo>>) fut -> {
            String rmtMsg;

            try {
                rmtMsg = fut.get().message();
            }
            catch (Exception e) {
                rmtMsg = "Diagnostic processing error: " + e;
            }

            final String rmtMsg0 = rmtMsg;

            IgniteInternalFuture<String> locFut = IgniteDiagnosticMessage.dumpCommunicationInfo(ctx, nodeId);

            locFut.listen(new CI1<IgniteInternalFuture<String>>() {
                @Override public void apply(IgniteInternalFuture<String> locFut) {
                    String locMsg;

                    try {
                        locMsg = locFut.get();
                    }
                    catch (Exception e) {
                        locMsg = "Failed to get info for local node: " + e;
                    }

                    String msg = baseMsg + U.nl() +
                        "Remote node information:" + U.nl() + rmtMsg0 +
                        U.nl() + "Local communication statistics:" + U.nl() +
                        locMsg;

                    infoFut.onDone(msg);
                }
            });
        });

        return infoFut;
    }

    /**
     * @param nodeId Target node ID.
     * @param c Message closure.
     * @return Message future.
     */
    private IgniteInternalFuture<IgniteDiagnosticInfo> sendDiagnosticMessage(UUID nodeId,
        IgniteClosure<GridKernalContext, IgniteDiagnosticInfo> c) {
        try {
            IgniteDiagnosticMessage msg = IgniteDiagnosticMessage.createRequest(marsh,
                c,
                diagFutId.getAndIncrement());

            InternalDiagnosticFuture fut = new InternalDiagnosticFuture(nodeId, msg.futureId());

            diagnosticFuturesMap().put(msg.futureId(), fut);

            ctx.io().sendToGridTopic(nodeId, TOPIC_INTERNAL_DIAGNOSTIC, msg, GridIoPolicy.SYSTEM_POOL);

            return fut;
        }
        catch (Exception e) {
            U.error(diagnosticLog, "Failed to send diagnostic message: " + e);

            return new GridFinishedFuture<>(new IgniteDiagnosticInfo("Failed to send diagnostic message: " + e));
        }
    }

    /**
     * @return Diagnostic messages futures map.
     */
    private ConcurrentHashMap<Long, InternalDiagnosticFuture> diagnosticFuturesMap() {
        ConcurrentHashMap<Long, InternalDiagnosticFuture> map = diagnosticFutMap.get();

        if (map == null) {
            if (!diagnosticFutMap.compareAndSet(null, map = new ConcurrentHashMap<>()))
                map = diagnosticFutMap.get();
        }

        return map;
    }

    /**
     * Update notifier timer task.
     */
    private static class UpdateNotifierTimerTask extends GridTimerTask {
        /** Logger. */
        private final IgniteLogger log;

        /** Version checker. */
        private final GridUpdateNotifier verChecker;

        /** Whether this is the first run. */
        private boolean first = true;

        /** */
        private final AtomicBoolean notifyEnabled;

        /**
         * Constructor.
         *
         * @param kernal Kernal.
         * @param verChecker Version checker.
         */
        private UpdateNotifierTimerTask(
            IgniteKernal kernal,
            GridUpdateNotifier verChecker,
            AtomicBoolean notifyEnabled
        ) {
            log = kernal.context().log(UpdateNotifierTimerTask.class);

            this.verChecker = verChecker;
            this.notifyEnabled = notifyEnabled;
        }

        /** {@inheritDoc} */
        @Override public void safeRun() throws InterruptedException {
            if (!notifyEnabled.get())
                return;

            verChecker.checkForNewVersion(log);

            // Just wait for 10 secs.
            Thread.sleep(PERIODIC_VER_CHECK_CONN_TIMEOUT);

            // Just wait another 60 secs in order to get
            // version info even on slow connection.
            for (int i = 0; i < 60 && verChecker.latestVersion() == null; i++)
                Thread.sleep(1000);

            // Report status if one is available.
            // No-op if status is NOT available.
            verChecker.reportStatus(log);

            if (first && verChecker.error() == null) {
                first = false;

                verChecker.reportOnlyNew(true);
            }
        }
    }

    /**
     *
     */
    class InternalDiagnosticFuture extends GridFutureAdapter<IgniteDiagnosticInfo> {
        /** */
        private final long id;

        /** */
        private final UUID nodeId;

        /**
         * @param nodeId Target node ID.
         * @param id Future ID.
         */
        InternalDiagnosticFuture(UUID nodeId, long id) {
            this.nodeId = nodeId;
            this.id = id;
        }

        /**
         * @param res Response.
         */
        public void onResponse(IgniteDiagnosticInfo res) {
            onDone(res);
        }

        /** {@inheritDoc} */
        @Override public boolean onDone(@Nullable IgniteDiagnosticInfo res, @Nullable Throwable err) {
            if (super.onDone(res, err)) {
                diagnosticFuturesMap().remove(id);

                return true;
            }

            return false;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(InternalDiagnosticFuture.class, this);
        }
    }

    /**
     *
     */
    private class MetricsUpdateTimeoutObject implements GridTimeoutObject, Runnable {
        /** */
        private final IgniteUuid id = IgniteUuid.randomUuid();

        /** */
        private long endTime;

        /** */
        private final long timeout;

        /**
         * @param timeout Timeout.
         */
        MetricsUpdateTimeoutObject(long timeout) {
            this.timeout = timeout;

            endTime = U.currentTimeMillis() + timeout;
        }

        /** {@inheritDoc} */
        @Override public IgniteUuid timeoutId() {
            return id;
        }

        /** {@inheritDoc} */
        @Override public long endTime() {
            return endTime;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            updateMetrics();

            endTime = U.currentTimeMillis() + timeout;

            ctx.timeout().addTimeoutObject(this);
        }

        /** {@inheritDoc} */
        @Override public void onTimeout() {
            ctx.pools().getSystemExecutorService().execute(this);
        }
    }
}
