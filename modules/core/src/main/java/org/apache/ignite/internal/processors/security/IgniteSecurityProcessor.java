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

package org.apache.ignite.internal.processors.security;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteFeatures;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.GridProcessor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.marshaller.MarshallerUtils;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.plugin.security.AuthenticationContext;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.plugin.security.SecurityPermissionSet;
import org.apache.ignite.plugin.security.SecuritySubject;
import org.apache.ignite.plugin.security.SecuritySubjectType;
import org.apache.ignite.spi.IgniteNodeValidationResult;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.communication.tcp.internal.CommunicationTcpUtils;
import org.apache.ignite.spi.discovery.DiscoveryDataBag;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.processors.security.SecurityUtils.nodeSecurityContext;

/**
 * Default {@code IgniteSecurity} implementation.
 * <p>
 * {@code IgniteSecurityProcessor} serves here as a facade with is exposed to Ignite internal code,
 * while {@code GridSecurityProcessor} is hidden and managed from {@code IgniteSecurityProcessor}.
 * <p>
 * This implementation of {@code IgniteSecurity} is responsible for:
 * <ul>
 *     <li>Keeping and propagating authenticated security contexts for cluster nodes;</li>
 *     <li>Delegating calls for all actions to {@code GridSecurityProcessor};</li>
 *     <li>Managing sandbox and proving point of entry to the internal sandbox API.</li>
 * </ul>
 */
public class IgniteSecurityProcessor implements IgniteSecurity, GridProcessor {
    /** Internal attribute name constant. */
    public static final String ATTR_GRID_SEC_PROC_CLASS = "grid.security.processor.class";

    /** Enable forcible node kill. */
    private boolean forcibleNodeKillEnabled = IgniteSystemProperties
        .getBoolean(IgniteSystemProperties.IGNITE_ENABLE_FORCIBLE_NODE_KILL);

    private boolean checkSenderNodeSubject = IgniteSystemProperties
        .getBoolean(IgniteSystemProperties.IGNITE_CHECK_SENDER_NODE_SUBJECT);

    /** Grid kernal context. */
    private final GridKernalContext ctx;

    /** Security processor. */
    private final GridSecurityProcessor secPrc;

    /** Must use JDK marshaller for Security Subject. */
    private final JdkMarshaller marsh;

    /** Map of security contexts. Key is the node's id. */
    private final Map<UUID, SecurityContext> secCtxs = new ConcurrentHashMap<>();

    /** Logger. */
    private final IgniteLogger log;

    /** Current security context. */
    private volatile ThreadLocal<SecurityContext> curSecCtx = ThreadLocal.withInitial(this::localSecurityContext);

    /**
     * @param ctx Grid kernal context.
     * @param secPrc Security processor.
     */
    public IgniteSecurityProcessor(GridKernalContext ctx, GridSecurityProcessor secPrc) {
        assert ctx != null;
        assert secPrc != null;

        this.ctx = ctx;
        this.log = ctx.log(IgniteSecurityProcessor.class);
        this.secPrc = secPrc;

        marsh = MarshallerUtils.jdkMarshaller(ctx.igniteInstanceName());
    }

    /** {@inheritDoc} */
    @Override public OperationSecurityContext withContext(SecurityContext secCtx) {
        assert secCtx != null;

        secPrc.touch(secCtx);

        SecurityContext old = curSecCtx.get();

        curSecCtx.set(secCtx);

        return new OperationSecurityContext(this, old);
    }

    @Override public OperationSecurityContext withContext(UUID nodeId) {
        return withContext(nodeId, nodeId);
    }

    /** {@inheritDoc} */
    @Override public OperationSecurityContext withContext(UUID senderNodeId, UUID subjId) {
        ClusterNode node = Optional.ofNullable(ctx.discovery().node(subjId))
            .orElseGet(() -> ctx.discovery().historicalNode(subjId));

        SecurityContext res = null;

        if (!senderNodeId.equals(subjId)) {
            ClusterNode senderNode = Optional.ofNullable(ctx.discovery().node(senderNodeId))
                .orElseGet(() -> ctx.discovery().historicalNode(senderNodeId));

            if (checkSenderNodeSubject && (senderNode == null || senderNode.isClient())) {
                SecuritySubjectType type = node != null ? SecuritySubjectType.REMOTE_NODE : SecuritySubjectType.REMOTE_CLIENT;

                log.warning("Switched to the 'deny all' policy because a client node tries to execute a request on behalf of another node " +
                    "[senderNodeId=" + senderNodeId + ", subjId=" + subjId + ", type=" + type + ']');

                if (forcibleNodeKillEnabled && senderNode != null && ctx.config().getCommunicationSpi() instanceof TcpCommunicationSpi) {
                    TcpCommunicationSpi tcpCommSpi = (TcpCommunicationSpi)ctx.config().getCommunicationSpi();

                    SecurityException ex = new SecurityException("Client node tried to execute a request on behalf of another node.");

                    CommunicationTcpUtils.failNode(senderNode, tcpCommSpi.getSpiContext(), ex, log);

                    String warn = "The client will be excluded from the topology since it tried to execute a non-secure operation [nodeId=" + senderNodeId + ']';

                    log.warning(warn);

                    //TODO: This exception is required until GG-33733 is implemented.
                    throw new SecurityException(warn);
                }

                res = new DenyAllSecurityContext(senderNodeId, type);
            }
        }

        if (res == null) {
            res = node != null ? secCtxs.computeIfAbsent(subjId,
                uuid -> nodeSecurityContext(marsh, U.resolveClassLoader(ctx.config()), node))
                : secPrc.securityContext(subjId);
        }

        if (res == null) {
            SecuritySubjectType type = node != null ? SecuritySubjectType.REMOTE_NODE : SecuritySubjectType.REMOTE_CLIENT;

            log.warning("Switched to the 'deny all' policy because of failing to find a security context " +
                "[subjId=" + subjId + ", type=" + type + ']');

            res = new DenyAllSecurityContext(subjId, type);
        }

        return withContext(res);
    }

    /** {@inheritDoc} */
    @Override public SecurityContext securityContext() {
        SecurityContext res = curSecCtx.get();

        assert res != null;

        return res;
    }

    /** {@inheritDoc} */
    @Override public SecurityContext authenticateNode(ClusterNode node, SecurityCredentials cred)
        throws IgniteCheckedException {
        return secPrc.authenticateNode(node, cred);
    }

    /** {@inheritDoc} */
    @Override public boolean isGlobalNodeAuthentication() {
        return secPrc.isGlobalNodeAuthentication();
    }

    /** {@inheritDoc} */
    @Override public SecurityContext authenticate(AuthenticationContext ctx) throws IgniteCheckedException {
        return secPrc.authenticate(ctx);
    }

    /** {@inheritDoc} */
    @Override public Collection<SecuritySubject> authenticatedSubjects() throws IgniteCheckedException {
        return secPrc.authenticatedSubjects();
    }

    /** {@inheritDoc} */
    @Override public SecuritySubject authenticatedSubject(UUID subjId) throws IgniteCheckedException {
        return secPrc.authenticatedSubject(subjId);
    }

    /** {@inheritDoc} */
    @Override public void onSessionExpired(UUID subjId) {
        secPrc.onSessionExpired(subjId);
    }

    /** {@inheritDoc} */
    @Override public void authorize(String name, SecurityPermission perm) throws SecurityException {
        SecurityContext secCtx = curSecCtx.get();

        assert secCtx != null;

        secPrc.authorize(name, perm, secCtx);
    }

    /** {@inheritDoc} */
    @Override public boolean enabled() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        ctx.addNodeAttribute(ATTR_GRID_SEC_PROC_CLASS, secPrc.getClass().getName());

        secPrc.start();
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        secPrc.stop(cancel);
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart(boolean active) throws IgniteCheckedException {
        ctx.event().addDiscoveryEventListener(
            (evt, discoCache) -> secCtxs.remove(evt.eventNode().id()), EVT_NODE_FAILED, EVT_NODE_LEFT
        );

        secPrc.onKernalStart(active);
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        secPrc.onKernalStop(cancel);
    }

    /** {@inheritDoc} */
    @Override public void collectJoiningNodeData(DiscoveryDataBag dataBag) {
        secPrc.collectJoiningNodeData(dataBag);
    }

    /** {@inheritDoc} */
    @Override public void collectGridNodeData(DiscoveryDataBag dataBag) {
        secPrc.collectGridNodeData(dataBag);
    }

    /** {@inheritDoc} */
    @Override public void onGridDataReceived(DiscoveryDataBag.GridDiscoveryData data) {
        secPrc.onGridDataReceived(data);
    }

    /** {@inheritDoc} */
    @Override public void onJoiningNodeDataReceived(DiscoveryDataBag.JoiningNodeDiscoveryData data) {
        secPrc.onJoiningNodeDataReceived(data);
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        secPrc.printMemoryStats();
    }

    /** {@inheritDoc} */
    @Override public @Nullable IgniteNodeValidationResult validateNode(ClusterNode node) {
        IgniteNodeValidationResult res = validateSecProcClass(node);

        return res != null ? res : secPrc.validateNode(node);
    }

    /** {@inheritDoc} */
    @Override public @Nullable IgniteNodeValidationResult validateNode(ClusterNode node,
        DiscoveryDataBag.JoiningNodeDiscoveryData discoData) {
        IgniteNodeValidationResult res = validateSecProcClass(node);

        return res != null ? res : secPrc.validateNode(node, discoData);
    }

    /** {@inheritDoc} */
    @Override public @Nullable DiscoveryDataExchangeType discoveryDataType() {
        return secPrc.discoveryDataType();
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(IgniteFuture<?> reconnectFut) throws IgniteCheckedException {
        secPrc.onDisconnected(reconnectFut);
    }

    /** {@inheritDoc} */
    @Override public @Nullable IgniteInternalFuture<?> onReconnected(
        boolean clusterRestarted) throws IgniteCheckedException {
        curSecCtx = ThreadLocal.withInitial(this::localSecurityContext);

        return secPrc.onReconnected(clusterRestarted);
    }

    /**
     * Getting local node's security context.
     *
     * @return Security context of local node.
     */
    private SecurityContext localSecurityContext() {
        return nodeSecurityContext(marsh, U.resolveClassLoader(ctx.config()), ctx.discovery().localNode());
    }

    /**
     * Validates that remote node's grid security processor class is the same as local one.
     *
     * @param node Joining node.
     * @return Validation result or {@code null} in case of success.
     */
    private IgniteNodeValidationResult validateSecProcClass(ClusterNode node) {
        String rmtCls = node.attribute(ATTR_GRID_SEC_PROC_CLASS);
        String locCls = secPrc.getClass().getName();

        boolean securityMsgSupported = IgniteFeatures.allNodesSupports(
            ctx,
            ctx.discovery().allNodes(),
            IgniteFeatures.IGNITE_SECURITY_PROCESSOR
        );

        if (securityMsgSupported && !F.eq(locCls, rmtCls)) {
            return new IgniteNodeValidationResult(node.id(),
                String.format(MSG_SEC_PROC_CLS_IS_INVALID, ctx.localNodeId(), node.id(), locCls, rmtCls),
                String.format(MSG_SEC_PROC_CLS_IS_INVALID, node.id(), ctx.localNodeId(), rmtCls, locCls));
        }

        return null;
    }

    /**
     * @return root security processor.
     */
    public GridSecurityProcessor gridSecurityProcessor() {
        return secPrc;
    }

    /**
     * Security context that deny all operation.
     */
    private static class DenyAllSecurityContext implements SecurityContext, Serializable {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** Security subject identifier. */
        private final SecuritySubject secSubj;

        /**
         * Creates a new security context for the given subject and type.
         *
         * @param subjId Security subject identifier.
         * @param subjType Subject type.
         */
        DenyAllSecurityContext(UUID subjId, SecuritySubjectType subjType) {
            secSubj = new DenyAllSecuritySubject(subjId, subjType);
        }

        /** {@inheritDoc} */
        @Override public SecuritySubject subject() {
            return secSubj;
        }

        /** {@inheritDoc} */
        @Override public boolean taskOperationAllowed(String taskClsName, SecurityPermission perm) {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean cacheOperationAllowed(String cacheName, SecurityPermission perm) {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean serviceOperationAllowed(String srvcName, SecurityPermission perm) {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean systemOperationAllowed(SecurityPermission perm) {
            return false;
        }
    }

    /**
     * Security subject with empty permission set.
     */
    private static class DenyAllSecuritySubject implements SecuritySubject {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** Subject identifier. */
        private final UUID subjId;

        /** Security subject type. */
        private final SecuritySubjectType subjType;

        /**
         * Creates a new security subject for the given subject id and type.
         *
         * @param subjId Subject identifier.
         * @param subjType Subject type.
         */
        DenyAllSecuritySubject(UUID subjId, SecuritySubjectType subjType) {
            this.subjId = subjId;
            this.subjType = subjType;
        }

        /** {@inheritDoc} */
        @Override public UUID id() {
            return subjId;
        }

        /** {@inheritDoc} */
        @Override public SecuritySubjectType type() {
            return subjType;
        }

        /** {@inheritDoc} */
        @Override public Object login() {
            return "";
        }

        /** {@inheritDoc} */
        @Override public InetSocketAddress address() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public SecurityPermissionSet permissions() {
            return new AllowNothingSecurityPermissionSet();
        }
    }

    /**
     * Empty permission set that deny all operations.
     */
    private static class AllowNothingSecurityPermissionSet implements SecurityPermissionSet {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public boolean defaultAllowAll() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public Map<String, Collection<SecurityPermission>> taskPermissions() {
            return Collections.emptyMap();
        }

        /** {@inheritDoc} */
        @Override public Map<String, Collection<SecurityPermission>> cachePermissions() {
            return Collections.emptyMap();
        }

        /** {@inheritDoc} */
        @Override public Map<String, Collection<SecurityPermission>> servicePermissions() {
            return Collections.emptyMap();
        }

        /** {@inheritDoc} */
        @Override public Collection<SecurityPermission> systemPermissions() {
            return Collections.emptyList();
        }
    }
}
