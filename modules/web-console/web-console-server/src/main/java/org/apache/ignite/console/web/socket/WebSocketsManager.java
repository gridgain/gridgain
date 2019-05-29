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

package org.apache.ignite.console.web.socket;

import java.io.IOException;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.PostConstruct;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.dto.Announcement;
import org.apache.ignite.console.json.JsonArray;
import org.apache.ignite.console.json.JsonObject;
import org.apache.ignite.console.repositories.AccountsRepository;
import org.apache.ignite.console.web.model.VisorTaskDescriptor;
import org.apache.ignite.console.websocket.AgentHandshakeRequest;
import org.apache.ignite.console.websocket.AgentHandshakeResponse;
import org.apache.ignite.console.websocket.TopologySnapshot;
import org.apache.ignite.console.websocket.WebSocketEvent;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jsr166.ConcurrentLinkedHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.stereotype.Service;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.PingMessage;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.ignite.console.json.JsonUtils.errorToJson;
import static org.apache.ignite.console.json.JsonUtils.fromJson;
import static org.apache.ignite.console.json.JsonUtils.toJson;
import static org.apache.ignite.console.websocket.WebSocketConsts.ADMIN_ANNOUNCEMENT;
import static org.apache.ignite.console.websocket.WebSocketConsts.AGENT_HANDSHAKE;
import static org.apache.ignite.console.websocket.WebSocketConsts.AGENT_REVOKE_TOKEN;
import static org.apache.ignite.console.websocket.WebSocketConsts.AGENT_STATUS;
import static org.apache.ignite.console.websocket.WebSocketConsts.CLUSTER_TOPOLOGY;
import static org.apache.ignite.console.websocket.WebSocketConsts.ERROR;
import static org.apache.ignite.console.websocket.WebSocketConsts.NODE_REST;
import static org.apache.ignite.console.websocket.WebSocketConsts.NODE_VISOR;
import static org.apache.ignite.console.websocket.WebSocketConsts.SCHEMA_IMPORT_DRIVERS;
import static org.apache.ignite.console.websocket.WebSocketConsts.SCHEMA_IMPORT_METADATA;
import static org.apache.ignite.console.websocket.WebSocketConsts.SCHEMA_IMPORT_SCHEMAS;

/**
 * Web sockets manager.
 */
@Service
public class WebSocketsManager {
    /** */
    private static final Logger log = LoggerFactory.getLogger(WebSocketsManager.class);

    /** */
    private static final String VISOR_IGNITE = "org.apache.ignite.internal.visor.";

    /** */
    private static final PingMessage PING = new PingMessage(UTF_8.encode("PING"));

    /** */
    protected final Map<WebSocketSession, AgentDescriptor> agents;

    /** */
    protected final Map<WebSocketSession, UUID> browsers;

    /** */
    private final Map<String, WebSocketSession> requests;

    /** */
    private final Map<String, TopologySnapshot> clusters;

    /** */
    private final Map<String, VisorTaskDescriptor> visorTasks;

    /** */
    private volatile Announcement lastAnn;

    /**
     * Default constructor.
     */
    public WebSocketsManager() {
        agents = new ConcurrentLinkedHashMap<>();
        browsers = new ConcurrentHashMap<>();
        clusters = new ConcurrentHashMap<>();
        requests = new ConcurrentHashMap<>();
        visorTasks = new ConcurrentHashMap<>();
    }

    /**
     * @param ws Session to close.
     */
    public void closeAgentSession(WebSocketSession ws) {
        AgentDescriptor desc = agents.remove(ws);

        updateClusterInBrowsers(desc.accIds);

        clusters.remove(desc.clusterId);
    }

    /**
     * @param ws Session to close.
     */
    public void closeBrowserSession(WebSocketSession ws) {
        browsers.remove(ws);
    }

    /**
     * @param ws Session to send message.
     * @param evt Event.
     * @throws IOException If failed to send message.
     */
    protected void sendMessage(WebSocketSession ws, WebSocketEvent evt) throws IOException {
        ws.sendMessage(new TextMessage(toJson(evt)));
    }

    /**
     * @param wsBrowser Session.
     * @param evt Event.
     * @param errMsg Error message.
     * @param err Error.
     */
    private void sendError(WebSocketSession wsBrowser, WebSocketEvent evt, String errMsg, Throwable err) {
        try {
            evt.setEventType(ERROR);
            evt.setPayload(errorToJson(errMsg, err));

            sendMessage(wsBrowser, evt);
        }
        catch (Throwable e) {
            log.error("Failed to send error message [session=" + wsBrowser + ", event=" + evt + "]", e);
        }
    }

    public void sendResponseToBrowser(WebSocketEvent evt) throws IOException {
        WebSocketSession browserWs = requests.remove(evt.getRequestId());

        if (browserWs != null)
            sendMessage(browserWs, evt);
        else
            log.warn("Failed to send event to browser: " + evt);
    }

    /**
     * Broadcast event to all connected agents.
     *
     * @param wsBrowser Browser session.
     * @param evt Event to send.
     */
    public void sendToAgent(WebSocketSession wsBrowser, WebSocketEvent evt) {
        try {
            UUID accId = browsers.get(wsBrowser);

            WebSocketSession wsAgent = agents
                .entrySet()
                .stream()
                .filter(e -> e.getValue().isActiveAccount(accId))
                .findFirst()
                .map(Map.Entry::getKey)
                .orElseThrow(() -> new IllegalStateException("Failed to find agent for account."));

            if (log.isDebugEnabled())
                log.debug("Found agent session [token=" + accId + ", session=" + wsAgent + ", event=" + evt + "]");

            if (NODE_VISOR.equals(evt.getEventType()))
                prepareNodeVisorParams(evt);

            requests.put(evt.getRequestId(), wsBrowser);

            sendMessage(wsAgent, evt);
        }
        catch (Throwable e) {
            String errMsg = "Failed to send event to agent: " + evt;

            log.error(errMsg, e);

            sendError(wsBrowser, evt, errMsg, e);
        }
    }

    /**
     * @param ws Session to ping.
     */
    private void ping(WebSocketSession ws) {
        try {
            if (ws.isOpen())
                ws.sendMessage(PING);
        }
        catch (Throwable e) {
            log.error("Failed to send PING request [session=" + ws + "]");

            try {
                ws.close(CloseStatus.SESSION_NOT_RELIABLE);
            }
            catch (IOException ignored) {
                // No-op.
            }
        }
    }

    /**
     * @param wsAgent Session.
     * @param oldTop Old topology.
     * @param newTop New topology.
     */
    protected void updateTopology(WebSocketSession wsAgent, TopologySnapshot oldTop, TopologySnapshot newTop) {
        AgentDescriptor desc = agents.get(wsAgent);

        if (!newTop.getId().equals(desc.clusterId)) {
            desc.clusterId = newTop.getId();

            updateClusterInBrowsers(desc.accIds);
        }
    }

    /**
     * @param wsAgent Session.
     * @param newTop Topology snapshot.
     */
    public void processTopologyUpdate(WebSocketSession wsAgent, TopologySnapshot newTop) {
        TopologySnapshot oldTop = null;

        AgentDescriptor desc = agents.get(wsAgent);

        if (desc.clusterId != null)
            oldTop = clusters.remove(desc.clusterId);

        if (F.isEmpty(newTop.getId())) {
            String clusterId = null;

            if (oldTop != null && !oldTop.differentCluster(newTop))
                clusterId = oldTop.getId();

            if (F.isEmpty(clusterId)) {
                clusterId = clusters.entrySet().stream()
                    .filter(e -> !e.getValue().differentCluster(newTop))
                    .map(Map.Entry::getKey)
                    .findFirst()
                    .orElse(null);
            }

            newTop.setId(F.isEmpty(clusterId) ? UUID.randomUUID().toString() : clusterId);
        }

        clusters.put(newTop.getId(), newTop);

        updateTopology(wsAgent, oldTop, newTop);
    }

    /**
     * @param wsBrowser Browser session.
     */
    public void onBrowserConnect(WebSocketSession wsBrowser) {
        Account acc = extractAccount(wsBrowser);

        browsers.put(wsBrowser, acc.getId());

        sendAgentStats(wsBrowser, acc.getId());

        sendAnnouncement(wsBrowser);
    }

    /**
     * Extract account from session.
     *
     * @param ws Websocket.
     * @return Account.
     */
    protected Account extractAccount(WebSocketSession ws) {
        Principal p = ws.getPrincipal();

        if (p instanceof UsernamePasswordAuthenticationToken) {
            UsernamePasswordAuthenticationToken t = (UsernamePasswordAuthenticationToken)p;

            Object tp = t.getPrincipal();

            if (tp instanceof Account)
                return (Account)tp;
        }

        throw new IllegalStateException("Account can't be found [session=" + ws + "]");
    }

    public void saveAgentSession(WebSocketSession ws, Set<UUID> accIds) {
        agents.put(ws, new AgentDescriptor(accIds));
    }

    /**
     * @param c Collection of Objects.
     * @param mapper Mapper.
     */
    private <T, R> Set<R> mapToSet(Collection<T> c, Function<? super T, ? extends R> mapper) {
        return c.stream().map(mapper).collect(Collectors.toSet());
    }

    /**
     * @param ann Announcement.
     */
    public void broadcastAnnouncement(Announcement ann) {
        try {
            lastAnn = ann;

            if (!browsers.isEmpty())
                sendAnnouncement(browsers.keySet(), ann);
        }
        catch (Throwable e) {
            log.error("Failed to broadcast announcement: " + ann, e);
        }
    }

    /**
     * @param wsBrowser Browser to send current announcement.
     */
    private void sendAnnouncement(WebSocketSession wsBrowser) {
        if (lastAnn != null)
            sendAnnouncement(Collections.singleton(wsBrowser), lastAnn);
    }

    /**
     * @param browsers Browsers to send announcement.
     * @param ann Announcement.
     */
    private void sendAnnouncement(Set<WebSocketSession> browsers, Announcement ann) {
        WebSocketEvent evt = new WebSocketEvent(ADMIN_ANNOUNCEMENT, toJson(ann));

        for (WebSocketSession browserWs : browsers) {
            try {
                sendMessage(browserWs, evt);
            }
            catch (Throwable e) {
                log.error("Failed to send announcement to: " + browserWs, e);
            }
        }
    }

    /**
     * Send to browser info about agent status.
     */
    private void sendAgentStats(WebSocketSession wsBrowser, UUID accId) {
        List<TopologySnapshot> tops = new ArrayList<>();

        agents.forEach((wsAgent, desc) -> {
            if (desc.isActiveAccount(accId)) {
                TopologySnapshot top = clusters.get(desc.clusterId);

                if (top != null && tops.stream().allMatch(t -> t.differentCluster(top)))
                    tops.add(top);
            }
        });

        Map<String, Object> res = new LinkedHashMap<>();

        res.put("count", tops.size());
        res.put("hasDemo", false);
        res.put("clusters", tops);

        try {
            sendMessage(wsBrowser, new WebSocketEvent(AGENT_STATUS, toJson(res)));
        }
        catch (Throwable e) {
            log.error("Failed to update agent status [session=" + wsBrowser + ", token=" + accId + "]", e);
        }
    }

    /**
     * Send to all connected browsers info about agent status.
     */
    private void updateClusterInBrowsers(Set<UUID> accIds) {
        browsers.entrySet().stream()
            .filter(e -> accIds.contains(e.getValue()))
            .forEach((e) -> sendAgentStats(e.getKey(), e.getValue()));
    }

    /**
     * @param acc Account.
     * @param oldTok Token to revoke.
     */
    public void revokeToken(Account acc, String oldTok) {
        log.info("Revoke token [old: " + oldTok + ", new: " + acc.getToken() + "]");

        agents.forEach((ws, desc) -> {
            try {
                if (desc.revokeAccount(acc.getId()))
                    sendMessage(ws, new WebSocketEvent(AGENT_REVOKE_TOKEN, oldTok));

                if (desc.canBeClose())
                    ws.close();
            }
            catch (Throwable e) {
                log.error("Failed to revoke token: " + oldTok);
            }
        });
    }

    /**
     * Periodically ping connected clients to keep connections alive.
     */
    @Scheduled(fixedRate = 5000)
    public void pingClients() {
        agents.keySet().forEach(this::ping);
        browsers.keySet().forEach(this::ping);
    }

    /**
     * @param shortName Class short name.
     * @return Full class name.
     */
    protected String igniteVisor(String shortName) {
        return VISOR_IGNITE + shortName;
    }

    /**
     * @param taskId Task ID.
     * @param taskCls Task class name.
     * @param argCls Arguments classes names.
     */
    protected void registerVisorTask(String taskId, String taskCls, String... argCls) {
        visorTasks.put(taskId, new VisorTaskDescriptor(taskCls, argCls));
    }

    /**
     * Register Visor tasks.
     */
    @PostConstruct
    protected void registerVisorTasks() {
        registerVisorTask(
            "querySql",
            igniteVisor("query.VisorQueryTask"),
            igniteVisor("query.VisorQueryArg"));

        registerVisorTask("querySqlV2",
            igniteVisor("query.VisorQueryTask"),
            igniteVisor("query.VisorQueryArgV2"));

        registerVisorTask("querySqlV3",
            igniteVisor("query.VisorQueryTask"),
            igniteVisor("query.VisorQueryArgV3"));

        registerVisorTask("querySqlX2",
            igniteVisor("query.VisorQueryTask"),
            igniteVisor("query.VisorQueryTaskArg"));

        registerVisorTask("queryScanX2",
            igniteVisor("query.VisorScanQueryTask"),
            igniteVisor("query.VisorScanQueryTaskArg"));

        registerVisorTask("queryFetch",
            igniteVisor("query.VisorQueryNextPageTask"),
            IgniteBiTuple.class.getName(), String.class.getName(), Integer.class.getName());

        registerVisorTask("queryFetchX2",
            igniteVisor("query.VisorQueryNextPageTask"),
            igniteVisor("query.VisorQueryNextPageTaskArg"));

        registerVisorTask("queryFetchFirstPage",
            igniteVisor("query.VisorQueryFetchFirstPageTask"),
            igniteVisor("query.VisorQueryNextPageTaskArg"));

        registerVisorTask("queryClose",
            igniteVisor("query.VisorQueryCleanupTask"),
            Map.class.getName(), UUID.class.getName(), Set.class.getName());

        registerVisorTask("queryCloseX2",
            igniteVisor("query.VisorQueryCleanupTask"),
            igniteVisor("query.VisorQueryCleanupTaskArg"));

        registerVisorTask("toggleClusterState",
            igniteVisor("misc.VisorChangeGridActiveStateTask"),
            igniteVisor("misc.VisorChangeGridActiveStateTaskArg"));

        registerVisorTask("cacheNamesCollectorTask",
            igniteVisor("cache.VisorCacheNamesCollectorTask"),
            Void.class.getName());

        registerVisorTask("cacheNodesTask",
            igniteVisor("cache.VisorCacheNodesTask"),
            String.class.getName());

        registerVisorTask("cacheNodesTaskX2",
            igniteVisor("cache.VisorCacheNodesTask"),
            igniteVisor("cache.VisorCacheNodesTaskArg"));
    }

    /**
     * Prepare task event for execution on agent.
     *
     * @param evt Task event.
     */
    private void prepareNodeVisorParams(WebSocketEvent evt) {
        JsonObject payload = fromJson(evt.getPayload());

        JsonObject params = payload.getJsonObject("params");

        String taskId = params.getString("taskId");

        if (F.isEmpty(taskId))
            throw new IllegalStateException("Task ID not specified [evt=" + evt + "]");

        String nids = params.getString("nids");

        VisorTaskDescriptor desc = visorTasks.get(taskId);

        if (desc == null)
            throw new IllegalStateException("Unknown task  [taskId=" + taskId + ", evt=" + evt + "]");

        JsonObject exeParams =  new JsonObject()
            .add("cmd", "exe")
            .add("name", "org.apache.ignite.internal.visor.compute.VisorGatewayTask")
            .add("p1", nids)
            .add("p2", desc.getTaskClass());

        AtomicInteger idx = new AtomicInteger(3);

        Arrays.stream(desc.getArgumentsClasses()).forEach(arg ->  exeParams.put("p" + idx.getAndIncrement(), arg));

        JsonArray args = params.getJsonArray("args");

        if (!F.isEmpty(args))
            args.forEach(arg -> exeParams.put("p" + idx.getAndIncrement(), arg));

        payload.put("params", exeParams);

        evt.setPayload(toJson(payload));
    }

    /**
     * Agent descriptor.
     */
    protected static class AgentDescriptor {
        /** */
        private Set<UUID> accIds;

        /** */
        private String clusterId;

        /**
         * @param accIds Account IDs.
         */
        AgentDescriptor(Set<UUID> accIds) {
            this.accIds = accIds;
        }

        /**
         * @param accId Account ID.
         * @return {@code True} if contained the specified account.
         */
        boolean isActiveAccount(UUID accId) {
            return accIds.contains(accId);
        }

        /**
         * @param accId Account ID.
         * @return {@code True} if contained the specified account.
         */
        boolean revokeAccount(UUID accId) {
            return accIds.remove(accId);
        }

        /**
         * @return {@code True} if connection to agent can be closed.
         */
        boolean canBeClose() {
            return accIds.isEmpty();
        }

        /**
         * @return Acc ids.
         */
        public Set<UUID> getAccIds() {
            return accIds;
        }

        /**
         * @return Cluster id.
         */
        public String getClusterId() {
            return clusterId;
        }
    }
}
