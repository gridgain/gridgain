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
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.dto.Announcement;
import org.apache.ignite.console.websocket.TopologySnapshot;
import org.apache.ignite.console.websocket.WebSocketEvent;
import org.apache.ignite.internal.util.typedef.F;
import org.jsr166.ConcurrentLinkedHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.PingMessage;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.ignite.console.json.JsonUtils.toJson;
import static org.apache.ignite.console.websocket.WebSocketConsts.ADMIN_ANNOUNCEMENT;
import static org.apache.ignite.console.websocket.WebSocketConsts.AGENT_REVOKE_TOKEN;
import static org.apache.ignite.console.websocket.WebSocketConsts.AGENT_STATUS;

/**
 * Web sockets manager.
 */
@Service
public class WebSocketsManager {
    /** */
    private static final Logger log = LoggerFactory.getLogger(WebSocketsManager.class);

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
    private volatile Announcement lastAnn;

    /**
     * Default constructor.
     */
    public WebSocketsManager() {
        agents = new ConcurrentLinkedHashMap<>();
        browsers = new ConcurrentHashMap<>();
        clusters = new ConcurrentHashMap<>();
        requests = new ConcurrentHashMap<>();
    }

    /**
     * @param ws Browser session.
     * @param accId Account Id.
     */
    public void onBrowserConnect(WebSocketSession ws, UUID accId) {
        browsers.put(ws, accId);

        if (lastAnn != null)
            sendAnnouncement(Collections.singleton(ws), lastAnn);

        sendAgentStats(ws, accId);
    }

    /**
     * @param ws Agent session.
     * @param accIds Account ids.
     */
    public void onAgentConnect(WebSocketSession ws, Set<UUID> accIds) {
        agents.put(ws, new AgentDescriptor(accIds));
    }

    /**
     * @param ws Session to close.
     */
    public void onAgentConnectionClosed(WebSocketSession ws) {
        AgentDescriptor desc = agents.remove(ws);

        if (desc != null) {
            updateClusterInBrowsers(desc.accIds);

            if (!F.isEmpty(desc.clusterId)) {
                Optional<AgentDescriptor> conn = agents.values().stream()
                    .filter(agent -> desc.getClusterId().equals(agent.getClusterId()))
                    .findFirst();

                if (!conn.isPresent())
                    clusters.remove(desc.clusterId);
            }
        }
        else
            log.warn("Agent descriptor not found for session: " + ws);
    }

    /**
     * @param ws Session to close.
     */
    public void onBrowserConnectionClosed(WebSocketSession ws) {
        browsers.remove(ws);
    }

    /**
     * @param evt Event.
     */
    public void sendResponseToBrowser(WebSocketEvent evt) throws IOException {
        WebSocketSession ws = requests.remove(evt.getRequestId());

        if (ws == null) {
            log.warn("Failed to send event to browser: " + evt);

            return;
        }

        sendMessage(ws, evt);
    }

    /**
     * Send event to first from connected agent.
     *
     * @param ws Browser session.
     * @param evt Event to send.
     */
    public void sendToFirstAgent(WebSocketSession ws, WebSocketEvent evt) throws IOException {
        UUID accId = browsers.get(ws);

        WebSocketSession wsAgent = agents.entrySet().stream()
            .filter(e -> e.getValue().isActiveAccount(accId))
            .findFirst()
            .map(Map.Entry::getKey)
            .orElseThrow(() -> new IllegalStateException("Failed to find agent for account: " + accId));

        if (log.isDebugEnabled())
            log.debug("Found agent session [accountId=" + accId + ", session=" + wsAgent + ", event=" + evt + "]");

        sendMessage(wsAgent, evt);

        requests.put(evt.getRequestId(), ws);
    }

    /**
     * Send event to first agent connected to specific cluster.
     * 
     * @param ws Ws.
     * @param clusterId Cluster id.
     * @param evt Event.
     */
    public void sendToNode(WebSocketSession ws, String clusterId, WebSocketEvent evt) throws IOException {
        UUID accId = browsers.get(ws);

        WebSocketSession wsAgent = agents.entrySet().stream()
            .filter(e -> e.getValue().isActiveAccount(accId))
            .filter(e -> clusterId.equals(e.getValue().getClusterId()))
            .findFirst()
            .map(Map.Entry::getKey)
            .orElseThrow(() -> new IllegalStateException("Failed to find agent for cluster [accountId=" + accId+", clusterId=" + clusterId + " ]"));

        if (log.isDebugEnabled())
            log.debug("Found agent session [accountId=" + accId + ", session=" + wsAgent + ", event=" + evt + "]");

        sendMessage(wsAgent, evt);

        requests.put(evt.getRequestId(), ws);
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
     * @param browsers Browsers to send announcement.
     * @param ann Announcement.
     */
    private void sendAnnouncement(Set<WebSocketSession> browsers, Announcement ann) {
        WebSocketEvent evt = new WebSocketEvent(ADMIN_ANNOUNCEMENT, toJson(ann));

        for (WebSocketSession ws : browsers) {
            try {
                sendMessage(ws, evt);
            }
            catch (Throwable e) {
                log.error("Failed to send announcement to: " + ws, e);
            }
        }
    }

    /**
     * Send to browser info about agent status.
     */
    private void sendAgentStats(WebSocketSession ws, UUID accId) {
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
            sendMessage(ws, new WebSocketEvent(AGENT_STATUS, toJson(res)));
        }
        catch (Throwable e) {
            log.error("Failed to update agent status [session=" + ws + ", token=" + accId + "]", e);
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
     * @param ws Session to send message.
     * @param evt Event.
     * @throws IOException If failed to send message.
     */
    protected void sendMessage(WebSocketSession ws, WebSocketEvent evt) throws IOException {
        ws.sendMessage(new TextMessage(toJson(evt)));
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
