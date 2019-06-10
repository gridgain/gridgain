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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
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
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.console.utils.Utils.toJson;
import static org.apache.ignite.console.websocket.WebSocketEvents.ADMIN_ANNOUNCEMENT;
import static org.apache.ignite.console.websocket.WebSocketEvents.AGENT_REVOKE_TOKEN;
import static org.apache.ignite.console.websocket.WebSocketEvents.AGENT_STATUS;
import static org.springframework.web.util.UriComponentsBuilder.fromUri;

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

            if (!F.isEmpty(desc.getClusterIds())) {
                for (String clusterId : desc.getClusterIds()) {
                    Optional<AgentDescriptor> conn = agents.values().stream()
                        .filter(agent -> agent.getClusterIds().contains(clusterId))
                        .findFirst();

                    if (!conn.isPresent())
                        clusters.remove(clusterId);
                }
            }

            if (F.isEmpty(desc.clusterIds)) 
                log.warn("Agent descriptor not found for session: " + ws);
        }
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
            .filter(e -> e.getValue().getClusterIds().contains(clusterId))
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

        updateClusterInBrowsers(desc.accIds);
    }

    /**
     * @param wsAgent Session.
     * @param tops Topology snapshots.
     */
    public void processTopologyUpdate(WebSocketSession wsAgent, Collection<TopologySnapshot> tops) {
        AgentDescriptor desc = agents.get(wsAgent);

        Set<TopologySnapshot> oldTops = desc.getClusterIds().stream().map(clusters::get).collect(toSet());

        for (TopologySnapshot newTop : tops) {
            String clusterId = newTop.getId();

            if (F.isEmpty(clusterId)) {
                clusterId = oldTops.stream()
                    .filter(t -> !t.differentCluster(newTop))
                    .map(TopologySnapshot::getId)
                    .findFirst()
                    .orElse(null);
            }

            if (F.isEmpty(clusterId)) {
                clusterId = clusters.entrySet().stream()
                    .filter(e -> !e.getValue().differentCluster(newTop))
                    .map(Map.Entry::getKey)
                    .findFirst()
                    .orElse(null);
            }

            newTop.setId(F.isEmpty(clusterId) ? UUID.randomUUID().toString() : clusterId);

            if (F.isEmpty(newTop.getName()))
                newTop.setName("Cluster " + newTop.getId().substring(0, 8).toUpperCase());

            TopologySnapshot oldTop = clusters.put(newTop.getId(), newTop);

            updateTopology(wsAgent, oldTop, newTop);
        }

        desc.setClusterIds(tops.stream().map(TopologySnapshot::getId).collect(Collectors.toSet()));

        // TODO GG-19218 Cleanup clusters.
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
        WebSocketEvent evt = new WebSocketEvent(ADMIN_ANNOUNCEMENT, ann);

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
        Map<String, TopologySnapshot> tops = new HashMap<>();

        agents.forEach((wsAgent, desc) -> {
            if (desc.isActiveAccount(accId)) {
                for (String clusterId : desc.clusterIds) {
                    if (!tops.containsKey(clusterId)) {
                        TopologySnapshot top = clusters.get(clusterId);

                        if (top != null)
                            tops.put(top.getId(), top);
                    }
                }
            }
        });

        boolean hasDemo = tops.values().stream().anyMatch(TopologySnapshot::isDemo);

        boolean isDemo = Boolean.parseBoolean(fromUri(ws.getUri()).build().getQueryParams().getFirst("demoMode"));

        Collection<TopologySnapshot> clusters = tops.values().stream()
            .filter(t -> t.isDemo() == isDemo)
            .collect(toList());

        Map<String, Object> res = new LinkedHashMap<>();

        res.put("count", clusters.size());
        res.put("hasDemo", hasDemo);
        res.put("clusters", clusters);

        try {
            sendMessage(ws, new WebSocketEvent(AGENT_STATUS, res));
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
        private final Set<UUID> accIds;

        /** */
        private Set<String> clusterIds = Collections.emptySet();

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
        public Set<String> getClusterIds() {
            return clusterIds;
        }

        /**
         * @param clusterIds Cluster ids.
         */
        public void setClusterIds(Set<String> clusterIds) {
            this.clusterIds = clusterIds;
        }
    }
}
