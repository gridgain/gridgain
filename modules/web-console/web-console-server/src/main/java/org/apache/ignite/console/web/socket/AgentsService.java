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
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;
import com.fasterxml.jackson.core.type.TypeReference;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterGroupEmptyException;
import org.apache.ignite.console.db.CacheHolder;
import org.apache.ignite.console.db.OneToManyIndex;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.repositories.AccountsRepository;
import org.apache.ignite.console.tx.TransactionManager;
import org.apache.ignite.console.web.AbstractSocketHandler;
import org.apache.ignite.console.websocket.AgentHandshakeRequest;
import org.apache.ignite.console.websocket.AgentHandshakeResponse;
import org.apache.ignite.console.websocket.TopologySnapshot;
import org.apache.ignite.console.websocket.WebSocketEvent;
import org.apache.ignite.console.websocket.WebSocketRequest;
import org.apache.ignite.console.websocket.WebSocketResponse;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jsr166.ConcurrentLinkedHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.WebSocketSession;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.StreamSupport.stream;
import static org.apache.ignite.console.utils.Utils.entriesToMap;
import static org.apache.ignite.console.utils.Utils.entry;
import static org.apache.ignite.console.utils.Utils.fromJson;
import static org.apache.ignite.console.web.socket.TransitionService.SEND_RESPONSE;
import static org.apache.ignite.console.web.socket.TransitionService.SEND_TO_USER_BROWSER;
import static org.apache.ignite.console.websocket.AgentHandshakeRequest.SUPPORTED_VERS;
import static org.apache.ignite.console.websocket.WebSocketEvents.AGENT_HANDSHAKE;
import static org.apache.ignite.console.websocket.WebSocketEvents.AGENT_REVOKE_TOKEN;
import static org.apache.ignite.console.websocket.WebSocketEvents.AGENT_STATUS;
import static org.apache.ignite.console.websocket.WebSocketEvents.CLUSTER_TOPOLOGY;

/**
 * Agents web sockets handler.
 */
@Service
public class AgentsService extends AbstractSocketHandler {
    /** Default for cluster topology expire. */
    private static final long DEFAULT_CLUSTER_CLEANUP = MINUTES.toMillis(10);

    /** Default for cluster topology expire. */
    private static final long DEFAULT_MAX_INACTIVE_INTERVAL = SECONDS.toMillis(30);

    /** */
    private static final Logger log = LoggerFactory.getLogger(AgentsService.class);

    /** */
    protected AccountsRepository accRepo;

    /** */
    private CacheHolder<String, TopologySnapshot> clusters;

    /** */
    private OneToManyIndex<UserKey, ClusterSession> clusterIdsByBrowser;

    /** */
    private OneToManyIndex<AgentKey, UUID> backendByAgent;

    /** */
    private final Map<WebSocketSession, AgentSession> locAgents;
    
    /** */
    private final Map<String, UUID> srcOfRequests;

    /**
     * @param accRepo Repository to work with accounts.
     */
    public AgentsService(Ignite ignite, TransactionManager txMgr, AccountsRepository accRepo) {
        super(ignite, txMgr);

        this.accRepo = accRepo;

        this.txMgr.registerStarter(() -> {
            clusters = new CacheHolder<>(ignite, "wc_clusters");
            backendByAgent = new OneToManyIndex<>(ignite, "wc_backends");
            clusterIdsByBrowser = new OneToManyIndex<>(ignite, "wc_clusters_idx");

            cleanupBackendIndex();
            cleanupClusterIndex();
        });

        locAgents = new ConcurrentLinkedHashMap<>();
        srcOfRequests = new ConcurrentHashMap<>();
    }

    /** {@inheritDoc} */
    @Override public void handleEvent(WebSocketSession ses, WebSocketRequest evt) throws IOException {
        switch (evt.getEventType()) {
            case AGENT_HANDSHAKE:
                try {
                    AgentHandshakeRequest req = fromJson(evt.getPayload(), AgentHandshakeRequest.class);

                    validateAgentHandshake(req);

                    Collection<Account> accounts = loadAccounts(req.getTokens());

                    sendResponse(ses, evt, new AgentHandshakeResponse(mapToSet(accounts, Account::getToken)));

                    Set<UUID> accIds = mapToSet(accounts, Account::getId);

                    updateAgentsCache(ses, accIds);

                    sendAgentStats(accIds);

                    log.info("Agent connected: " + req);
                }
                catch (Exception e) {
                    log.warn("Failed to establish connection in handshake: " + evt, e);

                    sendResponse(ses, evt, new AgentHandshakeResponse(e));

                    ses.close();
                }

                break;

            case CLUSTER_TOPOLOGY:
                try {
                    Collection<TopologySnapshot> tops = fromJson(
                        evt.getPayload(),
                        new TypeReference<Collection<TopologySnapshot>>() {}
                    );

                    processTopologyUpdate(ses, tops);
                }
                catch (Exception e) {
                    log.warn("Failed to process topology update: " + evt, e);
                }

                break;

            default:
                try {
                    UUID nid = srcOfRequests.remove(evt.getRequestId());

                    ignite.message(ignite.cluster().forNodeId(nid)).send(SEND_RESPONSE, evt);
                }
                catch (ClusterGroupEmptyException ignored) {
                    // No-op.
                }
                catch (Exception e) {
                    log.warn("Failed to send response to browser: " + evt, e);
                }
        }
    }

    /**
     * @param wsAgent Session.
     * @param tops Topology snapshots.
     */
    private void processTopologyUpdate(WebSocketSession wsAgent, Collection<TopologySnapshot> tops) {
        AgentSession desc = locAgents.get(wsAgent);

        Set<TopologySnapshot> oldTops =
            txMgr.doInTransaction(() -> desc.getClusterIds().stream().map(clusters::get).collect(toSet()));

        boolean clustersChanged = oldTops.size() != tops.size();

        for (TopologySnapshot newTop : tops) {
            String clusterId = newTop.getId();

            if (F.isEmpty(clusterId)) {
                clusterId = oldTops.stream()
                    .filter(t -> t.sameNodes(newTop))
                    .map(TopologySnapshot::getId)
                    .findFirst()
                    .orElse(null);
            }

            if (F.isEmpty(clusterId)) {
                clusterId = txMgr.doInTransaction(() ->
                    stream(this.clusters.cache().spliterator(), false)
                        .filter(e -> e.getValue().sameNodes(newTop))
                        .map(Cache.Entry::getKey)
                        .findFirst()
                        .orElse(null)
                );
            }

            newTop.setId(F.isEmpty(clusterId) ? UUID.randomUUID().toString() : clusterId);

            if (F.isEmpty(newTop.getName()))
                newTop.setName("Cluster " + newTop.getId().substring(0, 8).toUpperCase());

            TopologySnapshot oldTop = updateTopology(desc.getAccIds(), newTop);

            clustersChanged = clustersChanged || newTop.changed(oldTop);
        }

        desc.setClusterIds(mapToSet(tops, TopologySnapshot::getId));

        Set<String> leftClusterIds = mapToSet(oldTops, TopologySnapshot::getId);

        leftClusterIds.removeAll(desc.getClusterIds());

        if (!leftClusterIds.isEmpty())
            tryCleanupIndexes(desc.getAccIds(), leftClusterIds);

        if (clustersChanged)
            sendAgentStats(desc.getAccIds());
    }

    /**
     * @param acc Account.
     * @param oldTok Token to revoke.
     */
    public void revokeToken(Account acc, String oldTok) {
        log.info("Revoke token for account with email: " + acc.getUsername());

        locAgents.forEach((ws, agentSes) -> {
            if (agentSes.revokeAccount(acc.getId())) {
                sendMessageQuiet(ws, new WebSocketResponse(AGENT_REVOKE_TOKEN, oldTok));

                if (agentSes.canBeClosed())
                    U.closeQuiet(ws);
            }
        });
        
        sendAgentStats(Collections.singleton(acc.getId()));
    }

    /** {@inheritDoc} */
    @Override public void afterConnectionEstablished(WebSocketSession ws) {
        log.info("Agent session opened [socket=" + ws + "]");

        ws.setTextMessageSizeLimit(10 * 1024 * 1024);
    }

    /** {@inheritDoc} */
    @Override public void afterConnectionClosed(WebSocketSession ses, CloseStatus status) {
        log.info("Agent session closed [socket=" + ses + ", status=" + status + "]");

        AgentSession agentSes = locAgents.remove(ses);

        if (agentSes == null) {
            log.warn("Closed session before handshake: " + ses);

            return;
        }

        tryCleanupIndexes(agentSes.getAccIds(), agentSes.getClusterIds());

        sendAgentStats(agentSes.getAccIds());
    }

    /**
     * Send request to locally connected agent.
     * @param req Request.
     * @throws IllegalStateException If connected agent not founded.
     */
    void sendLocally(AgentRequest req) throws IllegalStateException, IOException {
        WebSocketSession ses = findLocalAgent(req.getKey()).orElseThrow(IllegalStateException::new);

        WebSocketEvent evt = req.getEvent();

        if (log.isDebugEnabled())
            log.debug("Found local agent session [session=" + ses + ", event=" + evt + "]");

        sendMessage(ses, evt);

        srcOfRequests.put(evt.getRequestId(), req.getSrcNid());
    }

    /**
     * Periodically ping connected agent to keep connections alive or detect failed.
     */
    @Scheduled(fixedRate = 3_000)
    public void heartbeat() {
        locAgents.keySet().forEach(this::ping);
    }

    /**
     * Periodically cleanup agents without topology updates.
     */
    @Scheduled(initialDelay = 0, fixedRate = 15_000)
    private void disconnectFreezedAgents() {
        Set<String> clusterIds = txMgr.doInTransaction(() -> stream(this.clusters.cache().spliterator(), false)
            .filter(entry -> entry.getValue().isExpired(DEFAULT_MAX_INACTIVE_INTERVAL))
            .map(Cache.Entry::getKey)
            .collect(toSet()));

        locAgents.entrySet().stream()
            .filter(e -> !Collections.disjoint(clusterIds, e.getValue().getClusterIds()))
            .map(Map.Entry::getKey)
            .forEach(U::closeQuiet);
    }

    /**
     * Periodically cleanup expired cluster topology.
     */
    @Scheduled(initialDelay = 0, fixedRate = 60_000)
    private void cleanupClusterHistory() {
        txMgr.doInTransaction(() -> {
            Set<String> clusterIds = stream(this.clusters.cache().spliterator(), false)
                .filter(entry -> entry.getValue().isExpired(DEFAULT_CLUSTER_CLEANUP))
                .map(Cache.Entry::getKey)
                .collect(toSet());

            if (!F.isEmpty(clusterIds)) {
                clusters.cache().removeAll(clusterIds);

                log.error("Failed to receive topology update for clusters: " + clusterIds);
            }
        });
    }

    /**
     * Cleanup backend index.
     */
    void cleanupBackendIndex() {
        Collection<UUID> nids = U.nodeIds(ignite.cluster().nodes());

        stream(backendByAgent.cache().spliterator(), false)
            .peek(entry -> entry.getValue().removeAll(nids))
            .filter(entry -> entry.getValue().isEmpty())
            .forEach(entry -> backendByAgent.cache().remove(entry.getKey()));
    }

    /**
     * Cleanup cluster index.
     */
    void cleanupClusterIndex() {
        Collection<UUID> nids = U.nodeIds(ignite.cluster().nodes());

        stream(clusterIdsByBrowser.cache().spliterator(), false)
            .peek(entry -> {
                Set<ClusterSession> activeClusters =
                    entry.getValue().stream().filter(cluster -> nids.contains(cluster.getNid())).collect(toSet());

                entry.getValue().removeAll(activeClusters);
            })
            .filter(entry -> !entry.getValue().isEmpty())
            .forEach(entry -> clusterIdsByBrowser.removeAll(entry.getKey(), entry.getValue()));
    }

    /**
     * @param accIds Acc ids.
     * @param clusterIds Cluster ids.
     */
    private void tryCleanupIndexes(Set<UUID> accIds, Set<String> clusterIds) {
        UUID nid = ignite.cluster().localNode().id();

        for (UUID accId : accIds) {
            this.txMgr.doInTransaction(() -> {
                AgentKey agentKey = new AgentKey(accId);

                boolean allAgentsLeft = !findLocalAgent(agentKey).isPresent();

                if (allAgentsLeft)
                    backendByAgent.remove(agentKey, nid);

                for (String clusterId : clusterIds) {
                    AgentKey clusterKey = new AgentKey(accId, clusterId);

                    if (allAgentsLeft || !findLocalAgent(clusterKey).isPresent())
                        backendByAgent.remove(clusterKey, nid);
                }
            });
        }
    }

    /**
     * Send to browser info about agent status.
     */
    WebSocketResponse collectAgentStats(UserKey userKey) {
        Map<String, Object> res = txMgr.doInTransaction(() -> {
            boolean hasAgent = !backendByAgent.get(new AgentKey(userKey.getAccId())).isEmpty();

            Set<TopologySnapshot> clusters = clusters(userKey);

            boolean hasDemo = !clusters.isEmpty();

            if (!userKey.isDemo())
                hasDemo = !F.isEmpty(clusterIdsByBrowser.get(new UserKey(userKey.getAccId(), true)));

            return Stream.<Map.Entry<String, Object>>of(
                entry("clusters", clusters),
                entry("hasAgent", hasAgent),
                entry("hasDemo", hasDemo)
            ).collect(entriesToMap());
        });

        return new WebSocketResponse(AGENT_STATUS, res);
    }

    /**
     * @param accIds Account ids.
     * @param newTop New topology.
     * @return Old topology.
     */
    protected TopologySnapshot updateTopology(Set<UUID> accIds, TopologySnapshot newTop) {
        UUID nid = ignite.cluster().localNode().id();

        return txMgr.doInTransaction(() -> {
            ClusterSession clusterId = new ClusterSession(nid, newTop.getId());

            for (UUID accId : accIds) {
                backendByAgent.add(new AgentKey(accId, newTop.getId()), nid);

                clusterIdsByBrowser.add(new UserKey(accId, newTop.isDemo()), clusterId);
            }

            return clusters.getAndPut(newTop.getId(), newTop);
        });
    }

    /**
     * @param req Agent handshake.
     */
    private void validateAgentHandshake(AgentHandshakeRequest req) {
        if (F.isEmpty(req.getTokens()))
            throw new IllegalArgumentException(messages.getMessage("err.tokens-no-specified-in-agent-handshake-req"));

        if (!SUPPORTED_VERS.contains(req.getVersion()))
            throw new IllegalArgumentException(messages.getMessageWithArgs("err.agent-unsupport-version", req.getVersion()));
    }

    /**
     * @param tokens Tokens.
     */
    private Collection<Account> loadAccounts(Set<String> tokens) {
        Collection<Account> accounts = accRepo.getAllByTokens(tokens);

        if (accounts.isEmpty())
            throw new IllegalArgumentException(messages.getMessageWithArgs("err.failed-auth-with-tokens", tokens));

        return accounts;
    }

    /**
     * @param user User.
     */
    private Set<TopologySnapshot> clusters(UserKey user) {
        return Optional.ofNullable(clusterIdsByBrowser.get(user))
            .orElseGet(Collections::emptySet).stream()
            .map(ClusterSession::getClusterId)
            .distinct()
            .map(this.clusters::get)
            .collect(toSet());
    }

    /**
     * @param key Agent key.
     */
    private Optional<WebSocketSession> findLocalAgent(AgentKey key) {
        return locAgents.entrySet().stream()
            .filter((e) -> {
                Set<UUID> accIds = e.getValue().getAccIds();

                if (F.isEmpty(key.getClusterId()))
                    return accIds.contains(key.getAccId());

                Set<String> clusterIds = e.getValue().getClusterIds();

                return accIds.contains(key.getAccId()) && clusterIds.contains(key.getClusterId());
            })
            .findFirst()
            .map(Map.Entry::getKey);
    }

    /**
     * @param accIds Account ids.
     * @param demo is demo stats.
     */
    private void sendAgentStats(Set<UUID> accIds, boolean demo) {
        for (UUID accId : accIds) {
            UserKey key = new UserKey(accId, demo);
            WebSocketResponse stats = collectAgentStats(key);

            ignite.message().send(SEND_TO_USER_BROWSER, new UserEvent(key, stats));
        }
    }

    /**
     * @param accIds Account ids.
     */
    private void sendAgentStats(Set<UUID> accIds) {
        sendAgentStats(accIds, true);
        sendAgentStats(accIds, false);
    }

    /**
     * @param ses Agent session.
     * @param accIds Account ids.
     */
    private void updateAgentsCache(WebSocketSession ses, Set<UUID> accIds) {
        UUID nid = ignite.cluster().localNode().id();

        this.txMgr.doInTransaction(() ->
            accIds.forEach(accId -> backendByAgent.add(new AgentKey(accId), nid))
        );

        locAgents.put(ses, new AgentSession(accIds));
    }
}
