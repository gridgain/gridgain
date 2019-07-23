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
import org.apache.ignite.messaging.MessagingListenActor;
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
import static org.apache.ignite.console.utils.Utils.extractErrorMessage;
import static org.apache.ignite.console.utils.Utils.fromJson;
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
    private final Map<WebSocketSession, AgentSession> locAgents;
    
    /** */
    private final Map<String, UUID> backendByReq;

    /** */
    private CacheHolder<String, TopologySnapshot> clusters;

    /** */
    private OneToManyIndex<UserKey, ClusterSession> clusterIdsByBrowser;

    /** */
    private OneToManyIndex<AgentKey, UUID> backendByAgent;

    /**
     * @param accRepo Repository to work with accounts.
     */
    public AgentsService(Ignite ignite, TransactionManager txMgr, AccountsRepository accRepo) {
        super(ignite, txMgr);

        this.accRepo = accRepo;

        locAgents = new ConcurrentLinkedHashMap<>();
        backendByReq = new ConcurrentHashMap<>();

        this.txMgr.registerStarter(() -> {
            backendByAgent = new OneToManyIndex<>(ignite, "wc_backend", (key) -> messages.getMessage("err.data-access-violation"));
            cleanupBackendIndex(backendByAgent);

            clusterIdsByBrowser = new OneToManyIndex<>(ignite, "wc_clusters_idx", (key) -> messages.getMessage("err.data-access-violation"));
            cleanupClusterIndex();

            clusters = new CacheHolder<>(ignite, "wc_clusters");
        });
    }

    /** {@inheritDoc} */
    @Override public void registerListeners() {
        ignite.message().localListen(SEND_TO_BACKEND, new MessagingListenActor<AgentEvent>() {
            @Override protected void receive(UUID nodeId, AgentEvent req) {
                Optional<WebSocketSession> locAgent = findLocalAgent(req.getAgentKey());
                WebSocketEvent evt = req.getEvent();


                if (locAgent.isPresent()) {
                    sendLocally(locAgent.get(), evt, nodeId);

                    return;
                }

                ignite.message(ignite.cluster().forNodeId(nodeId)).send(
                    SEND_RESPONSE_TO_BROWSER,
                    evt.withError(messages.getMessage("err.agent-not-found"))
                );
            }
        });
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
                    UUID nid = backendByReq.remove(evt.getRequestId());

                    ignite.message(ignite.cluster().forNodeId(nid)).send(SEND_RESPONSE_TO_BROWSER, evt);
                }
                catch (Exception e) {
                    log.warn("Failed to backend to send response: " + evt, e);
                }
        }
    }

    /**
     * @param wsAgent Session.
     * @param tops Topology snapshots.
     */
    public void processTopologyUpdate(WebSocketSession wsAgent, Collection<TopologySnapshot> tops) {
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

        desc.setClusterIds(tops.stream().map(TopologySnapshot::getId).collect(toSet()));

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

                if (agentSes.canBeClose())
                    U.closeQuiet(ws);

                cleanupAccountCache(Collections.singleton(acc.getId()), agentSes.getClusterIds());
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

        cleanupAccountCache(agentSes.getAccIds(), agentSes.getClusterIds());

        sendAgentStats(agentSes.getAccIds());
    }

    private void cleanupAccountCache(Set<UUID> accIds, Set<String> clusterIds) {
        UUID nid = ignite.cluster().localNode().id();

        for (UUID accId : accIds) {
            this.txMgr.doInTransaction(() -> {
                backendByAgent.remove(new AgentKey(accId), nid);

                for (String clusterId : clusterIds)
                    backendByAgent.remove(new AgentKey(accId, clusterId), nid);
            });
        }
    }

    /**
     * @param ses Agent session.
     * @param evt Event.
     */
    private void sendLocally(WebSocketSession ses, WebSocketEvent evt, UUID destNid) {
        log.debug("Found local agent session [session=" + ses + ", event=" + evt + "]");

        try {
            sendMessage(ses, evt);

            backendByReq.put(evt.getRequestId(), destNid);
        }
        catch (Exception e) {
            ignite.message(ignite.cluster().forLocal()).send(
                SEND_RESPONSE_TO_BROWSER,
                evt.withError(extractErrorMessage(messages.getMessage("err.failed-to-send-to-agent"), e))
            );
        }
    }

    /**
     * @param nids Nodes to send.
     * @param req Request object.
     */
    private void sendRemote(Set<UUID> nids, AgentEvent req) {
        try {
            if (nids.isEmpty())
                throw new IllegalStateException(messages.getMessage("err.agent-not-found"));

            ignite.message(ignite.cluster().forNodeIds(nids).forRandom()).send(SEND_TO_BACKEND, req);
        }
        catch (Exception e) {
            ignite.message(ignite.cluster().forLocal()).send(
                SEND_RESPONSE_TO_BROWSER,
                req.getEvent().withError(extractErrorMessage(messages.getMessage("err.failed-to-send-to-agent"), e))
            );
        }
    }

    /**
     * Send event to first user agent.
     *
     * @param key Agent key.
     * @param evt Event.
     */
    void sendToAgent(AgentKey key, WebSocketEvent evt) {
        Optional<WebSocketSession> locAgent = findLocalAgent(key);

        if (locAgent.isPresent()) {
            sendLocally(locAgent.get(), evt, ignite.cluster().localNode().id());

            return;
        }

        Set<UUID> nids = txMgr.doInTransaction(() -> backendByAgent.get(key));

        sendRemote(nids, new AgentEvent(key, evt));
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

            if (!clusterIds.isEmpty()) {
                clusters.cache().removeAll(clusterIds);

                log.error("Failed to receive topology update for clusters: " + clusterIds);
            }
        });
    }

    /**
     * Periodically cleanup agents without topology updates.
     */
    private void cleanupClusterIndex() {
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
     * Periodically cleanup agents without topology updates.
     */
    private <K> void cleanupBackendIndex(OneToManyIndex<K, UUID> backendBy) {
        Collection<UUID> nids = U.nodeIds(ignite.cluster().nodes());

        stream(backendBy.cache().spliterator(), false)
            .peek(entry -> entry.getValue().removeAll(nids))
            .filter(entry -> entry.getValue().isEmpty())
            .forEach(entry -> backendBy.cache().remove(entry.getKey()));
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
        accIds.stream()
            .map(accId -> new UserKey(accId, demo))
            .forEach((key) -> {
                WebSocketResponse stats = collectAgentStats(key);

                ignite.message().send(SEND_TO_USER_BROWSER, new UserEvent(key, stats));
            });
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
