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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
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
import org.springframework.web.socket.PongMessage;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.ignite.console.json.JsonUtils.errorToJson;
import static org.apache.ignite.console.json.JsonUtils.fromJson;
import static org.apache.ignite.console.json.JsonUtils.toJson;
import static org.apache.ignite.console.websocket.WebSocketConsts.ADMIN_ANNOUNCEMENT;
import static org.apache.ignite.console.websocket.WebSocketConsts.AGENTS_PATH;
import static org.apache.ignite.console.websocket.WebSocketConsts.AGENT_HANDSHAKE;
import static org.apache.ignite.console.websocket.WebSocketConsts.AGENT_REVOKE_TOKEN;
import static org.apache.ignite.console.websocket.WebSocketConsts.AGENT_STATUS;
import static org.apache.ignite.console.websocket.WebSocketConsts.BROWSERS_PATH;
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
public class WebSocketManager extends TextWebSocketHandler {
    /** */
    private static final Logger log = LoggerFactory.getLogger(WebSocketManager.class);

    /** */
    private static final String VISOR_IGNITE = "org.apache.ignite.internal.visor.";

    /** */
    private static final PingMessage PING = new PingMessage(UTF_8.encode("PING"));

    /** */
    private final AccountsRepository accRepo;

    /** */
    private final Map<WebSocketSession, Set<String>> agents;

    /** */
    private final Map<WebSocketSession, TopologySnapshot> clusters;

    /** */
    private final Map<WebSocketSession, String> browsers;

    /** */
    private final Map<String, WebSocketSession> requests;

    /** */
    private final Map<String, String> supportedAgents;

    /** */
    private final Map<String, VisorTaskDescriptor> visorTasks;

    /** */
    volatile private Announcement lastAnn;

    /**
     * @param accRepo Repository to work with accounts.
     */
    public WebSocketManager(AccountsRepository accRepo) {
        this.accRepo = accRepo;

        agents = new ConcurrentLinkedHashMap<>();
        clusters = new ConcurrentHashMap<>();
        browsers = new ConcurrentHashMap<>();
        requests = new ConcurrentHashMap<>();
        supportedAgents = new ConcurrentHashMap<>();
        visorTasks = new ConcurrentHashMap<>();
    }

    /**
     * @param ws Session to close.
     */
    public void closeSession(WebSocketSession ws) {
        log.info("Session closed: " + ws);

        if (AGENTS_PATH.equals(ws.getUri().getPath())) {
            agents.remove(ws);
            clusters.remove(ws);

            updateAgentStatus();
        }
        else
            browsers.remove(ws);
    }

    /**
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

    /**
     * Broadcast event to all connected agents.
     *
     * @param wsBrowser Browser session.
     * @param evt Event to send.
     */
    private void sendToAgent(WebSocketSession wsBrowser, WebSocketEvent evt) {
        try {
            String tok = token(wsBrowser);

            WebSocketSession wsAgent = agents
                .entrySet()
                .stream()
                .filter(e -> e.getValue().contains(tok))
                .findFirst()
                .map(Map.Entry::getKey)
                .orElseThrow(() -> new IllegalStateException("Agent not found for token: " + tok));

            if (log.isDebugEnabled())
                log.debug("Found agent session [token=" + tok + ", session=" + wsAgent + ", event=" + evt + "]");

            if (NODE_VISOR.equals(evt.getEventType()))
                prepareNodeVisorParams(evt);

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
        }
    }

    /**
     * @param evt Event to process.
     * @throws IOException If failed to process.
     */
    private AgentHandshakeResponse agentHandshake(WebSocketEvent evt) throws IOException {
        AgentHandshakeRequest req = fromJson(evt.getPayload(), AgentHandshakeRequest.class);

        if (F.isEmpty(req.getTokens()))
            return new AgentHandshakeResponse("Tokens not set. Please reload agent or check settings.");

        if (!F.isEmpty(req.getVersion()) && !F.isEmpty(req.getBuildTime()) & !F.isEmpty(supportedAgents)) {
            // TODO WC-1053 Implement version check in beta2 stage.
            return new AgentHandshakeResponse("You are using an older version of the agent. Please reload agent.");
        }

        Set<String> tokens = req.getTokens();

        Set<String> validTokens = accRepo.validateTokens(tokens);

        if (F.isEmpty(validTokens)) {
            return new AgentHandshakeResponse("Failed to authenticate with token(s): " + tokens + "." +
                " Please reload agent or check settings.");
        }

        return new AgentHandshakeResponse(validTokens);
    }

    /**
     * @param ws Session.
     * @param evt Event to process.
     */
    private void registerCluster(WebSocketSession ws, WebSocketEvent evt) {
        try {
            TopologySnapshot top = fromJson(evt.getPayload(), TopologySnapshot.class);

            clusters.put(ws, top);

            updateAgentStatus();
        }
        catch (Throwable e) {
            log.error("Failed to send information about clusters to browsers", e);
        }
    }

    /**
     * @param ws Session.
     */
    protected void registerBrowser(WebSocketSession ws) {
        log.info("Browser connected: " + ws);

        browsers.put(ws, token(ws));

        updateAgentStatus();

        sendAnnouncement(ws);
    }

    /**
     * Extract user token from session.
     *
     * @param ws Websocket.
     * @return Token.
     */
    private String token(WebSocketSession ws) {
        Principal p = ws.getPrincipal();

        if (p instanceof UsernamePasswordAuthenticationToken) {
            UsernamePasswordAuthenticationToken t = (UsernamePasswordAuthenticationToken)p;

            Object tp = t.getPrincipal();

            if (tp instanceof Account) {
                Account acc = (Account)tp;

                return acc.getToken();
            }
        }

        log.error("Token not found [session=" + ws + "]");

        return UUID.randomUUID().toString();
    }

    /**
     * @param ws Websocket.
     * @param msg Incoming message.
     * @throws IOException If failed to handle event.
     */
    private void handleAgentEvents(WebSocketSession ws, TextMessage msg) throws IOException {
        WebSocketEvent evt = fromJson(msg.getPayload(), WebSocketEvent.class);

        switch (evt.getEventType()) {
            case AGENT_HANDSHAKE:
                AgentHandshakeResponse res = agentHandshake(evt);

                if (F.isEmpty(res.getError())) {
                    log.info("Agent connected [evt=" + evt + "]");

                    agents.put(ws, res.getTokens());
                }
                else
                    log.warn("Agent not connected [err=" + res.getError() + ", evt=" + evt + "]");

                sendMessage(ws, evt.setPayload(toJson(res)));

                break;

            case CLUSTER_TOPOLOGY:
                registerCluster(ws, evt);

                break;

            default:
                WebSocketSession browserWs = requests.remove(evt.getRequestId());

                if (browserWs != null)
                    sendMessage(browserWs, evt);
                else
                    log.warn("Failed to send event to browser: " + evt);
        }
    }

    /**
     * @param ws Websocket.
     * @param msg Incoming message.
     * @throws IOException If failed to handle event.
     */
    private void handleBrowserEvents(WebSocketSession ws, TextMessage msg) throws IOException {
        WebSocketEvent evt = fromJson(msg.getPayload(), WebSocketEvent.class);

        switch (evt.getEventType()) {
            case SCHEMA_IMPORT_DRIVERS:
            case SCHEMA_IMPORT_SCHEMAS:
            case SCHEMA_IMPORT_METADATA:
            case NODE_REST:
            case NODE_VISOR:
                requests.put(evt.getRequestId(), ws);

                sendToAgent(ws, evt);

                break;

            default:
                log.warn("Unknown event: " + evt);
        }
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
     * @param browserWs Browser to send current announcement.
     */
    private void sendAnnouncement(WebSocketSession browserWs) {
        if (lastAnn != null)
            sendAnnouncement(Collections.singleton(browserWs), lastAnn);
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
     * Send to all connected browsers info about agent status.
     */
    private void updateAgentStatus() {
        browsers.forEach((wsBrowser, token) -> {
            List<TopologySnapshot> tops = new ArrayList<>();

            agents.forEach((wsAgent, tokens) -> {
                if (tokens.contains(token)) {
                    TopologySnapshot top = clusters.get(wsAgent);

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
                log.error("Failed to update agent status [session=" + wsBrowser + ", token=" + token + "]", e);
            }
        });
    }

    /**
     * @param oldTok Token to revoke.
     * @param newTok New token.
     */
    public void revokeToken(String oldTok, String newTok) {
        log.info("Revoke token [old: " + oldTok + ", new: " + newTok + "]");

        agents.forEach((ws, tokens) -> {
            try {
                if (tokens.remove(oldTok))
                    sendMessage(ws, new WebSocketEvent(AGENT_REVOKE_TOKEN, oldTok));
            }
            catch (Throwable e) {
                log.error("Failed to revoke token: " + oldTok);
            }
        });

        browsers.forEach((ws, tok) -> {
            if (tok.equals(oldTok))
                browsers.put(ws, newTok);
        });
    }

    /** {@inheritDoc} */
    @Override public void handleTextMessage(WebSocketSession ws, TextMessage msg) {
        try {
            if (BROWSERS_PATH.equals(ws.getUri().getPath()))
                handleBrowserEvents(ws, msg);
            else
                handleAgentEvents(ws, msg);
        }
        catch (Throwable e) {
            log.error("Failed to process websocket message [session=" + ws + ", msg=" + msg + "]", e);
        }
    }

    /** {@inheritDoc} */
    @Override protected void handlePongMessage(WebSocketSession ws, PongMessage msg) {
        try {
            if (log.isTraceEnabled())
                log.trace("Received pong message [socket=" + ws + ", msg=" + msg + "]");
        }
        catch (Throwable e) {
            log.error("Failed to process pong message [socket=" + ws + ", msg=" + msg + "]", e);
        }
    }

    /** {@inheritDoc} */
    @Override public void afterConnectionEstablished(WebSocketSession ws) {
        log.info("Session opened [socket=" + ws + "]");

        ws.setTextMessageSizeLimit(10 * 1024 * 1024); // TODO IGNITE-5617 how to configure in more correct way

        if (BROWSERS_PATH.equals(ws.getUri().getPath()))
            registerBrowser(ws);
    }

    /** {@inheritDoc} */
    @Override public void afterConnectionClosed(WebSocketSession ws, CloseStatus status) {
        log.info("Session closed [socket=" + ws + ", status=" + status + "]");

        closeSession(ws);
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
}
