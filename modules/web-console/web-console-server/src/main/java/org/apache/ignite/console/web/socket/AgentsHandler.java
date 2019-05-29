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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.repositories.AccountsRepository;
import org.apache.ignite.console.websocket.AgentHandshakeRequest;
import org.apache.ignite.console.websocket.AgentHandshakeResponse;
import org.apache.ignite.console.websocket.TopologySnapshot;
import org.apache.ignite.console.websocket.WebSocketEvent;
import org.apache.ignite.internal.util.typedef.F;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import static org.apache.ignite.console.json.JsonUtils.fromJson;
import static org.apache.ignite.console.json.JsonUtils.toJson;
import static org.apache.ignite.console.websocket.WebSocketConsts.AGENT_HANDSHAKE;
import static org.apache.ignite.console.websocket.WebSocketConsts.CLUSTER_TOPOLOGY;

/**
 * Agents web sockets handler.
 */
@Service
public class AgentsHandler extends TextWebSocketHandler {
    /** */
    private static final Logger log = LoggerFactory.getLogger(AgentsHandler.class);

    /** */
    private Map<String, String> supportedAgents;

    /** */
    private AccountsRepository accRepo;

    /** */
    private WebSocketsManager wsm;

    /**
     * @param accRepo Repository to work with accounts.
     * @param wsm Web sockets manager.
     */
    public AgentsHandler(AccountsRepository accRepo, WebSocketsManager wsm) {
        supportedAgents = new ConcurrentHashMap<>();

        this.accRepo = accRepo;
        this.wsm = wsm;
    }

    /**
     * @param req Agent handshake.
     */
    private void validateAgentHandshake(AgentHandshakeRequest req) {
        if (F.isEmpty(req.getTokens()))
            throw new IllegalArgumentException("Tokens not set. Please reload agent or check settings.");

        if (!F.isEmpty(req.getVersion()) && !F.isEmpty(req.getBuildTime()) & !F.isEmpty(supportedAgents)) {
            // TODO WC-1053 Implement version check in beta2 stage.
            throw new IllegalArgumentException("You are using an older version of the agent. Please reload agent.");
        }
    }

    /**
     * @param tokens Tokens.
     */
    private Collection<Account> loadAccounts(Set<String> tokens) {
        Collection<Account> accounts = accRepo.getAllByTokens(tokens);

        if (accounts.isEmpty()) {
            throw new IllegalArgumentException("Failed to authenticate with token(s): " + tokens + ". " +
                "Please reload agent or check settings.");
        }

        return accounts;
    }

    /**
     * @param ws Session to send message.
     * @param reqEvt Event .
     * @throws IOException If failed to send message.
     */
    private void sendResponse(WebSocketSession ws, WebSocketEvent reqEvt, Object payload) throws IOException {
        WebSocketEvent resEvt = new WebSocketEvent(reqEvt).setPayload(toJson(payload));

        ws.sendMessage(new TextMessage(toJson(resEvt)));
    }

    /**
     * @param c Collection of Objects.
     * @param mapper Mapper.
     */
    private <T, R> Set<R> mapToSet(Collection<T> c, Function<? super T, ? extends R> mapper) {
        return c.stream().map(mapper).collect(Collectors.toSet());
    }

    /** {@inheritDoc} */
    @Override public void handleTextMessage(WebSocketSession ws, TextMessage msg) {
        try {
            WebSocketEvent evt = fromJson(msg.getPayload(), WebSocketEvent.class);

            switch (evt.getEventType()) {
                case AGENT_HANDSHAKE:
                    try {
                        AgentHandshakeRequest req = fromJson(evt.getPayload(), AgentHandshakeRequest.class);

                        validateAgentHandshake(req);

                        Collection<Account> accounts = loadAccounts(req.getTokens());

                        sendResponse(ws, evt, new AgentHandshakeResponse(mapToSet(accounts, Account::getToken)));

                        wsm.saveAgentSession(ws, mapToSet(accounts, Account::getId));

                        log.info("Agent connected: " + req);
                    }
                    catch (Exception e) {
                        log.warn("Failed to establish connection in handshake: " + evt, e);

                        sendResponse(ws, evt, new AgentHandshakeResponse(e));

                        ws.close();
                    }

                    break;

                case CLUSTER_TOPOLOGY:
                    TopologySnapshot top = fromJson(evt.getPayload(), TopologySnapshot.class);

                    wsm.processTopologyUpdate(ws, top);

                    break;

                default:
                    wsm.sendResponseToBrowser(evt);
            }
        }
        catch (Throwable e) {
            log.error("Failed to process message from agent [session=" + ws + ", msg=" + msg + "]", e);
        }
    }

    /** {@inheritDoc} */
    @Override public void afterConnectionEstablished(WebSocketSession ws) {
        log.info("Agent session opened [socket=" + ws + "]");

        ws.setTextMessageSizeLimit(10 * 1024 * 1024);
    }

    /** {@inheritDoc} */
    @Override public void afterConnectionClosed(WebSocketSession ws, CloseStatus status) {
        log.info("Agent session closed [socket=" + ws + ", status=" + status + "]");

        wsm.closeAgentSession(ws);
    }
}
