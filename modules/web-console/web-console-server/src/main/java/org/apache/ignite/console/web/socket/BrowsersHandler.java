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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.console.json.JsonArray;
import org.apache.ignite.console.json.JsonObject;
import org.apache.ignite.console.web.model.VisorTaskDescriptor;
import org.apache.ignite.console.websocket.WebSocketEvent;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import static org.apache.ignite.console.json.JsonUtils.fromJson;
import static org.apache.ignite.console.json.JsonUtils.toJson;
import static org.apache.ignite.console.websocket.WebSocketConsts.NODE_REST;
import static org.apache.ignite.console.websocket.WebSocketConsts.NODE_VISOR;
import static org.apache.ignite.console.websocket.WebSocketConsts.SCHEMA_IMPORT_DRIVERS;
import static org.apache.ignite.console.websocket.WebSocketConsts.SCHEMA_IMPORT_METADATA;
import static org.apache.ignite.console.websocket.WebSocketConsts.SCHEMA_IMPORT_SCHEMAS;

/**
 * Browsers web sockets handler.
 */
@Service
public class BrowsersHandler extends TextWebSocketHandler {
    /** */
    private static final Logger log = LoggerFactory.getLogger(BrowsersHandler.class);

    /** */
    private static final String VISOR_IGNITE = "org.apache.ignite.internal.visor.";

    /** */
    private final Map<String, VisorTaskDescriptor> visorTasks = new HashMap<>();

    /** */
    private final WebSocketsManager wsm;

    /**
     * @param wsm Web sockets manager.
     */
    public BrowsersHandler(WebSocketsManager wsm) {
        this.wsm = wsm;

        registerVisorTasks();
    }

    /** {@inheritDoc} */
    @Override public void handleTextMessage(WebSocketSession ws, TextMessage msg) {
        try {
            WebSocketEvent evt = fromJson(msg.getPayload(), WebSocketEvent.class);

            switch (evt.getEventType()) {
                case SCHEMA_IMPORT_DRIVERS:
                case SCHEMA_IMPORT_SCHEMAS:
                case SCHEMA_IMPORT_METADATA:
                    wsm.sendToFirstAgent(ws, evt);

                    break;

                case NODE_REST:
                    wsm.sendToFirstAgent(ws, evt);

                    break;
                case NODE_VISOR:


                    wsm.sendToFirstAgent(ws, evt);

                    break;

                default:
                    throw new IllegalStateException("Unknown event: " + evt);
            }
        }
        catch (Throwable e) {
            log.error("Failed to process message from browser [session=" + ws + ", msg=" + msg + "]", e);

            String errMsg = "Failed to send event to agent: " + evt;

            log.error(errMsg, e);

            sendError(wsBrowser, evt, errMsg, e);
        }
    }

    /** {@inheritDoc} */
    @Override public void afterConnectionEstablished(WebSocketSession ws) {
        log.info("Browser session opened [socket=" + ws + "]");

        ws.setTextMessageSizeLimit(10 * 1024 * 1024);

        wsm.onBrowserConnect(ws);
    }

    /** {@inheritDoc} */
    @Override public void afterConnectionClosed(WebSocketSession ws, CloseStatus status) {
        log.info("Browser session closed [socket=" + ws + ", status=" + status + "]");

        wsm.onBrowserConnectionClosed(ws);
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
    private void registerVisorTasks() {
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
