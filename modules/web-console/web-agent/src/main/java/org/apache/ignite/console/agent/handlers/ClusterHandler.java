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

package org.apache.ignite.console.agent.handlers;

import java.io.IOException;
import java.net.ConnectException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.console.agent.AgentConfiguration;
import org.apache.ignite.console.agent.rest.RestExecutor;
import org.apache.ignite.console.agent.rest.RestResult;
import org.apache.ignite.console.json.JsonObject;
import org.apache.ignite.console.websocket.TopologySnapshot;
import org.apache.ignite.console.websocket.WebSocketEvent;
import org.apache.ignite.internal.processors.rest.client.message.GridClientNodeBean;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.logger.slf4j.Slf4jLogger;
import org.slf4j.LoggerFactory;

import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR;
import static org.apache.ignite.console.json.JsonUtils.fromJson;
import static org.apache.ignite.console.websocket.WebSocketConsts.CLUSTER_DISCONNECTED;
import static org.apache.ignite.console.websocket.WebSocketConsts.CLUSTER_TOPOLOGY;
import static org.apache.ignite.internal.processors.rest.GridRestResponse.STATUS_SUCCESS;
import static org.apache.ignite.internal.processors.rest.client.message.GridClientResponse.STATUS_FAILED;

/**
 * API to transfer topology from Ignite cluster to Web Console.
 */
public class ClusterHandler {
    /** */
    private static final IgniteLogger log = new Slf4jLogger(LoggerFactory.getLogger(ClusterHandler.class));

    /** */
    private static final IgniteProductVersion IGNITE_2_0 = IgniteProductVersion.fromString("2.0.0");

    /** */
    private static final IgniteProductVersion IGNITE_2_1 = IgniteProductVersion.fromString("2.1.0");

    /** */
    private static final IgniteProductVersion IGNITE_2_3 = IgniteProductVersion.fromString("2.3.0");

    /** Unique Visor key to get events last order. */
    private static final String EVT_LAST_ORDER_KEY = "WEB_AGENT_" + UUID.randomUUID().toString();

    /** Unique Visor key to get events throttle counter. */
    private static final String EVT_THROTTLE_CNTR_KEY = "WEB_AGENT_" + UUID.randomUUID().toString();

    /** */
    private static final String EXPIRED_SES_ERROR_MSG = "Failed to handle request - unknown session token (maybe expired session)";

    /** */
    private String sesTok;

    /** Topology refresh frequency. */
    private static final long REFRESH_FREQ = 3000L;

    /** Latest topology snapshot. */
    private TopologySnapshot top;

    /** */
    private final AgentConfiguration cfg;

    /** */
    private final WebSocketSession wss;

    /** */
    private final RestExecutor restExecutor;

    /** */
    private static final ScheduledExecutorService pool = Executors.newScheduledThreadPool(1);

    /** */
    private ScheduledFuture<?> refreshTask;

    /**
     * @param cfg Web agent configuration.
     * @param wss Websocket session.
     * @throws Exception If failed to create cluster handler.
     */
    public ClusterHandler(AgentConfiguration cfg, WebSocketSession wss) throws Exception {
        this.cfg = cfg;
        this.wss = wss;

        restExecutor = new RestExecutor(cfg);
    }

    /**
     * Callback on disconnect from cluster.
     */
    private void clusterDisconnect() {
        if (top == null)
            return;

        top = null;

        log.info("Connection to cluster was lost");

        try {
            wss.send(CLUSTER_DISCONNECTED, null);
        }
        catch (Throwable e) {
            log.error("Failed to send 'Cluster disconnected' event to server", e);
        }
    }

    /**
     * Execute REST command under agent user.
     *
     * @param params Command params.
     * @return Command result.
     * @throws IOException If failed to execute.
     */
    private RestResult restCommand(JsonObject params) throws IOException {
        if (!F.isEmpty(sesTok))
            params.put("sessionToken", sesTok);
        else if (!F.isEmpty(cfg.nodeLogin()) && !F.isEmpty(cfg.nodePassword())) {
            params.put("user", cfg.nodeLogin());
            params.put("password", cfg.nodePassword());
        }

        RestResult res = restExecutor.sendRequest(params);

        switch (res.getStatus()) {
            case STATUS_SUCCESS:
                sesTok = res.getSessionToken();

                return res;

            case STATUS_FAILED:
                if (res.getError().startsWith(EXPIRED_SES_ERROR_MSG)) {
                    sesTok = null;

                    params.remove("sessionToken");

                    return restCommand(params);
                }

            default:
                return res;
        }
    }

    /**
     * @param ver Cluster version.
     * @param nid Node ID.
     * @return Cluster active state.
     * @throws IOException If failed to collect cluster active state.
     */
    private boolean active(IgniteProductVersion ver, UUID nid) throws IOException {
        // 1.x clusters are always active.
        if (ver.compareTo(IGNITE_2_0) < 0)
            return true;

        JsonObject params = new JsonObject();

        boolean v23 = ver.compareTo(IGNITE_2_3) >= 0;

        if (v23)
            params.put("cmd", "currentState");
        else {
            params.put("cmd", "exe");
            params.put("name", "org.apache.ignite.internal.visor.compute.VisorGatewayTask");
            params.put("p1", nid);
            params.put("p2", "org.apache.ignite.internal.visor.node.VisorNodeDataCollectorTask");
            params.put("p3", "org.apache.ignite.internal.visor.node.VisorNodeDataCollectorTaskArg");
            params.put("p4", false);
            params.put("p5", EVT_LAST_ORDER_KEY);
            params.put("p6", EVT_THROTTLE_CNTR_KEY);

            if (ver.compareTo(IGNITE_2_1) >= 0)
                params.put("p7", false);
            else {
                params.put("p7", 10);
                params.put("p8", false);
            }
        }

        RestResult res = restCommand(params);

        if (res.getStatus() == STATUS_SUCCESS)
            return v23 ? Boolean.valueOf(res.getData()) : res.getData().contains("\"active\":true");

        return false;
    }

    /**
     * Collect topology.
     *
     * @return REST result.
     * @throws IOException If failed to collect cluster topology.
     */
    private RestResult topology() throws IOException {
        JsonObject params = new JsonObject()
            .add("cmd", "top")
            .add("attr", true)
            .add("mtr", false)
            .add("caches", false);

        return restCommand(params);
    }

    /**
     * Start watch cluster.
     */
    public void start() {
        refreshTask = pool.scheduleWithFixedDelay(() -> {
            try {
                RestResult res = topology();

                if (res.getStatus() == STATUS_SUCCESS) {
                    List<GridClientNodeBean> nodes = fromJson(
                        res.getData(),
                        new TypeReference<List<GridClientNodeBean>>() {}
                    );

                    TopologySnapshot newTop = new TopologySnapshot(nodes);

                    if (newTop.differentCluster(top))
                        log.info("Connection successfully established to cluster with nodes: " + newTop.nid8());
                    else if (newTop.topologyChanged(top))
                        log.info("Cluster topology changed, new topology: " + newTop.nid8());

                    boolean active = active(newTop.clusterVersion(), F.first(newTop.getNids()));

                    newTop.setActive(active);
                    newTop.setSecured(!F.isEmpty(res.getSessionToken()));

                    top = newTop;

                    wss.send(CLUSTER_TOPOLOGY, top);
                }
                else {
                    LT.warn(log, res.getError());

                    clusterDisconnect();
                }
            }
            catch (ConnectException ignored) {
                clusterDisconnect();
            }
            catch (Throwable e) {
                LT.error(log, e, "WatchTask failed");

                clusterDisconnect();
            }
        }, 0L, REFRESH_FREQ, TimeUnit.MILLISECONDS);
    }

    /**
     * Stop cluster watch.
     */
    public void stop() {
        refreshTask.cancel(true);

        pool.shutdownNow();
    }

    /**
     * @param evt Websocket event.
     */
    public void restRequest(WebSocketEvent evt) {
        if (log.isDebugEnabled())
            log.debug("Processing REST request: " + evt);

        try {
            JsonObject args = fromJson(evt.getPayload());

            JsonObject params = args.containsKey("params") ? args.getJsonObject("params") : new JsonObject();

            // TODO IGNITE-5617 Restore demo mode.
//            if (!args.containsKey("demo"))
//                throw new IllegalArgumentException("Missing demo flag in arguments: " + args);

//            boolean demo = (boolean)args.get("demo");
//
//            if (F.isEmpty((String)args.get("token")))
//                return RestResult.fail(401, "Request does not contain user token.");

            RestResult res;

            try {
                res = restExecutor.sendRequest(params);
            }
            catch (Throwable e) {
                res = RestResult.fail(HTTP_INTERNAL_ERROR, e.getMessage());
            }

            wss.reply(evt, res);
        }
        catch (Throwable e) {
            String errMsg = "Failed to handle REST request: " + evt;

            log.error(errMsg, e);

            wss.fail(evt, errMsg, e);
        }
    }
}
