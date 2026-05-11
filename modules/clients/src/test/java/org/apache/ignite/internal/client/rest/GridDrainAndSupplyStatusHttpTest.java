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

package org.apache.ignite.internal.client.rest;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.PluginProvider;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * HTTP-level tests for the v14 probe/lifecycle endpoints: {@code cmd=alive},
 * {@code cmd=probe}, {@code cmd=drain} (start / status / stop), and
 * {@code cmd=supply-status}. Exercises the Jetty path end-to-end (request
 * parsing, handler dispatch, response status, JSON body, and the
 * {@code X-Active-Thin-Clients} / {@code X-Supplying} response headers
 * required for shell-parseable preStop hooks).
 *
 * <p>Per HLD v14 §2 ({@code 14-probe-rework.md}), {@code cmd=probe} now
 * returns 503 if any of: kernel not started; drain active; PME in progress;
 * any non-system cache group has active outbound supply. The
 * MOVING-partition / supplying transitions during rebalance are exercised
 * by {@code GridDrainAndSupplyStatusRebalanceTest}.</p>
 */
public class GridDrainAndSupplyStatusHttpTest extends GridCommonAbstractTest {
    /** Default REST port. */
    private static final int JETTY_PORT = 8080;

    /** Latch released by the delayed-start plugin once the rest http processor is live. */
    private CountDownLatch triggerPluginStartLatch = new CountDownLatch(1);

    /** Latch awaited by the delayed-start plugin until the test finishes its HTTP probe. */
    private CountDownLatch triggerRestCmdLatch = new CountDownLatch(1);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConnectorConfiguration(new ConnectorConfiguration());

        if ("persistent".equals(igniteInstanceName)) {
            cfg.setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(true)));
        }
        else if ("delayedStart".equals(igniteInstanceName)) {
            PluginProvider delayedStart = new DelayedStartPluginProvider(triggerPluginStartLatch, triggerRestCmdLatch);

            cfg.setPluginProviders(new PluginProvider[] {delayedStart});
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids(false);

        cleanPersistenceDir();
    }

    /**
     * cmd=probe on a fully ready node returns HTTP 200 "grid has started".
     */
    @Test
    public void testProbeReadyState() throws Exception {
        startGrid("regular").cluster().state(ClusterState.ACTIVE);

        Resp resp = http("GET", "/ignite?cmd=probe");
        assertEquals(200, resp.code);
        assertEquals(0, resp.body.get("successStatus"));
        assertEquals("grid has started", resp.body.get("response"));
    }

    /**
     * cmd=probe returns 503 with error="draining" when the drain flag is set.
     * action=stop clears the flag and probe returns 200 again.
     */
    @Test
    public void testProbeReturns503WhenDraining() throws Exception {
        startGrid("regular").cluster().state(ClusterState.ACTIVE);

        // Sanity: probe is 200 before drain is set.
        Resp pre = http("GET", "/ignite?cmd=probe");
        assertEquals(200, pre.code);

        // Trip drain.
        Resp start = http("POST", "/ignite?cmd=drain&action=start");
        assertEquals(200, start.code);
        assertEquals("draining", start.body.get("response"));

        // Probe now returns 503 with reason "draining".
        Resp draining = http("GET", "/ignite?cmd=probe");
        assertEquals(503, draining.code);
        assertEquals(503, draining.body.get("successStatus"));
        assertEquals("draining", draining.body.get("error"));

        // action=stop clears the flag.
        Resp stop = http("POST", "/ignite?cmd=drain&action=stop");
        assertEquals(200, stop.code);
        assertEquals("ready", stop.body.get("response"));

        // Probe is 200 again.
        Resp ready = http("GET", "/ignite?cmd=probe");
        assertEquals(200, ready.code);
        assertEquals("grid has started", ready.body.get("response"));
    }

    /**
     * cmd=probe returns 503 "grid has not started" while the kernel is
     * mid-startup. Mirrors the {@code GridProbeCommandTest#testRestProbeCommandGridNotStarted}
     * pattern using a plugin that pauses inside {@code onIgniteStart}.
     */
    @Test
    public void testProbeReturns503WhenKernelNotStarted() throws Exception {
        new Thread(new Runnable() {
            @Override public void run() {
                try {
                    startGrid("delayedStart");
                }
                catch (Exception e) {
                    log.error("error when starting delayedStart grid", e);
                }
            }
        }).start();

        triggerPluginStartLatch.await();

        Resp resp;
        try {
            resp = http("GET", "/ignite?cmd=probe");
        }
        finally {
            // Always release the plugin so the grid finishes starting and afterTest can stop it.
            triggerRestCmdLatch.countDown();
        }

        assertEquals(503, resp.code);
        assertEquals(503, resp.body.get("successStatus"));
        assertEquals("grid has not started", resp.body.get("error"));
    }

    /**
     * cmd=probe on an INACTIVE cluster returns HTTP 200. v14 §2 doesn't
     * special-case cluster state; an INACTIVE cluster with kernel started,
     * no drain, no PME, and no supply satisfies all 4 conditions.
     */
    @Test
    public void testProbeOnInactiveCluster() throws Exception {
        // Persistence-enabled config comes up INACTIVE by default — DO NOT activate.
        startGrid("persistent");

        // Sanity: cluster is INACTIVE.
        assertFalse(ClusterState.active(grid("persistent").context().state().clusterState().state()));

        Resp resp = http("GET", "/ignite?cmd=probe");
        assertEquals(200, resp.code);
        assertEquals(0, resp.body.get("successStatus"));
        assertEquals("grid has started", resp.body.get("response"));
    }

    /**
     * cmd=alive returns HTTP 200 "alive" on a started node, regardless of
     * cluster state. Liveness is independent of drain / PME / supply.
     */
    @Test
    public void testAliveOnStartedNode() throws Exception {
        startGrid("regular").cluster().state(ClusterState.ACTIVE);

        Resp resp = http("GET", "/ignite?cmd=alive");
        assertEquals(200, resp.code);
        assertEquals(0, resp.body.get("successStatus"));
        assertEquals("alive", resp.body.get("response"));
    }

    /**
     * cmd=alive returns 200 even on an INACTIVE cluster (liveness is "is the
     * node running?", nothing more).
     */
    @Test
    public void testAliveOnInactiveCluster() throws Exception {
        startGrid("persistent");

        Resp resp = http("GET", "/ignite?cmd=alive");
        assertEquals(200, resp.code);
        assertEquals("alive", resp.body.get("response"));
    }

    /**
     * cmd=alive returns 503 "not started" while the kernel is mid-startup.
     */
    @Test
    public void testAliveReturns503WhenKernelNotStarted() throws Exception {
        new Thread(new Runnable() {
            @Override public void run() {
                try {
                    startGrid("delayedStart");
                }
                catch (Exception e) {
                    log.error("error when starting delayedStart grid", e);
                }
            }
        }).start();

        triggerPluginStartLatch.await();

        Resp resp;
        try {
            resp = http("GET", "/ignite?cmd=alive");
        }
        finally {
            triggerRestCmdLatch.countDown();
        }

        assertEquals(503, resp.code);
        assertEquals(503, resp.body.get("successStatus"));
        assertEquals("not started", resp.body.get("error"));
    }

    /**
     * action=status emits the {@code X-Active-Thin-Clients} response header
     * and a structured body containing {@code activeThinClients} and {@code draining}.
     */
    @Test
    public void testDrainStatusEmitsHeader() throws Exception {
        startGrid("regular").cluster().state(ClusterState.ACTIVE);

        Resp status = http("GET", "/ignite?cmd=drain&action=status");
        assertEquals(200, status.code);
        assertEquals(0, status.body.get("successStatus"));

        // Header must be present and match the body field.
        assertNotNull("X-Active-Thin-Clients header missing", status.headerOf("X-Active-Thin-Clients"));

        // Response payload is a Map (Jackson default for the typed POJO).
        Object payload = status.body.get("response");
        assertTrue("response payload not a map: " + payload, payload instanceof Map);

        Map<?, ?> p = (Map<?, ?>)payload;
        assertEquals(Boolean.FALSE, p.get("draining"));
        assertEquals(Integer.parseInt(status.headerOf("X-Active-Thin-Clients")), ((Number)p.get("activeThinClients")).intValue());
    }

    /**
     * cmd=supply-status returns 200 with {@code X-Supplying: false} on a steady-state
     * single-node cluster.
     */
    @Test
    public void testSupplyStatusEmitsHeader() throws Exception {
        startGrid("regular").cluster().state(ClusterState.ACTIVE);

        Resp resp = http("GET", "/ignite?cmd=supply-status");
        assertEquals(200, resp.code);
        assertEquals(0, resp.body.get("successStatus"));

        assertEquals("false", resp.headerOf("X-Supplying"));

        Object payload = resp.body.get("response");
        assertTrue("response payload not a map: " + payload, payload instanceof Map);

        Map<?, ?> p = (Map<?, ?>)payload;
        assertEquals(Boolean.FALSE, p.get("supplying"));
    }

    /**
     * @param method HTTP method.
     * @param pathQ Path with query string (no host/port).
     * @return Parsed response.
     */
    private static Resp http(String method, String pathQ) throws IOException {
        URL url = new URL("http://localhost:" + JETTY_PORT + pathQ);

        HttpURLConnection conn = (HttpURLConnection)url.openConnection();
        conn.setRequestMethod(method);
        conn.connect();

        int code = conn.getResponseCode();

        Map<String, Object> body;

        try (InputStreamReader rdr = new InputStreamReader(
                code >= 400 ? conn.getErrorStream() : conn.getInputStream())) {
            body = new ObjectMapper().readValue(rdr, new TypeReference<Map<String, Object>>() { /* No-op. */ });
        }

        return new Resp(code, body, conn);
    }

    /** Captures status, body, and response headers. */
    private static class Resp {
        /** HTTP status code. */
        final int code;

        /** Parsed JSON body. */
        final Map<String, Object> body;

        /** Connection used to read response headers. */
        private final HttpURLConnection conn;

        /**
         * @param code Status code.
         * @param body Parsed body.
         * @param conn Connection.
         */
        Resp(int code, Map<String, Object> body, HttpURLConnection conn) {
            this.code = code;
            this.body = body;
            this.conn = conn;
        }

        /**
         * @param name Header name.
         * @return Header value or {@code null} if absent.
         */
        String headerOf(String name) {
            return conn.getHeaderField(name);
        }
    }

    /**
     * Plugin that pauses inside {@code onIgniteStart} so a test can issue an
     * HTTP request against a kernal that is mid-startup (REST processor live,
     * but kernal not yet fully started). Mirror of
     * {@code GridProbeCommandTest.DelayedStartPluginProvider}.
     */
    public static class DelayedStartPluginProvider extends AbstractTestPluginProvider {
        /** Latch awaited by this plugin until the test finishes its HTTP probe. */
        private final CountDownLatch triggerRestCmd;

        /** Latch released by this plugin once it has paused (signal to test). */
        private final CountDownLatch triggerPluginStart;

        /**
         * @param triggerPluginStartLatch Released when plugin pauses.
         * @param triggerRestCmdLatch Awaited until test releases.
         */
        public DelayedStartPluginProvider(CountDownLatch triggerPluginStartLatch,
            CountDownLatch triggerRestCmdLatch) {
            this.triggerPluginStart = triggerPluginStartLatch;
            this.triggerRestCmd = triggerRestCmdLatch;
        }

        /** {@inheritDoc} */
        @Override public String name() {
            return "DelayedStartPlugin";
        }

        /** {@inheritDoc} */
        @Override public void onIgniteStart() {
            super.onIgniteStart();

            triggerPluginStart.countDown();

            log.info("awaiting rest command latch ...");

            try {
                triggerRestCmd.await();
            }
            catch (InterruptedException e) {
                log.error("error in custom plugin", e);
            }

            log.info("finished awaiting rest command latch.");
        }
    }
}
