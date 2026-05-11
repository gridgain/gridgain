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
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Multi-node HTTP-level tests for the rebalance-aware branches of HLD v14
 * {@code cmd=probe} (which returns 503 while a node has active outbound
 * supply) and {@code cmd=supply-status} (which reports the same condition
 * informationally).
 *
 * <p>Single-node coverage lives in {@link GridDrainAndSupplyStatusHttpTest};
 * this class complements it by stalling rebalance with a
 * {@link TestRecordingCommunicationSpi} so we can observe the HTTP response
 * while a supplier node is mid-supply.</p>
 *
 * <h3>The rebalance-stall recipe (non-obvious — read before editing)</h3>
 * <ul>
 *   <li><b>3 existing nodes + 1 joining node</b>, persistence on, {@code backups=1}
 *       PARTITIONED cache. Backups=1 with 3 servers means partitions actually have
 *       multiple copies, so the joining node has a real demander/supplier exchange.</li>
 *   <li><b>{@link CacheRebalanceMode#ASYNC}, NOT {@code SYNC}</b>. {@code SYNC} blocks
 *       the joining node's {@code onKernalStart} until rebalance completes; the REST
 *       start-latch never counts down and HTTP requests against the joining node queue
 *       forever.</li>
 *   <li><b>{@code setRebalanceBatchSize(256)}</b>. The default 512 KB batch size completes
 *       in one batch, so {@code isSupply()} flips back to {@code false} instantly even
 *       with the SPI holding the message. Small batches force multi-batch supply.</li>
 *   <li><b>Cache-group-scoped block predicate</b>. Blocking ALL
 *       {@link GridDhtPartitionSupplyMessage}s stalls {@code ignite-sys-cache} too and
 *       node 3 never finishes start. Block only the test cache group's supply.</li>
 *   <li><b>Baseline auto-adjust</b>. With persistence, the baseline pins to the original
 *       3 nodes; node 3 joins topology but isn't in baseline → no partitions assigned →
 *       no supply demand → {@code waitForBlocked()} hangs. Enable
 *       {@code baselineAutoAdjustEnabled(true)} + {@code baselineAutoAdjustTimeout(0)}
 *       so node 3 is added immediately.</li>
 *   <li>After the block is in place, start node 3, then {@code waitForBlocked()} on the
 *       suppliers' SPIs. At that moment node 0 has {@code isSupply() == true} for the
 *       test group.</li>
 *   <li>After all assertions: {@code stopBlock()} on each existing node's SPI,
 *       {@code awaitPartitionMapExchange()}, then re-curl to assert the
 *       {@code false} / 200 transition.</li>
 * </ul>
 */
public class GridDrainAndSupplyStatusRebalanceTest extends GridCommonAbstractTest {
    /** Cache name used for the rebalance-controlled cache group. */
    private static final String CACHE = "test";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConnectorConfiguration(new ConnectorConfiguration());

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(true)));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids(false);

        cleanPersistenceDir();
    }

    /**
     * Bring up nodes 0/1/2, activate, create a partitioned-with-backups cache configured
     * for ASYNC rebalance with small batches, seed it, and let the initial rebalance settle.
     *
     * @throws Exception If failed.
     */
    private void seedCluster() throws Exception {
        IgniteEx g0 = startGrid(0);
        startGrid(1);
        startGrid(2);

        g0.cluster().state(ClusterState.ACTIVE);

        // Auto-adjust baseline so node 3 will be added to the baseline (and thus assigned
        // partitions, triggering rebalance) as soon as it joins. Without this, persistence
        // pins the baseline at the 3-node set and node 3 never demands supply.
        g0.cluster().baselineAutoAdjustEnabled(true);
        g0.cluster().baselineAutoAdjustTimeout(0);

        IgniteCache<Integer, Integer> cache = g0.getOrCreateCache(
            new CacheConfiguration<Integer, Integer>(CACHE)
                .setBackups(1)
                .setRebalanceMode(CacheRebalanceMode.ASYNC)
                .setRebalanceBatchSize(256));

        for (int i = 0; i < 5_000; i++)
            cache.put(i, i);

        awaitPartitionMapExchange();
    }

    /**
     * Install a cache-group-scoped supply-message block on every existing supplier so the
     * about-to-join node 3 will have MOVING partitions and node 0 will be {@code isSupply()}.
     */
    private void blockSupplyForTestGroup() {
        final int grpId = grid(0).cachex(CACHE).context().groupId();

        for (int i = 0; i < 3; i++) {
            TestRecordingCommunicationSpi.spi(grid(i)).blockMessages((node, msg) ->
                msg instanceof GridDhtPartitionSupplyMessage
                    && ((GridDhtPartitionSupplyMessage)msg).groupId() == grpId);
        }
    }

    /** Release the supply block on every existing supplier so rebalance can finish. */
    private void unblockSupply() {
        for (int i = 0; i < 3; i++)
            TestRecordingCommunicationSpi.spi(grid(i)).stopBlock();
    }

    /**
     * Per HLD v14 §2, {@code cmd=probe} returns 503 if any non-system cache group has
     * active outbound supply. While supply is blocked node 0 is mid-supply for the
     * {@code test} group, so {@code GET /ignite?cmd=probe} on node 0 must respond 503
     * with {@code error} containing the cache group name. After unblock + PME, node 0
     * must transition back to 200 "grid has started".
     *
     * @throws Exception If failed.
     */
    @Test
    public void testProbeReturns503OnSupplier() throws Exception {
        seedCluster();

        blockSupplyForTestGroup();

        // Start node 3 in a side thread; it'll demand supply from node 0/1/2.
        Thread joiner = new Thread(() -> {
            try {
                startGrid(3);
            }
            catch (Exception e) {
                log.error("error starting node 3", e);
            }
        }, "joiner-3");

        joiner.start();

        // Wait for at least one supply message intercepted on node 0 (sufficient evidence
        // that node 0 has isSupply() == true for the test group right now).
        TestRecordingCommunicationSpi.spi(grid(0)).waitForBlocked();

        // Node 0 (supplier): cmd=probe must return 503 with reason mentioning the supply.
        Resp probeOnSupplier = http(8080, "GET", "/ignite?cmd=probe");

        log.info("node0 probe response: code=" + probeOnSupplier.code + " body=" + probeOnSupplier.body);

        assertEquals(503, probeOnSupplier.code);
        assertEquals(503, probeOnSupplier.body.get("successStatus"));
        Object err = probeOnSupplier.body.get("error");
        assertNotNull("error field missing on 503 body: " + probeOnSupplier.body, err);
        assertTrue("probe error must mention supply: " + err,
            String.valueOf(err).startsWith("supplying:") || String.valueOf(err).contains(CACHE));

        // Release the block and let rebalance complete.
        unblockSupply();

        joiner.join(60_000);

        awaitPartitionMapExchange();

        // After rebalance node 0 must transition to 200 "grid has started".
        Resp probeAfter = http(8080, "GET", "/ignite?cmd=probe");

        log.info("node0 post-rebalance probe: code=" + probeAfter.code + " body=" + probeAfter.body);

        assertEquals(200, probeAfter.code);
        assertEquals(0, probeAfter.body.get("successStatus"));
        assertEquals("grid has started", probeAfter.body.get("response"));
    }

    /**
     * While supply is blocked, node 0 is mid-supply for the {@code test} group, so
     * {@code GET /ignite?cmd=supply-status} on node 0 must return 200 with
     * {@code X-Supplying: true}, body {@code supplying:true}, and {@code supplyingCacheGroups}
     * containing {@code "test"}. After unblock + PME the same query must report
     * {@code X-Supplying: false} and an empty {@code supplyingCacheGroups}.
     *
     * <p>Also verifies the v14 §2 invariant that {@code cmd=probe} returns 503 on the
     * supplier during the same window — {@code cmd=supply-status} is informational
     * (always 200) while {@code cmd=probe} is the gating signal.</p>
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSupplyStatusReturnsTrueDuringRebalance() throws Exception {
        seedCluster();

        blockSupplyForTestGroup();

        // Start node 3 — it'll demand supply, the SPI on node 0 holds the message.
        Thread joiner = new Thread(() -> {
            try {
                startGrid(3);
            }
            catch (Exception e) {
                log.error("error starting node 3", e);
            }
        }, "joiner-3");

        joiner.start();

        TestRecordingCommunicationSpi.spi(grid(0)).waitForBlocked();

        // Node 0 is now supplying.
        Resp supplying = http(8080, "GET", "/ignite?cmd=supply-status");

        log.info("node0 supplying response: code=" + supplying.code
            + " X-Supplying=" + supplying.headerOf("X-Supplying")
            + " body=" + supplying.body);

        assertEquals(200, supplying.code);
        assertEquals("true", supplying.headerOf("X-Supplying"));

        Object payload = supplying.body.get("response");
        assertTrue("response payload not a map: " + payload, payload instanceof Map);

        Map<?, ?> p = (Map<?, ?>)payload;
        assertEquals(Boolean.TRUE, p.get("supplying"));

        Object groups = p.get("supplyingCacheGroups");
        assertTrue("supplyingCacheGroups not a list: " + groups, groups instanceof List);
        assertTrue("expected '" + CACHE + "' in supplyingCacheGroups, got " + groups,
            ((List<?>)groups).contains(CACHE));

        // v14 invariant: cmd=probe on the same supplier returns 503 in the same window.
        Resp probe = http(8080, "GET", "/ignite?cmd=probe");
        assertEquals("cmd=probe must return 503 on a supplier per HLD v14 §2: " + probe.body,
            503, probe.code);

        // Release the block and let rebalance complete.
        unblockSupply();

        joiner.join(60_000);

        awaitPartitionMapExchange();

        // After rebalance node 0 must report idle.
        Resp idle = http(8080, "GET", "/ignite?cmd=supply-status");

        log.info("node0 post-rebalance supply-status: code=" + idle.code
            + " X-Supplying=" + idle.headerOf("X-Supplying")
            + " body=" + idle.body);

        assertEquals(200, idle.code);
        assertEquals("false", idle.headerOf("X-Supplying"));

        Object idlePayload = idle.body.get("response");
        assertTrue("response payload not a map: " + idlePayload, idlePayload instanceof Map);

        Map<?, ?> ip = (Map<?, ?>)idlePayload;
        assertEquals(Boolean.FALSE, ip.get("supplying"));

        Object idleGroups = ip.get("supplyingCacheGroups");
        assertTrue("supplyingCacheGroups not a list: " + idleGroups, idleGroups instanceof List);
        assertTrue("expected empty supplyingCacheGroups, got " + idleGroups,
            ((List<?>)idleGroups).isEmpty());
    }

    /**
     * @param port Jetty port to hit.
     * @param method HTTP method.
     * @param pathQ Path with query string (no host/port).
     * @return Parsed response.
     * @throws IOException If the connection failed.
     */
    private static Resp http(int port, String method, String pathQ) throws IOException {
        URL url = new URL("http://localhost:" + port + pathQ);

        HttpURLConnection conn = (HttpURLConnection)url.openConnection();
        conn.setConnectTimeout(5_000);
        conn.setReadTimeout(15_000);
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
}
