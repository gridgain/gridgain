/*
 * Copyright 2026 GridGain Systems, Inc. and Contributors.
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

import java.io.IOException;
import java.util.Map;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteState;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * HTTP-level tests for {@code cmd=supply-status&shutdown=*}: lenient {@code shutdown} parsing, the
 * informational read, the idle gate-pass clean stop, and the in-flight-supply 503.
 */
public class GridSupplyStatusShutdownTest extends GridCommonAbstractTest {
    /** Default Jetty REST port (the first node started → 8080). */
    private static final int JETTY_PORT = 8080;

    /** Jetty REST port of the joiner (second node started → 8081). */
    private static final int JOINER_PORT = 8081;

    /** Cache/group whose rebalance messages the SPI holds for the in-flight 503 tests. */
    private static final String SUPPLY_CACHE = "supplyTest";

    /** When {@code true}, hold inbound demand (demander test) instead of outbound supply (supplier test). */
    private boolean blockDemand;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConnectorConfiguration(new ConnectorConfiguration());

        // Hold a SUPPLY_CACHE rebalance message so a node stays mid-exchange until released. Inert for
        // the single-node tests, which never create SUPPLY_CACHE so nothing matches.
        TestRecordingCommunicationSpi spi = new TestRecordingCommunicationSpi();

        if (blockDemand)
            spi.blockMessages((node, msg) -> msg instanceof GridDhtPartitionDemandMessage
                && ((GridDhtPartitionDemandMessage)msg).groupId() == CU.cacheId(SUPPLY_CACHE));
        else
            spi.blockMessages((node, msg) -> msg instanceof GridDhtPartitionSupplyMessage
                && ((GridDhtPartitionSupplyMessage)msg).groupId() == CU.cacheId(SUPPLY_CACHE));

        cfg.setCommunicationSpi(spi);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids(false);

        super.afterTest();
    }

    /**
     * Gate-pass success path: {@code shutdown=true} on an idle single node
     * returns 200 and then triggers a clean graceful stop on the dedicated
     * {@code supply-status-shutdown-trigger} daemon thread. The node MUST
     * reach {@link IgniteState#STOPPED} (a clean stop), NOT
     * {@code STOPPED_ON_FAILURE}, confirming the stop no longer runs on a grid
     * pool that the stop itself tears down, and that the 200 is flushed first.
     *
     * <p>Single node, ACTIVE, no in-flight supply/PME and the OSS
     * replication stub reports inactive → the gate passes.</p>
     */
    @Test
    public void testIdleShutdownTrue_stopsNodeCleanly() throws Exception {
        IgniteEx grid = startGrid("regular");
        grid.cluster().state(ClusterState.ACTIVE);

        final String name = grid.name();

        awaitPartitionMapExchange();

        // Gate passes (idle) → 200 with the supply-status body; stop is triggered after the flush.
        GridRestHttpClient.Response resp = GridRestHttpClient.get(JETTY_PORT, "/ignite?cmd=supply-status&shutdown=true");
        assertEquals(200, resp.code);
        assertEquals(0, resp.body.get("successStatus"));

        Object payload = resp.body.get("response");
        assertTrue("response not a map: " + payload, payload instanceof Map);
        assertEquals(Boolean.FALSE, ((Map<?, ?>)payload).get("supplying"));

        // The dedicated daemon thread runs Ignition.stop(name, true); wait for a clean STOP.
        assertTrue("node did not reach STOPPED after shutdown=true",
            GridTestUtils.waitForCondition(
                () -> IgnitionEx.state(name) == IgniteState.STOPPED, 30_000));

        // Clean stop — NOT a failure stop.
        assertEquals(IgniteState.STOPPED, IgnitionEx.state(name));
    }

    /**
     * Lenient parse (Boolean.parseBoolean, matching the rest of the REST surface): a non-canonical
     * {@code shutdown} token takes the informational path — 200 with the supply-status body — and
     * does NOT trigger a shutdown. Only a literal case-insensitive {@code "true"} fires the gate, so
     * a sloppy token can never stop the node by accident.
     */
    @Test
    public void testNonCanonicalShutdownTokenIsLenientInformational() throws Exception {
        IgniteEx grid = startGrid("regular");
        grid.cluster().state(ClusterState.ACTIVE);

        final String name = grid.name();

        for (String tok : new String[] {"yes", "1", "on", "tru"}) {
            GridRestHttpClient.Response resp = GridRestHttpClient.get(JETTY_PORT, "/ignite?cmd=supply-status&shutdown=" + tok);

            assertEquals("shutdown=" + tok + " must take the informational path (200): " + resp.body,
                200, resp.code);
            assertEquals("shutdown=" + tok + " must not be an error: " + resp.body,
                0, resp.body.get("successStatus"));
            assertTrue("informational body expected for shutdown=" + tok + ": " + resp.body,
                resp.body.get("response") instanceof Map);
        }

        // None of the garbage tokens triggered a shutdown — the node is still running.
        assertEquals("non-canonical shutdown tokens must NOT stop the node",
            IgniteState.STARTED, IgnitionEx.state(name));
    }

    /**
     * {@code shutdown=false} returns 200 with the body shape
     * {@code {supplying, supplyingCacheGroups, activeThinClients, ...}} — same as the bare-GET form.
     * Two successive calls return identical bodies (no side-effect from the {@code shutdown=false}
     * read path).
     */
    @Test
    public void testShutdownFalse_idempotentInformational() throws Exception {
        startGrid("regular").cluster().state(ClusterState.ACTIVE);

        GridRestHttpClient.Response first = GridRestHttpClient.get(JETTY_PORT, "/ignite?cmd=supply-status&shutdown=false");
        assertEquals(200, first.code);
        assertEquals(0, first.body.get("successStatus"));

        Object firstPayload = first.body.get("response");
        assertTrue("response not a map: " + firstPayload, firstPayload instanceof Map);
        Map<?, ?> fp = (Map<?, ?>)firstPayload;
        assertEquals(Boolean.FALSE, fp.get("supplying"));
        assertTrue("activeThinClients must be present", fp.containsKey("activeThinClients"));

        // Second call — same body, no state change.
        GridRestHttpClient.Response second = GridRestHttpClient.get(JETTY_PORT, "/ignite?cmd=supply-status&shutdown=false");
        assertEquals(200, second.code);
        Map<?, ?> sp = (Map<?, ?>)second.body.get("response");
        assertEquals(fp.get("supplying"), sp.get("supplying"));
        assertEquals(fp.get("activeThinClients"), sp.get("activeThinClients"));
    }

    /**
     * Case-insensitive strict parse: {@code shutdown=FALSE} and {@code shutdown=False} both parse
     * cleanly to {@link Boolean#FALSE} (informational path).
     */
    @Test
    public void testShutdownFalse_caseInsensitive() throws Exception {
        startGrid("regular").cluster().state(ClusterState.ACTIVE);

        for (String v : new String[]{"FALSE", "False", "false"}) {
            GridRestHttpClient.Response resp = GridRestHttpClient.get(JETTY_PORT, "/ignite?cmd=supply-status&shutdown=" + v);
            assertEquals("shutdown=" + v + " must parse cleanly: " + resp.body, 200, resp.code);
            assertEquals(0, resp.body.get("successStatus"));
        }
    }

    /**
     * In-flight outbound supply → 503 (retryable), shutdown NOT triggered. The first node (port
     * 8080) seeds a {@code backups=1} cache; a second node joins and demands a copy, and the first
     * node's {@link GridDhtPartitionSupplyMessage} is held by the SPI so it is mid-supply
     * ({@code isSupply()==true}). {@code cmd=supply-status&shutdown=true} on the supplier must 503,
     * name the supplying group, and leave the node running.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSupplierReturns503() throws Exception {
        IgniteEx supplier = startGrid("regular");
        supplier.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Integer, Integer> cache = supplier.getOrCreateCache(
            new CacheConfiguration<Integer, Integer>(SUPPLY_CACHE)
                .setBackups(1)
                .setRebalanceMode(CacheRebalanceMode.ASYNC)
                .setRebalanceBatchSize(256));

        for (int i = 0; i < 5_000; i++)
            cache.put(i, i);

        awaitPartitionMapExchange();

        // Second node joins and demands a backup copy; the supplier holds its supply message.
        startGrid("regular1");

        TestRecordingCommunicationSpi.spi(supplier).waitForBlocked();

        GridRestHttpClient.Response supplying = GridRestHttpClient.get(JETTY_PORT, "/ignite?cmd=supply-status&shutdown=true");

        assertEquals("shutdown gate must 503 while supplying: " + supplying.body, 503, supplying.code);
        assertEquals(503, supplying.body.get("successStatus"));
        assertTrue("503 reason must name the supplying group: " + supplying.body.get("error"),
            String.valueOf(supplying.body.get("error")).contains("supplying"));

        // Supplier still alive — a 503 gate path never triggers Ignition.stop.
        GridRestHttpClient.Response live = GridRestHttpClient.get(JETTY_PORT, "/ignite?cmd=probe&kind=liveness");
        assertEquals("supplier must stay alive after a 503 shutdown gate: " + live.body, 200, live.code);

        TestRecordingCommunicationSpi.spi(supplier).stopBlock();

        awaitPartitionMapExchange();
    }

    /**
     * Inbound rebalance (demander) → 503 (retryable), shutdown NOT triggered. The first node seeds a
     * {@code backups=1} cache; a joiner (port 8081) demands a copy, and the joiner's
     * {@link GridDhtPartitionDemandMessage} is held by the SPI so it stays a demander
     * ({@code rebalanceFuture().isDone()==false}). {@code shutdown=true} on the joiner must 503,
     * name the rebalance, and leave the node running.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDemanderReturns503() throws Exception {
        blockDemand = true;

        IgniteEx node0 = startGrid("regular");
        node0.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Integer, Integer> cache = node0.getOrCreateCache(
            new CacheConfiguration<Integer, Integer>(SUPPLY_CACHE)
                .setBackups(1)
                .setRebalanceMode(CacheRebalanceMode.ASYNC)
                .setRebalanceBatchSize(256));

        for (int i = 0; i < 5_000; i++)
            cache.put(i, i);

        awaitPartitionMapExchange();

        // Joiner demands a backup copy; its demand is held → it stays mid inbound-rebalance.
        IgniteEx joiner = startGrid("regular1");

        TestRecordingCommunicationSpi.spi(joiner).waitForBlocked();

        GridRestHttpClient.Response demanding = GridRestHttpClient.get(JOINER_PORT, "/ignite?cmd=supply-status&shutdown=true");

        assertEquals("shutdown gate must 503 while demanding: " + demanding.body, 503, demanding.code);
        assertEquals(503, demanding.body.get("successStatus"));
        assertTrue("503 reason must name the inbound rebalance: " + demanding.body.get("error"),
            String.valueOf(demanding.body.get("error")).contains("rebalancing"));

        // Joiner still alive — a 503 gate path never triggers Ignition.stop.
        GridRestHttpClient.Response live = GridRestHttpClient.get(JOINER_PORT, "/ignite?cmd=probe&kind=liveness");
        assertEquals("demander must stay alive after a 503 shutdown gate: " + live.body, 200, live.code);

        TestRecordingCommunicationSpi.spi(joiner).stopBlock();

        awaitPartitionMapExchange();
    }

    /**
     * {@code activeThinClients} reflects the live thin-client connection count (spec Scenario 2):
     * {@code 0} with none open, {@code N} with N open, back to {@code 0} once they close. The Jetty
     * REST request and any JDBC/ODBC sessions are not counted (the {@code THIN_CLIENT} filter).
     *
     * @throws Exception If failed.
     */
    @Test
    public void testActiveThinClientsReflectsOpenConnections() throws Exception {
        startGrid("regular").cluster().state(ClusterState.ACTIVE);

        assertEquals("no thin clients at baseline", 0, activeThinClients());

        try (IgniteClient c1 = Ignition.startClient(thinClientCfg())) {
            waitForActiveThinClients(1);

            try (IgniteClient c2 = Ignition.startClient(thinClientCfg())) {
                waitForActiveThinClients(2);
            }

            // c2 closed → count drops back to one (server-side session close is observed asynchronously).
            waitForActiveThinClients(1);
        }

        // Both closed → back to the baseline.
        waitForActiveThinClients(0);
    }

    /** @return thin-client config for the first node's connector (single node → one connection per client). */
    private static ClientConfiguration thinClientCfg() {
        return new ClientConfiguration().setAddresses("127.0.0.1:10800..10809");
    }

    /**
     * @return {@code activeThinClients} reported by the informational {@code cmd=supply-status}.
     * @throws IOException If the request failed.
     */
    private static int activeThinClients() throws IOException {
        Object payload = GridRestHttpClient.get(JETTY_PORT, "/ignite?cmd=supply-status").body.get("response");

        return ((Number)((Map<?, ?>)payload).get("activeThinClients")).intValue();
    }

    /**
     * @param expected Expected active thin-client count.
     * @throws Exception If the count is not reached within the timeout.
     */
    private static void waitForActiveThinClients(int expected) throws Exception {
        assertTrue("activeThinClients did not reach " + expected,
            GridTestUtils.waitForCondition(() -> {
                try {
                    return activeThinClients() == expected;
                }
                catch (IOException e) {
                    return false;
                }
            }, 10_000));
    }
}
