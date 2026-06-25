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

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.rest.GridRestCommand;
import org.apache.ignite.internal.processors.rest.GridRestResponse;
import org.apache.ignite.internal.processors.rest.handlers.GridRestCommandHandler;
import org.apache.ignite.internal.processors.rest.handlers.probe.GridProbeCommandHandler;
import org.apache.ignite.internal.processors.rest.request.GridRestCacheRequest;
import org.apache.ignite.maintenance.MaintenanceTask;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.PluginProvider;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests the {@code cmd=probe} REST command and its {@code kind} sub-commands.
 *
 * <ul>
 *     <li>Bare {@code cmd=probe} (BC): kernel-started check -> "grid has started" / 503
 *         "grid has not started".</li>
 *     <li>{@code kind=liveness}: the same kernel-started check as bare (same body).</li>
 *     <li>{@code kind=readiness}: 503 "kernel not started" while starting; 200 "ready"
 *         once active and rebalanced; cluster INACTIVE -> 200 "ready"; maintenance mode ->
 *         503 "maintenance mode".</li>
 *     <li>Unknown {@code kind} -> application-level {@code STATUS_FAILED}.</li>
 * </ul>
 *
 * Multi-node, rebalance-gated readiness (the inbound-demand 503 -> 200 latch) is covered by
 * {@link GridProbeReadinessRebalanceTest}.
 */
public class GridProbeCommandTest extends GridCommonAbstractTest {

    private static final int JETTY_PORT = 8080;

    /** Enables persistence (required by the maintenance-mode test). */
    private boolean persistence;

    /** Forces the cluster INACTIVE on start (an in-memory node left un-activated). */
    private boolean inactiveOnStart;

    private final CountDownLatch triggerRestCmdLatch = new CountDownLatch(1);

    private final CountDownLatch triggerPluginStartLatch = new CountDownLatch(1);


    /**
     * {@inheritDoc}
     */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);
        cfg.setConnectorConfiguration(new ConnectorConfiguration());

        if (persistence) {
            cfg.setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true)));
        }

        if (inactiveOnStart)
            cfg.setClusterStateOnStart(ClusterState.INACTIVE);

        if (igniteInstanceName.equals("delayedStart")) {
            PluginProvider delayedStartPluginProvider = new DelayedStartPluginProvider(triggerPluginStartLatch, triggerRestCmdLatch);

            cfg.setPluginProviders(delayedStartPluginProvider);
        }

        return cfg;
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void afterTest() throws Exception {
        stopAllGrids(false);

        cleanPersistenceDir();

        super.afterTest();
    }

    /**
     * <p>Test for the REST probe command (handler level).
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRestProbeCommand() throws Exception {
        startGrid("regular");

        GridRestCommandHandler hnd = new GridProbeCommandHandler((grid("regular")).context());

        GridRestCacheRequest req = new GridRestCacheRequest();
        req.command(GridRestCommand.PROBE);

        IgniteInternalFuture<GridRestResponse> resp = hnd.handleAsync(req);
        resp.get();

        assertEquals(GridRestResponse.STATUS_SUCCESS, resp.result().getSuccessStatus());
        assertEquals("grid has started", resp.result().getResponse());

    }

    /**
     * <p>Test rest cmd=probe command given a non fully started kernal. </p>
     *  <p>1. start the grid on a separate thread w/a plugin that will keep it waiting, at a point after rest http processor is ready,
     *  until signaled to proceed. </p>
     *  <p>2. when the grid.start() has reached the plugin init method(rest http processor has started now), issue a rest command against
     *  the non-fully started kernal. </p>
     *  <p>3. validate that the probe cmd has returned the appropriate erroneous code and message. </p>
     *  <p>4. stop the grid. </p>
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRestProbeCommandGridNotStarted() throws Exception {
        Map<String, Object> probeRestCommandResponse = executeWhileKernelStarting("cmd=probe");

        assertTrue(probeRestCommandResponse.get("error").equals("grid has not started"));
        assertEquals(GridRestResponse.SERVICE_UNAVAILABLE, probeRestCommandResponse.get("successStatus"));
    }

    /**
     * <p>Start a regular grid, issue a cmd=probe rest command, and validate response
     * @throws Exception If failed.
     */
    @Test
    public void testRestProbeCommandGridStarted() throws Exception {
        startGrid("regular");

        Map<String, Object> probeRestCommandResponse;

        probeRestCommandResponse = executeProbeRestRequest();

        assertTrue(probeRestCommandResponse.get("response").equals("grid has started"));
        assertEquals(0, probeRestCommandResponse.get("successStatus"));
    }

    /**
     * {@code kind=liveness} is the same kernel-started check as bare {@code cmd=probe}:
     * a started node -> 200 "grid has started".
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRestProbeCommandLivenessKind() throws Exception {
        startGrid("regular");

        Map<String, Object> resp = executeProbeRestRequest("cmd=probe&kind=liveness");

        assertEquals(0, resp.get("successStatus"));
        assertEquals("grid has started", resp.get("response"));
    }

    /**
     * {@code kind=readiness} on a node whose kernel has not finished starting -> 503
     * "kernel not started".
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRestProbeCommandReadinessKernelNotStarted() throws Exception {
        Map<String, Object> resp = executeWhileKernelStarting("cmd=probe&kind=readiness");

        assertEquals(GridRestResponse.SERVICE_UNAVAILABLE, resp.get("successStatus"));
        assertEquals("kernel not started", resp.get("error"));
    }

    /**
     * {@code kind=readiness} on an active node with nothing to rebalance -> 200 "ready".
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRestProbeCommandReadinessAllClear() throws Exception {
        startGrid("regular").cluster().state(ClusterState.ACTIVE);

        awaitPartitionMapExchange();

        Map<String, Object> resp = executeProbeRestRequest("cmd=probe&kind=readiness");

        assertEquals(0, resp.get("successStatus"));
        assertEquals("ready", resp.get("response"));
    }

    /**
     * {@code kind=readiness} on an INACTIVE cluster -> 200 "ready" (so a k8s rolling update
     * may proceed). Uses an in-memory node forced INACTIVE on start.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRestProbeCommandReadinessInactive() throws Exception {
        inactiveOnStart = true;

        startGrid("regular");

        assertEquals(ClusterState.INACTIVE, grid("regular").cluster().state());

        Map<String, Object> resp = executeProbeRestRequest("cmd=probe&kind=readiness");

        assertEquals(0, resp.get("successStatus"));
        assertEquals("ready", resp.get("response"));
    }

    /**
     * {@code kind=readiness} on a node restarted into maintenance mode -> 503 "maintenance
     * mode" (liveness, by contrast, stays 200). Maintenance is re-evaluated live and wins
     * over the INACTIVE short-circuit (a maintenance node is forced INACTIVE).
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRestProbeCommandReadinessMaintenance() throws Exception {
        persistence = true;

        IgniteEx g = startGrid("regular");

        g.cluster().state(ClusterState.ACTIVE);

        g.context().maintenanceRegistry()
            .registerMaintenanceTask(new MaintenanceTask("probe-readiness-test", "probe readiness MM test", null));

        stopGrid("regular");

        g = startGrid("regular");

        assertTrue("node should be in maintenance mode", g.context().maintenanceRegistry().isMaintenanceMode());

        Map<String, Object> resp = executeProbeRestRequest("cmd=probe&kind=readiness");

        assertEquals(GridRestResponse.SERVICE_UNAVAILABLE, resp.get("successStatus"));
        assertEquals("maintenance mode", resp.get("error"));
    }

    /**
     * Unknown {@code kind} -> application-level {@code STATUS_FAILED} (HTTP 200).
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRestProbeCommandUnknownKind() throws Exception {
        startGrid("regular");

        Map<String, Object> resp = executeProbeRestRequest("cmd=probe&kind=bogus");

        assertEquals(GridRestResponse.STATUS_FAILED, resp.get("successStatus"));
        assertTrue("error must mention unknown probe kind: " + resp.get("error"),
            String.valueOf(resp.get("error")).contains("Unknown probe kind"));
    }

    /**
     * Starts the "delayedStart" grid on a separate thread (a plugin holds it after the REST
     * HTTP processor is up but before the kernel has fully started), issues the given probe
     * query against the not-fully-started kernel, then releases the grid.
     *
     * @param query Query string after {@code /ignite?} (e.g. {@code cmd=probe&kind=readiness}).
     * @return Parsed probe response.
     * @throws Exception If failed.
     */
    private Map<String, Object> executeWhileKernelStarting(String query) throws Exception {
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

        assertTrue("timed out waiting for delayedStart plugin to reach onIgniteStart",
            triggerPluginStartLatch.await(getTestTimeout(), TimeUnit.MILLISECONDS));

        try {
            return executeProbeRestRequest(query);
        }
        finally {
            triggerRestCmdLatch.countDown(); // make sure the grid shuts down
        }
    }

    /**
     * @return Parsed response of bare {@code cmd=probe}.
     * @throws IOException If failed.
     */
    public static Map<String, Object> executeProbeRestRequest() throws IOException {
        return executeProbeRestRequest("cmd=probe");
    }

    /**
     * @param query Query string after {@code /ignite?}.
     * @return Parsed probe response.
     * @throws IOException If failed.
     */
    public static Map<String, Object> executeProbeRestRequest(String query) throws IOException {
        return GridRestHttpClient.get(JETTY_PORT, "/ignite?" + query).body;
    }

    /**
     * This plugin awaits until it is given the signal to process -- thereby allowing an http request
     * against a non fully started kernal.
     */
    public static class DelayedStartPluginProvider extends AbstractTestPluginProvider {

        private final CountDownLatch triggerRestCmd;

        private final CountDownLatch triggerPluginStart;

        public DelayedStartPluginProvider(CountDownLatch triggerPluginStartLatch,
            CountDownLatch triggerRestCmdLatch) {
            this.triggerPluginStart = triggerPluginStartLatch;
            this.triggerRestCmd = triggerRestCmdLatch;
        }

        /**
         * {@inheritDoc}
         */
        @Override public String name() {
            return "DelayedStartPlugin";
        }

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
