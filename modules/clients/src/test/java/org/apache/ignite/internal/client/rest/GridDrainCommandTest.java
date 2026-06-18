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

import org.apache.ignite.ShutdownPolicy;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.processors.rest.GridRestCommand;
import org.apache.ignite.internal.processors.rest.GridRestResponse;
import org.apache.ignite.internal.processors.rest.handlers.drain.DrainStatusResponse;
import org.apache.ignite.internal.processors.rest.handlers.drain.DrainUniqueDataResponse;
import org.apache.ignite.internal.processors.rest.handlers.drain.GridDrainCommandHandler;
import org.apache.ignite.internal.processors.rest.request.GridRestDrainRequest;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.internal.processors.rest.request.GridRestDrainRequest.Action.START;
import static org.apache.ignite.internal.processors.rest.request.GridRestDrainRequest.Action.STATUS;
import static org.apache.ignite.internal.processors.rest.request.GridRestDrainRequest.Action.STOP;

/**
 * Handler-level tests for {@link GridDrainCommandHandler}.
 * {@code cmd=drain} has three operations: {@code action=start},
 * {@code action=stop}, and {@code action=status}, plus the unique-data guard +
 * {@code force} on {@code action=start} under {@code ShutdownPolicy.GRACEFUL}.
 * Readiness lives behind {@code cmd=probe&kind=readiness}.
 */
public class GridDrainCommandTest extends GridCommonAbstractTest {
    /** Shutdown policy applied to the next started node (Ignite default is IMMEDIATE). */
    private ShutdownPolicy shutdownPlc = ShutdownPolicy.IMMEDIATE;

    /** Optional cache configured on the next started node (drives the unique-data guard). */
    private CacheConfiguration<Object, Object> cacheCfg;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConnectorConfiguration(new ConnectorConfiguration());

        cfg.setShutdownPolicy(shutdownPlc);

        if (cacheCfg != null)
            cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids(false);

        // Reset config knobs so they don't leak between tests.
        shutdownPlc = ShutdownPolicy.IMMEDIATE;
        cacheCfg = null;

        super.afterTest();
    }

    /**
     * action=status - returns the {@link DrainStatusResponse} payload with the
     * current flag. Body is {@code {draining}} only.
     */
    @Test
    public void testActionStatus() throws Exception {
        startGrid("regular").cluster().state(ClusterState.ACTIVE);

        GridDrainCommandHandler hnd = new GridDrainCommandHandler(grid("regular").context());

        GridRestResponse resp = invoke(hnd, STATUS);

        assertEquals(GridRestResponse.STATUS_SUCCESS, resp.getSuccessStatus());
        assertTrue(resp.getResponse() instanceof DrainStatusResponse);

        DrainStatusResponse payload = (DrainStatusResponse)resp.getResponse();

        assertFalse(payload.isDraining());

        // Trip the flag, then re-read.
        invoke(hnd, START);

        GridRestResponse resp2 = invoke(hnd, STATUS);

        DrainStatusResponse payload2 = (DrainStatusResponse)resp2.getResponse();

        assertTrue(payload2.isDraining());
    }

    /**
     * action=start sets the drain flag; action=stop clears it (operator
     * rollback path). Default policy (IMMEDIATE) and no user caches - the
     * unique-data guard does not apply.
     */
    @Test
    public void testActionStartAndStop() throws Exception {
        startGrid("regular").cluster().state(ClusterState.ACTIVE);

        GridDrainCommandHandler hnd = new GridDrainCommandHandler(grid("regular").context());

        // Start.
        GridRestResponse start = invoke(hnd, START);

        assertEquals(GridRestResponse.STATUS_SUCCESS, start.getSuccessStatus());
        assertEquals("draining", start.getResponse());
        assertTrue(hnd.drainingFlag());

        // Stop - clears flag, opens cmd=probe&kind=readiness back to ready state.
        GridRestResponse stop = invoke(hnd, STOP);

        assertEquals(GridRestResponse.STATUS_SUCCESS, stop.getSuccessStatus());
        assertEquals("ready", stop.getResponse());
        assertFalse(hnd.drainingFlag());
    }

    /**
     * Missing action (handler invoked with a {@code null} action) returns
     * {@code STATUS_FAILED} naming the expected actions. An <em>unknown</em> action
     * string never reaches the handler - it is rejected at the protocol layer; see
     * {@link #testHttpUnknownActionRejected()}.
     */
    @Test
    public void testMissingAction() throws Exception {
        startGrid("regular").cluster().state(ClusterState.ACTIVE);

        GridDrainCommandHandler hnd = new GridDrainCommandHandler(grid("regular").context());

        GridRestResponse resp = invoke(hnd, null);

        assertEquals(GridRestResponse.STATUS_FAILED, resp.getSuccessStatus());
        assertNotNull(resp.getError());
        assertTrue("error must list expected actions: " + resp.getError(),
            resp.getError().contains("start") && resp.getError().contains("stop") && resp.getError().contains("status"));
    }

    /**
     * Unknown action over HTTP is rejected by {@code createRequest} at the protocol layer (before the
     * handler / auth): HTTP 200 with {@code STATUS_FAILED} and a message naming the valid actions.
     * No security plugin here, so this is the pure parse-error path.
     */
    @Test
    public void testHttpUnknownActionRejected() throws Exception {
        startGrid("regular").cluster().state(ClusterState.ACTIVE);

        GridRestHttpClient.Response resp = GridRestHttpClient.get(restPort("regular"), "/ignite?cmd=drain&action=bogus");

        log.info("malformed drain action response: code=" + resp.code + " body=" + resp.body);

        assertEquals(200, resp.code);
        assertEquals(GridRestResponse.STATUS_FAILED, resp.body.get("successStatus"));
        assertTrue("error must name the valid actions: " + resp.body.get("error"),
            String.valueOf(resp.body.get("error")).contains("start"));
    }

    /**
     * Unique-data guard: under {@code ShutdownPolicy.GRACEFUL}, a node that is the
     * only owner of a cache group's partitions (here a {@code backups=1} cache on a
     * single node - the backup cannot be placed) refuses {@code action=start} with a
     * {@code 503}/{@link DrainUniqueDataResponse} and does NOT flip the flag.
     * {@code force=true} bypasses the guard.
     */
    @Test
    public void testUniqueDataGuardOnStartUnderGraceful() throws Exception {
        shutdownPlc = ShutdownPolicy.GRACEFUL;
        cacheCfg = new CacheConfiguration<>("guardCache").setCacheMode(PARTITIONED).setBackups(1);

        startGrid("regular").cluster().state(ClusterState.ACTIVE);

        awaitPartitionMapExchange();

        GridDrainCommandHandler hnd = new GridDrainCommandHandler(grid("regular").context());

        // Guard blocks: sole owner of guardCache partitions.
        GridRestResponse blocked = invoke(hnd, START, false);

        assertEquals("guard must return 503 SERVICE_UNAVAILABLE: " + blocked.getResponse(),
            GridRestResponse.SERVICE_UNAVAILABLE, blocked.getSuccessStatus());
        assertFalse("flag must NOT flip when the guard blocks", hnd.drainingFlag());
        assertTrue("guard body must be DrainUniqueDataResponse: " + blocked.getResponse(),
            blocked.getResponse() instanceof DrainUniqueDataResponse);

        DrainUniqueDataResponse body = (DrainUniqueDataResponse)blocked.getResponse();

        assertTrue(body.isUniqueDataHeld());
        assertTrue("guard must name the sole-owned group: " + body.getCacheGroups(),
            body.getCacheGroups().contains("guardCache"));

        // force=true bypasses the guard.
        GridRestResponse forced = invoke(hnd, START, true);

        assertEquals(GridRestResponse.STATUS_SUCCESS, forced.getSuccessStatus());
        assertEquals("draining", forced.getResponse());
        assertTrue(hnd.drainingFlag());
    }

    /**
     * Under {@code ShutdownPolicy.IMMEDIATE} the unique-data guard is NOT applied -
     * the same sole-owner topology drains without {@code force} (Mode B: data loss
     * accepted by design).
     */
    @Test
    public void testUniqueDataGuardSkippedUnderImmediate() throws Exception {
        shutdownPlc = ShutdownPolicy.IMMEDIATE;
        cacheCfg = new CacheConfiguration<>("guardCache").setCacheMode(PARTITIONED).setBackups(1);

        startGrid("regular").cluster().state(ClusterState.ACTIVE);

        awaitPartitionMapExchange();

        GridDrainCommandHandler hnd = new GridDrainCommandHandler(grid("regular").context());

        GridRestResponse start = invoke(hnd, START, false);

        assertEquals(GridRestResponse.STATUS_SUCCESS, start.getSuccessStatus());
        assertEquals("draining", start.getResponse());
        assertTrue(hnd.drainingFlag());
    }

    /**
     * @param hnd Handler.
     * @param action Sub-action ({@code null} = absent).
     * @return Resolved response.
     */
    private GridRestResponse invoke(GridDrainCommandHandler hnd, GridRestDrainRequest.Action action)
        throws Exception {
        return invoke(hnd, action, false);
    }

    /**
     * @param hnd Handler.
     * @param action Sub-action ({@code null} = absent).
     * @param force {@code force} flag.
     * @return Resolved response.
     */
    private GridRestResponse invoke(GridDrainCommandHandler hnd, GridRestDrainRequest.Action action,
        boolean force) throws Exception {
        GridRestDrainRequest req = new GridRestDrainRequest();

        req.command(GridRestCommand.DRAIN);
        req.action(action);
        req.force(force);

        IgniteInternalFuture<GridRestResponse> fut = hnd.handleAsync(req);

        return fut.get(getTestTimeout());
    }

    /** @return REST (Jetty) port the named node actually bound (no brittle hardcoded port). */
    private int restPort(String name) {
        return (Integer)grid(name).context().nodeAttribute(IgniteNodeAttributes.ATTR_REST_JETTY_PORT);
    }
}
