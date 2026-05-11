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

import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.rest.GridRestCommand;
import org.apache.ignite.internal.processors.rest.GridRestResponse;
import org.apache.ignite.internal.processors.rest.handlers.drain.DrainStatusResponse;
import org.apache.ignite.internal.processors.rest.handlers.drain.GridDrainCommandHandler;
import org.apache.ignite.internal.processors.rest.request.GridRestDrainRequest;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Handler-level tests for {@link GridDrainCommandHandler}. Per HLD v14 §3,
 * {@code cmd=drain} has three operations: {@code action=start},
 * {@code action=stop}, and {@code action=status}. The bare-GET readiness
 * ladder from v13 is gone — readiness moved to {@code cmd=probe}, exercised
 * by {@code GridProbeCommandTest} and the HTTP-level rebalance tests.
 */
public class GridDrainCommandTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConnectorConfiguration(new ConnectorConfiguration());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids(false);
    }

    /**
     * action=status — returns the {@link DrainStatusResponse} payload with the
     * current flag and (best-effort) connection count. Connection count is
     * {@code 0} in this test because no thin client has connected.
     */
    @Test
    public void testActionStatus() throws Exception {
        startGrid("regular").cluster().state(ClusterState.ACTIVE);

        GridDrainCommandHandler hnd = new GridDrainCommandHandler(grid("regular").context());

        GridRestResponse resp = invoke(hnd, GridDrainCommandHandler.ACTION_STATUS);

        assertEquals(GridRestResponse.STATUS_SUCCESS, resp.getSuccessStatus());
        assertTrue(resp.getResponse() instanceof DrainStatusResponse);

        DrainStatusResponse payload = (DrainStatusResponse)resp.getResponse();

        assertFalse(payload.isDraining());
        assertEquals(0, payload.getActiveThinClients());

        // Trip the flag, then re-read.
        invoke(hnd, GridDrainCommandHandler.ACTION_START);

        GridRestResponse resp2 = invoke(hnd, GridDrainCommandHandler.ACTION_STATUS);

        DrainStatusResponse payload2 = (DrainStatusResponse)resp2.getResponse();

        assertTrue(payload2.isDraining());
    }

    /**
     * action=start sets the drain flag; action=stop clears it (operator
     * rollback path per HLD v14 §7).
     */
    @Test
    public void testActionStartAndStop() throws Exception {
        startGrid("regular").cluster().state(ClusterState.ACTIVE);

        GridDrainCommandHandler hnd = new GridDrainCommandHandler(grid("regular").context());

        // Start.
        GridRestResponse start = invoke(hnd, GridDrainCommandHandler.ACTION_START);

        assertEquals(GridRestResponse.STATUS_SUCCESS, start.getSuccessStatus());
        assertEquals("draining", start.getResponse());
        assertTrue(hnd.drainingFlag());

        // Stop — clears flag, opens cmd=probe back to ready state.
        GridRestResponse stop = invoke(hnd, GridDrainCommandHandler.ACTION_STOP);

        assertEquals(GridRestResponse.STATUS_SUCCESS, stop.getSuccessStatus());
        assertEquals("ready", stop.getResponse());
        assertFalse(hnd.drainingFlag());
    }

    /**
     * Unknown / missing action returns {@code STATUS_FAILED} with a clear
     * error mentioning the expected actions. Per v14 §3, cmd=drain has no
     * bare-GET path.
     */
    @Test
    public void testUnknownAction() throws Exception {
        startGrid("regular").cluster().state(ClusterState.ACTIVE);

        GridDrainCommandHandler hnd = new GridDrainCommandHandler(grid("regular").context());

        GridRestResponse resp = invoke(hnd, null);

        assertEquals(GridRestResponse.STATUS_FAILED, resp.getSuccessStatus());
        assertNotNull(resp.getError());
        assertTrue("error must list expected actions: " + resp.getError(),
            resp.getError().contains("start") && resp.getError().contains("stop") && resp.getError().contains("status"));
    }

    /**
     * @param hnd Handler.
     * @param action Sub-action.
     * @return Resolved response.
     */
    private static GridRestResponse invoke(GridDrainCommandHandler hnd, String action) throws Exception {
        GridRestDrainRequest req = new GridRestDrainRequest();

        req.command(GridRestCommand.DRAIN);
        req.action(action);

        IgniteInternalFuture<GridRestResponse> fut = hnd.handleAsync(req);

        return fut.get();
    }
}
