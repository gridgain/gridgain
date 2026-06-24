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
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.rest.GridRestCommand;
import org.apache.ignite.internal.processors.rest.GridRestResponse;
import org.apache.ignite.internal.processors.rest.handlers.drain.GridSupplyStatusCommandHandler;
import org.apache.ignite.internal.processors.rest.handlers.drain.SupplyStatusResponse;
import org.apache.ignite.internal.processors.rest.request.GridRestRequest;
import org.apache.ignite.internal.processors.rest.request.GridRestSupplyStatusRequest;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Handler-level tests for {@link GridSupplyStatusCommandHandler}: the informational body, INACTIVE
 * safety, and the {@code shutdown=true} unique-data guard / force-bypass / idempotency. The shutdown
 * trigger fires only when the {@code afterWrite} hook runs in the Jetty layer, so a
 * {@code handleAsync().get()} call observes the gate decision (status + presence of the hook) without
 * stopping the grid. The HTTP trigger and the in-flight-rebalance 503s live in
 * {@code GridSupplyStatusShutdownTest}.
 */
public class GridSupplyStatusCommandTest extends GridCommonAbstractTest {
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

        if (igniteInstanceName.equals("persistent")) {
            cfg.setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true)));
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids(false);
        cleanPersistenceDir();

        shutdownPlc = ShutdownPolicy.IMMEDIATE;
        cacheCfg = null;

        super.afterTest();
    }

    /** Steady single node, no caches: supplying:false, empty groups, uniqueDataHeld:false. */
    @Test
    public void testSteadyStateNotSupplying() throws Exception {
        startGrid("regular").cluster().state(ClusterState.ACTIVE);

        GridSupplyStatusCommandHandler hnd = new GridSupplyStatusCommandHandler(grid("regular").context());

        GridRestResponse resp = invoke(hnd);

        assertEquals(GridRestResponse.STATUS_SUCCESS, resp.getSuccessStatus());
        assertTrue(resp.getResponse() instanceof SupplyStatusResponse);

        SupplyStatusResponse payload = (SupplyStatusResponse)resp.getResponse();

        assertFalse(payload.isSupplying());
        assertTrue(payload.getSupplyingCacheGroups().isEmpty());
        assertFalse(payload.isUniqueDataHeld());
        assertTrue(payload.getUniqueDataCacheGroups().isEmpty());
    }

    /** INACTIVE cluster: handler must not throw iterating cache groups before activation. */
    @Test
    public void testInactiveClusterNoException() throws Exception {
        startGrid("persistent");

        assertFalse(ClusterState.active(grid("persistent").context().state().clusterState().state()));

        GridSupplyStatusCommandHandler hnd = new GridSupplyStatusCommandHandler(grid("persistent").context());

        GridRestResponse resp = invoke(hnd);

        assertEquals(GridRestResponse.STATUS_SUCCESS, resp.getSuccessStatus());

        SupplyStatusResponse payload = (SupplyStatusResponse)resp.getResponse();

        assertFalse(payload.isSupplying());
        assertTrue(payload.getSupplyingCacheGroups().isEmpty());
    }

    /** GRACEFUL sole-owner: shutdown=true (no force) -> 503 unique-data guard, no trigger (afterWrite null). */
    @Test
    public void testShutdownUniqueDataGuardUnderGraceful() throws Exception {
        shutdownPlc = ShutdownPolicy.GRACEFUL;
        cacheCfg = new CacheConfiguration<>("guardCache").setCacheMode(PARTITIONED).setBackups(1);

        startGrid("regular").cluster().state(ClusterState.ACTIVE);

        awaitPartitionMapExchange();

        GridSupplyStatusCommandHandler hnd = new GridSupplyStatusCommandHandler(grid("regular").context());

        SupplyStatusResponse info = (SupplyStatusResponse)invoke(hnd).getResponse();
        assertTrue("informational body must report uniqueDataHeld", info.isUniqueDataHeld());
        assertTrue(info.getUniqueDataCacheGroups().contains("guardCache"));

        GridRestResponse blocked = invoke(hnd, true, false);

        assertEquals("guard must return 503: " + blocked.getResponse(),
            GridRestResponse.SERVICE_UNAVAILABLE, blocked.getSuccessStatus());
        assertTrue(blocked.getResponse() instanceof SupplyStatusResponse);
        assertTrue(((SupplyStatusResponse)blocked.getResponse()).isUniqueDataHeld());
        assertNull("503 guard must NOT register a shutdown trigger", blocked.afterWrite());
        assertFalse("503 guard must NOT trigger shutdown", grid("regular").context().isStopping());
    }

    /** GRACEFUL sole-owner + force=true: bypasses the guard, idle node -> gate passes (200 + afterWrite set). */
    @Test
    public void testShutdownForceTrueBypassesUniqueDataGuard() throws Exception {
        shutdownPlc = ShutdownPolicy.GRACEFUL;
        cacheCfg = new CacheConfiguration<>("guardCache").setCacheMode(PARTITIONED).setBackups(1);

        startGrid("regular").cluster().state(ClusterState.ACTIVE);

        awaitPartitionMapExchange();

        GridSupplyStatusCommandHandler hnd = new GridSupplyStatusCommandHandler(grid("regular").context());

        GridRestResponse forced = invoke(hnd, true, true);

        assertEquals("force=true must bypass the guard -> 200: " + forced.getError(),
            GridRestResponse.STATUS_SUCCESS, forced.getSuccessStatus());
        assertNotNull("gate pass must register a shutdown trigger", forced.afterWrite());
        assertFalse("handler-level call must not stop the grid", grid("regular").context().isStopping());
    }

    /** Idempotency (SC-004): first shutdown=true registers one trigger; the second registers none. */
    @Test
    public void testShutdownTrueIsIdempotent() throws Exception {
        startGrid("regular").cluster().state(ClusterState.ACTIVE);

        awaitPartitionMapExchange();

        GridSupplyStatusCommandHandler hnd = new GridSupplyStatusCommandHandler(grid("regular").context());

        GridRestResponse first = invoke(hnd, true, false);

        assertEquals(GridRestResponse.STATUS_SUCCESS, first.getSuccessStatus());
        assertNotNull("first shutdown=true must register the trigger", first.afterWrite());

        GridRestResponse second = invoke(hnd, true, false);

        assertEquals(GridRestResponse.STATUS_SUCCESS, second.getSuccessStatus());
        assertNull("second shutdown=true must be idempotent - no re-trigger", second.afterWrite());
        assertFalse("handler-level calls must not stop the grid", grid("regular").context().isStopping());
    }

    /** @return supply-status informational response (no shutdown / force). */
    private GridRestResponse invoke(GridSupplyStatusCommandHandler hnd) throws Exception {
        GridRestRequest req = new GridRestRequest();

        req.command(GridRestCommand.SUPPLY_STATUS);

        return hnd.handleAsync(req).get(getTestTimeout());
    }

    /** @return supply-status response for {@code shutdown} + {@code force}. */
    private GridRestResponse invoke(GridSupplyStatusCommandHandler hnd, boolean shutdown,
        boolean force) throws Exception {
        GridRestSupplyStatusRequest req = new GridRestSupplyStatusRequest();

        req.command(GridRestCommand.SUPPLY_STATUS);
        req.shutdown(shutdown);
        req.force(force);

        IgniteInternalFuture<GridRestResponse> fut = hnd.handleAsync(req);

        return fut.get(getTestTimeout());
    }
}
