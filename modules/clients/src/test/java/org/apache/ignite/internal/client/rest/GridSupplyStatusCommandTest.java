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
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.rest.GridRestCommand;
import org.apache.ignite.internal.processors.rest.GridRestResponse;
import org.apache.ignite.internal.processors.rest.handlers.drain.GridSupplyStatusCommandHandler;
import org.apache.ignite.internal.processors.rest.handlers.drain.SupplyStatusResponse;
import org.apache.ignite.internal.processors.rest.request.GridRestRequest;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Handler-level tests for {@link GridSupplyStatusCommandHandler}. Exercises the
 * two cases that are testable without forcing a multi-node rebalance — the
 * supplying-true case is covered by the HTTP-level rebalance tests.
 */
public class GridSupplyStatusCommandTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConnectorConfiguration(new ConnectorConfiguration());

        if (igniteInstanceName.equals("persistent")) {
            cfg.setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(true)));
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
     * Steady state on a single-node cluster: nothing to supply,
     * {@code supplying:false}, empty cache-group list.
     */
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
    }

    /**
     * INACTIVE cluster: handler must not throw when iterating cache groups
     * before activation — returns {@code supplying:false} cleanly.
     */
    @Test
    public void testInactiveClusterNoException() throws Exception {
        startGrid("persistent");

        // Cluster comes up INACTIVE with persistence; do not activate.
        assertFalse(ClusterState.active(grid("persistent").context().state().clusterState().state()));

        GridSupplyStatusCommandHandler hnd = new GridSupplyStatusCommandHandler(grid("persistent").context());

        GridRestResponse resp = invoke(hnd);

        assertEquals(GridRestResponse.STATUS_SUCCESS, resp.getSuccessStatus());

        SupplyStatusResponse payload = (SupplyStatusResponse)resp.getResponse();

        assertFalse(payload.isSupplying());
        assertTrue(payload.getSupplyingCacheGroups().isEmpty());
    }

    /**
     * @param hnd Handler.
     * @return Resolved response.
     */
    private static GridRestResponse invoke(GridSupplyStatusCommandHandler hnd) throws Exception {
        GridRestRequest req = new GridRestRequest();

        req.command(GridRestCommand.SUPPLY_STATUS);

        IgniteInternalFuture<GridRestResponse> fut = hnd.handleAsync(req);

        return fut.get();
    }
}
