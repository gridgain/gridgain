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
import org.apache.ignite.internal.processors.rest.handlers.alive.GridAliveCommandHandler;
import org.apache.ignite.internal.processors.rest.request.GridRestRequest;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Handler-level tests for {@link GridAliveCommandHandler}. Per HLD v14 §1,
 * {@code cmd=alive} returns 200 when the JVM kernel has started, 503
 * otherwise — and is independent of drain, PME, and supply state.
 */
public class GridAliveCommandTest extends GridCommonAbstractTest {
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
     * Kernel started → HTTP 200 "alive".
     */
    @Test
    public void testAliveOnStartedNode() throws Exception {
        startGrid("regular").cluster().state(ClusterState.ACTIVE);

        GridAliveCommandHandler hnd = new GridAliveCommandHandler(grid("regular").context());

        GridRestResponse resp = invoke(hnd);

        assertEquals(GridRestResponse.STATUS_SUCCESS, resp.getSuccessStatus());
        assertEquals("alive", resp.getResponse());
    }

    /**
     * Kernel started but cluster INACTIVE — alive is independent of cluster state.
     * (cmd=alive answers only "is the node running?").
     */
    @Test
    public void testAliveOnInactiveCluster() throws Exception {
        startGrid("regular");
        // Do NOT activate.

        GridAliveCommandHandler hnd = new GridAliveCommandHandler(grid("regular").context());

        GridRestResponse resp = invoke(hnd);

        assertEquals(GridRestResponse.STATUS_SUCCESS, resp.getSuccessStatus());
        assertEquals("alive", resp.getResponse());
    }

    /**
     * @param hnd Handler.
     * @return Resolved response.
     */
    private static GridRestResponse invoke(GridAliveCommandHandler hnd) throws Exception {
        GridRestRequest req = new GridRestRequest();

        req.command(GridRestCommand.ALIVE);

        IgniteInternalFuture<GridRestResponse> fut = hnd.handleAsync(req);

        return fut.get();
    }
}
