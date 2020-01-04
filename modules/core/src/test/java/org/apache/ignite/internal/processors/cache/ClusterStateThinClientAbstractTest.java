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

package org.apache.ignite.internal.processors.cache;

import java.util.Collections;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.client.GridClientFactory;
import org.apache.ignite.internal.client.GridClientProtocol;

import static org.apache.ignite.configuration.ConnectorConfiguration.DFLT_TCP_PORT;

/**
 * Tests that cluster state change works correctly with connected thin client in different situations.
 */
public abstract class ClusterStateThinClientAbstractTest extends ClusterStateAbstractTest {
    /** */
    private static final String HOST = "127.0.0.1";

    /** */
    private static GridClient gridClient;

    /** */
    private int port = DFLT_TCP_PORT;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        return super.getConfiguration(gridName)
            .setConnectorConfiguration(new ConnectorConfiguration().setPort(port++).setHost(HOST));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        GridClientConfiguration cfg = new GridClientConfiguration();

        cfg.setProtocol(GridClientProtocol.TCP);
        cfg.setServers(Collections.singletonList(HOST + ":" + DFLT_TCP_PORT));

        gridClient = GridClientFactory.start(cfg);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        if (gridClient != null)
            gridClient.close();

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected void changeState(ClusterState state) {
        try {
            gridClient.state().state(state);
        }
        catch (GridClientException e) {
            throw new RuntimeException("Can't change state to " + state, e);
        }
    }
}
