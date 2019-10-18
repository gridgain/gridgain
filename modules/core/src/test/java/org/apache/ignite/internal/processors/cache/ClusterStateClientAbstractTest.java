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

import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;

/**
 *
 */
public abstract class ClusterStateClientAbstractTest extends ClusterStateAbstractTest {
    /** */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected int nodesCount() {
        return GRID_CNT + 1;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        return super.getConfiguration(gridName).setClientMode(client);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        client = true;

        startGrid(GRID_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void changeState(ClusterState state) {
        IgniteEx cl = grid(GRID_CNT);

        assertTrue(cl.configuration().isClientMode());

        cl.cluster().state(state);
    }
}
