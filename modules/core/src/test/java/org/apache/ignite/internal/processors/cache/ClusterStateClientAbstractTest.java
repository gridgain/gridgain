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
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.INACTIVE;
import static org.apache.ignite.cluster.ClusterState.READ_ONLY;

/**
 *
 */
public abstract class ClusterStateClientAbstractTest extends ClusterStateAbstractTest {
    /** */
    private static final int TOTAL_NODES = GRID_CNT + 1;

    /** */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected void checkAllNodesInactive() {
        checkInactive(TOTAL_NODES);
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

    /** */
    @Test
    public void testActivationFromClient() {
        changeStateFromClient(INACTIVE, ACTIVE);
    }

    /** */
    @Test
    public void testActivationWithReadOnlyFromClient() {
        changeStateFromClient(INACTIVE, READ_ONLY);
    }

    /** */
    @Test
    public void testEnablingReadOnlyFromClient() {
        changeStateFromClient(ACTIVE, READ_ONLY);
    }

    /** */
    @Test
    public void testDisablingReadOnlyFromClient() {
        changeStateFromClient(READ_ONLY, ACTIVE);
    }

    /** */
    private void changeStateFromClient(ClusterState state, ClusterState targetState) {
        assertNotSame(state, targetState);

        IgniteEx crd = grid(0);
        IgniteEx cl = grid(GRID_CNT);

        checkInactive(TOTAL_NODES);

        crd.cluster().state(state);

        checkClusterState(TOTAL_NODES, state);

        cl.cluster().state(targetState);

        checkClusterState(TOTAL_NODES, targetState);

        if (targetState != READ_ONLY) {
            for (int k = 0; k < ENTRY_CNT; k++)
                cl.cache(CACHE_NAME).put(k, k);
        }

        for (int g = 0; g < GRID_CNT + 1; g++) {
            for (int k = 0; k < ENTRY_CNT; k++)
                assertEquals(targetState != READ_ONLY ? k : null,  grid(g).cache(CACHE_NAME).get(k));
        }
    }
}
