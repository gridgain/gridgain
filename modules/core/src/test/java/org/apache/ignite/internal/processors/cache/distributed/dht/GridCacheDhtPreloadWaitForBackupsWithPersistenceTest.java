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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import org.apache.ignite.Ignite;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_WAIT_FOR_BACKUPS_ON_SHUTDOWN;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;

/**
 * Tests for "wait for backups on shutdown" flag with persistence.
 */
@WithSystemProperty(key = IGNITE_WAIT_FOR_BACKUPS_ON_SHUTDOWN, value = "true")
public class GridCacheDhtPreloadWaitForBackupsWithPersistenceTest extends GridCacheDhtPreloadWaitForBackupsTest {
    /** {@inheritDoc} */
    @Override protected boolean persistenceEnabled() {
        return true;
    }

    /** */
    @Override protected int cacheSize() {
        return 2000;
    }

    /** */
    @Override protected int iterations() {
        return 2;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();

        super.beforeTest();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        cleanPersistenceDir();
    }

    @Test
    public void testSimultaneousClusterShutdownWithNonBaselineNodes() throws Exception {
        int gridCnt = 3;

        Ignite g0 = startGrids(gridCnt);

        g0.cluster().state(ACTIVE);

        // Start server nodes outside the current baseline.
        startGrid(3);
        startGrid(4);

        gridCnt += 2;

        // Start client node.
        IgniteEx client = startClientGrid(gridCnt);

        stopGridsInParallel(gridCnt, gridCnt);

        assertEquals("Only 1 node should be alive.", 1, G.allGrids().size());

        assertTrue("Only client node should be alive.", G.allGrids().get(0).configuration().isClientMode());

        assertFalse("Client node is stopping.", client.context().isStopping());
    }

    @Test
    public void testSimultaneousClusterShutdownWithOfflineBaselineNodes() throws Exception {
        backups = 2;

        int gridCnt = 5;

        Ignite g0 = startGrids(gridCnt);

        g0.cluster().state(ACTIVE);

        // Stop 2 of 5 baseline nodes.
        stopGrid(0);
        stopGrid(1);

        // Start client node.
        IgniteEx client = startClientGrid(gridCnt);

        stopGridsInParallel(gridCnt, 3);

        assertEquals("Only 1 node should be alive.", 1, G.allGrids().size());

        assertTrue("Only client node should be alive.", G.allGrids().get(0).configuration().isClientMode());

        assertFalse("Client node is stopping.", client.context().isStopping());
    }
}
