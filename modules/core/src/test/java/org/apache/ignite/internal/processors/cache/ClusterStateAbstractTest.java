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
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cluster.ClusterState.INACTIVE;

/**
 *
 */
public abstract class ClusterStateAbstractTest extends GridCommonAbstractTest {
    /** Entry count. */
    protected static final int ENTRY_CNT = 5000;

    /** */
    protected static final int GRID_CNT = 4;

    /** */
    protected static final String CACHE_NAME = "cache1";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setActiveOnStart(false);

        cfg.setCacheConfiguration(cacheConfiguration(CACHE_NAME));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        stopAllGrids();

        startGridsMultiThreaded(GRID_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        grid(0).cluster().state(INACTIVE);

        checkAllNodesInactive();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        grid(0).cluster().state(INACTIVE);

        checkAllNodesInactive();

        super.afterTest();
    }

    /**
     * @param cacheName Cache name.
     * @return Cache configuration.
     */
    protected abstract CacheConfiguration cacheConfiguration(String cacheName);

    /** */
    protected void checkAllNodesInactive() {
        checkInactive(GRID_CNT);
    }

    /** */
    void checkClusterState(int nodesCnt, ClusterState state) {
        for (int g = 0; g < nodesCnt ; g++)
            assertEquals(grid(g).name(), state, grid(g).cluster().state());
    }

    /** */
    void checkInactive(int cnt) {
        checkClusterState(cnt, INACTIVE);
    }
}
