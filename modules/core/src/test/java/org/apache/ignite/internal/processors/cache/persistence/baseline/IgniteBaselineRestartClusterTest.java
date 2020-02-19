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
package org.apache.ignite.internal.processors.cache.persistence.baseline;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests the restart of the entire cluster with the cleaning the working directory on one of the nodes.
 */
public class IgniteBaselineRestartClusterTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                        .setMaxSize(128 * 1024 * 1024)
                )
        );

        cfg.setConsistentId(igniteInstanceName);

        return cfg;
    }

    /**
     * Called before execution of every test method in class.
     *
     * @throws Exception If failed.
     */
    @Before
    public void setupTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * Stops all nodes and cleans working directories.
     *
     * @throws Exception If failed.
     */
    @After
    public void testCleanup() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * Tests the restart of the entire cluster with the cleaning the working directory on one of the nodes.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = "IGNITE_STOP_EMPTY_NODE_ON_JOIN", value = "true")
    public void testRestartClusterWithCleaningWorkingDir() throws Exception {
        // There is no need to check something special for MVCC mode.
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.EVICTION);

        int nodeCnt = 3;

        int cleanNodeCnt = nodeCnt - 1;

        // Start cluster and set baseline.
        startGrids(nodeCnt);

        grid(0).cluster().active(true);

        // Stop the whole cluster and clean working directory of the first node.
        List<String> nodeNames = new ArrayList<>(cleanNodeCnt);
        for (int i = 0; i < cleanNodeCnt; ++i)
            nodeNames.add(grid(i).name());

        stopAllGrids();

        for (String name : nodeNames)
            cleanPersistenceDir(name);

        // Start the clean node first.
        List<IgniteEx> cleanNodes = new ArrayList<>(cleanNodeCnt);

        for (int i = 0; i < cleanNodeCnt; ++i)
            cleanNodes.add(startGrid(i));

        IgniteEx dirtyNode = startGrid(cleanNodeCnt);

        // Check that all clean nodes are stopped, and so dirtyNode is coordinator.
        for (int i = 0; i < cleanNodeCnt; ++i)
            assertTrue("The clean nodes should be stopped ", cleanNodes.get(i).context().isStopping());

        // Check that clean nodes can be joined to the cluster at this point.
        for (int i = 0; i < cleanNodeCnt; ++i)
            startGrid(i);
    }
}
