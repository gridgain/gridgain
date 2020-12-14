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

package org.apache.ignite.internal.managers.deployment;

import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_DEPLOYMENT_PRESERVE_LOCAL;

/** Multiple local deployments. */
public class GridDifferentLocalDeploymentSelfTest extends GridCommonAbstractTest {
    /** Task name. */
    private static final String TASK_NAME1 = "org.apache.ignite.tests.p2p.P2PTestTaskExternalPath1";

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * Test GridDeploymentMode.SHARED mode.
     *
     * @throws Exception if error occur.
     */
    @Test
    @WithSystemProperty(key = IGNITE_DEPLOYMENT_PRESERVE_LOCAL, value = "true")
    public void testCheckTaskClassloaderCacheSharedMode() throws Exception {
        testCheckTaskClassloaderCache(DeploymentMode.SHARED);
    }

    /**
     * Test GridDeploymentMode.PRIVATE mode.
     *
     * @throws Exception if error occur.
     */
    @Test
    @WithSystemProperty(key = IGNITE_DEPLOYMENT_PRESERVE_LOCAL, value = "true")
    public void testCheckTaskClassloaderCachePrivateMode() throws Exception {
        testCheckTaskClassloaderCache(DeploymentMode.PRIVATE);
    }

    /**
     * Test GridDeploymentMode.ISOLATED mode.
     *
     * @throws Exception if error occur.
     */
    @Test
    @WithSystemProperty(key = IGNITE_DEPLOYMENT_PRESERVE_LOCAL, value = "true")
    public void testCheckTaskClassloaderCacheIsolatedMode() throws Exception {
        testCheckTaskClassloaderCache(DeploymentMode.ISOLATED);
    }

    /**
     * Test GridDeploymentMode.CONTINUOUS mode.
     *
     * @throws Exception if error occur.
     */
    @Test
    @WithSystemProperty(key = IGNITE_DEPLOYMENT_PRESERVE_LOCAL, value = "true")
    public void testCheckTaskClassloaderCacheContinuousMode() throws Exception {
        testCheckTaskClassloaderCache(DeploymentMode.CONTINUOUS);
    }

    /** */
    public void testCheckTaskClassloaderCache(DeploymentMode depMode) throws Exception {
        IgniteEx server = startGrid(0);

        IgniteEx client = startClientGrid(1);

        ClassLoader clsLdr1 = getExternalClassLoader();

        ClassLoader clsLdr2 = getExternalClassLoader();

        Class<ComputeTask> taskCls1 = (Class<ComputeTask>) clsLdr1.loadClass(TASK_NAME1);
        Class<ComputeTask> taskCls2 = (Class<ComputeTask>) clsLdr2.loadClass(TASK_NAME1);

        client.compute().execute(taskCls1.newInstance(), server.localNode().id());
        client.compute().execute(taskCls2.newInstance(), server.localNode().id());
        client.compute().execute(taskCls1.newInstance(), server.localNode().id());

        GridDeploymentManager deploymentMgr = client.context().deploy();

        GridDeploymentStore store = GridTestUtils.getFieldValue(deploymentMgr, "locStore");

        ConcurrentMap<String, Deque<GridDeployment>> cache = GridTestUtils.getFieldValue(store, "cache");

        assertEquals(2, cache.get(TASK_NAME1).size());

        deploymentMgr = server.context().deploy();

        GridDeploymentStore verStore = GridTestUtils.getFieldValue(deploymentMgr, "verStore");

        // deployments per userVer map.
        Map<String, List<Object>> varCache = GridTestUtils.getFieldValue(verStore, "cache");

        if (depMode == DeploymentMode.CONTINUOUS || depMode == DeploymentMode.SHARED) {
            for (List<Object> deps : varCache.values())
                assertEquals(2, deps.size());
        }
    }
}
