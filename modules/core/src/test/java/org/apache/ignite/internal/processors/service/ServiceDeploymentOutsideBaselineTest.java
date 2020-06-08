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

package org.apache.ignite.internal.processors.service;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.GridStringBuilder;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.SupportFeaturesUtils.IGNITE_DISTRIBUTED_META_STORAGE_FEATURE;

/** */
public class ServiceDeploymentOutsideBaselineTest extends GridCommonAbstractTest {
    /** */
    private static final String SERVICE_NAME = "test-service";

    /** */
    private boolean persistence;

    /** */
    private ServiceConfiguration srvcCfg;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (persistence) {
            cfg.setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(
                        new DataRegionConfiguration()
                            .setPersistenceEnabled(true)
                            .setMaxSize(10 * 1024 * 1024)
                    ).setWalMode(WALMode.LOG_ONLY)
            );
        }

        if (srvcCfg != null)
            cfg.setServiceConfiguration(srvcCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        persistence = false;
        srvcCfg = null;

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_DISTRIBUTED_META_STORAGE_FEATURE, value = "true")
    public void testDeployOutsideBaseline() throws Exception {
        checkDeploymentFromOutsideNode(true, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_DISTRIBUTED_META_STORAGE_FEATURE, value = "true")
    public void testDeployOutsideBaselineNoPersistence() throws Exception {
        checkDeploymentFromOutsideNode(false, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_DISTRIBUTED_META_STORAGE_FEATURE, value = "true")
    public void testDeployOutsideBaselineStatic() throws Exception {
        checkDeploymentFromOutsideNode(true, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_DISTRIBUTED_META_STORAGE_FEATURE, value = "true")
    public void testDeployOutsideBaselineStaticNoPersistence() throws Exception {
        checkDeploymentFromOutsideNode(false, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_DISTRIBUTED_META_STORAGE_FEATURE, value = "true")
    public void testDeployFromNodeAddedToBlt() throws Exception {
        checkDeployWithNodeAddedToBlt(true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_DISTRIBUTED_META_STORAGE_FEATURE, value = "true")
    public void testDeployToNodeAddedToBlt() throws Exception {
        checkDeployWithNodeAddedToBlt(false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_DISTRIBUTED_META_STORAGE_FEATURE, value = "true")
    public void testDeployFromNodeRemovedFromBlt() throws Exception {
        checkDeployFromNodeRemovedFromBlt(true, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_DISTRIBUTED_META_STORAGE_FEATURE, value = "true")
    public void testDeployFromNodeRemovedFromBltStatic() throws Exception {
        checkDeployFromNodeRemovedFromBlt(true, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_DISTRIBUTED_META_STORAGE_FEATURE, value = "true")
    public void testDeployToNodeRemovedFromBlt() throws Exception {
        checkDeployFromNodeRemovedFromBlt(false, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_DISTRIBUTED_META_STORAGE_FEATURE, value = "true")
    public void testDeployFromEachNodes() throws Exception {
        checkDeployFromEachNodes(false, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_DISTRIBUTED_META_STORAGE_FEATURE, value = "true")
    public void testStaticDeployFromEachNodes() throws Exception {
        checkDeployFromEachNodes(false, true);
    }

    /**
     * @param persistence If {@code true}, then persistence will be enabled.
     * @param staticDeploy If {@code true}, then static deployment will be used instead of a dynamic one.
     * @throws Exception If failed.
     */
    private void checkDeployFromEachNodes(boolean persistence, boolean staticDeploy) throws Exception {
        this.persistence = persistence;

        CountDownLatch exeLatch = new CountDownLatch(1);

        DummyService.exeLatch(SERVICE_NAME, exeLatch);

        Ignite ignite0 = deployServiceFromNewNode(staticDeploy, 0);

        if (!staticDeploy) {
            IgniteCluster cluster = ignite0.cluster();

            cluster.setBaselineTopology(cluster.topologyVersion());
        }

        assertTrue(exeLatch.await(10, TimeUnit.SECONDS));

        IgniteInternalFuture startFut = GridTestUtils.runAsync(() -> {
            try {
                deployServiceFromNewNode(staticDeploy);
            }
            catch (Exception e) {
                fail(e.getMessage());
            }
        });

        try {
            startFut.get(10, TimeUnit.SECONDS);
        }
        catch (IgniteFutureTimeoutCheckedException e) {
            GridStringBuilder sb = new SB()
                .a("Node can not start out of baseline till ")
                .a(10_000L)
                .a("ms")
                .a(U.nl());

            for (Thread t: Thread.getAllStackTraces().keySet())
                if (t.getName().startsWith("async-runnable-runner"))
                    U.printStackTrace(t.getId(), sb);

            fail(sb.toString());
        }
    }

    /**
     * @param persistence If {@code true}, then persistence will be enabled.
     * @param staticDeploy If {@code true}, then static deployment will be used instead of a dynamic one.
     * @throws Exception If failed.
     */
    private void checkDeploymentFromOutsideNode(boolean persistence, boolean staticDeploy) throws Exception {
        this.persistence = persistence;

        IgniteEx insideNode = startGrid(0);

        insideNode.cluster().baselineAutoAdjustEnabled(false);

        if (persistence)
            insideNode.cluster().active(true);
        else {
            IgniteCluster cluster = insideNode.cluster();

            cluster.setBaselineTopology(cluster.topologyVersion());
        }

        CountDownLatch exeLatch = new CountDownLatch(1);

        DummyService.exeLatch(SERVICE_NAME, exeLatch);

        deployServiceFromNewNode(staticDeploy);

        assertTrue(exeLatch.await(10, TimeUnit.SECONDS));
    }

    /**
     * @param from If {@code true}, then added node will be an initiator of deployment.
     * Otherwise deployment <b>to</b> this node will be tested.
     * @throws Exception If failed.
     */
    private void checkDeployWithNodeAddedToBlt(boolean from) throws Exception {
        persistence = true;

        IgniteEx insideNode = startGrid(0);

        insideNode.cluster().baselineAutoAdjustEnabled(false);

        IgniteCluster cluster = insideNode.cluster();

        cluster.active(true);

        Ignite outsideNode = startGrid(1);

        cluster.setBaselineTopology(cluster.topologyVersion());

        CountDownLatch exeLatch = new CountDownLatch(from ? 1 : 2);

        DummyService.exeLatch(SERVICE_NAME, exeLatch);

        if (from) {
            IgniteFuture<Void> depFut = outsideNode.services().deployClusterSingletonAsync(SERVICE_NAME, new DummyService());

            depFut.get(10, TimeUnit.SECONDS);
        }
        else {
            IgniteFuture<Void> depFut = outsideNode.services().deployNodeSingletonAsync(SERVICE_NAME, new DummyService());

            depFut.get(10, TimeUnit.SECONDS);
        }

        assertTrue(exeLatch.await(10, TimeUnit.SECONDS));
    }

    /**
     * @param from If {@code true}, then added node will be an initiator of deployment.
     * Otherwise deployment <b>to</b> this node will be tested.
     * @param staticDeploy If {@code true}, then static deployment will be used instead of a dynamic one.
     * @throws Exception If failed.
     */
    private void checkDeployFromNodeRemovedFromBlt(boolean from, boolean staticDeploy) throws Exception {
        persistence = true;

        IgniteEx insideNode = startGrid(0);

        insideNode.cluster().baselineAutoAdjustEnabled(false);

        startGrid(1);

        IgniteCluster cluster = insideNode.cluster();

        cluster.active(true);

        stopGrid(1);

        cluster.setBaselineTopology(cluster.topologyVersion());

        CountDownLatch exeLatch = new CountDownLatch(from ? 1 : 2);

        DummyService.exeLatch(SERVICE_NAME, exeLatch);

        if (from)
            deployServiceFromNewNode(staticDeploy);
        else {
            startGrid(1);

            IgniteFuture<Void> depFut = insideNode.services().deployNodeSingletonAsync(SERVICE_NAME, new DummyService());

            depFut.get(10, TimeUnit.SECONDS);
        }

        assertTrue(exeLatch.await(10, TimeUnit.SECONDS));
    }

    /**
     * @param staticDeploy If {@code true}, then static deployment will be used instead of a dynamic one.
     * @throws Exception If node failed to start.
     */
    private Ignite deployServiceFromNewNode(boolean staticDeploy) throws Exception {
        return deployServiceFromNewNode(staticDeploy, 1);
    }

    /**
     * @param staticDeploy If {@code true}, then static deployment will be used instead of a dynamic one.
     * @param nodeNum Nouber of test node.
     * @throws Exception If node failed to start.
     */
    private Ignite deployServiceFromNewNode(boolean staticDeploy, int nodeNum) throws Exception {
        IgniteEx ignite;

        if (staticDeploy) {
            srvcCfg = getClusterSingletonServiceConfiguration();

            ignite = startGrid(nodeNum);
        }
        else {
            ignite = startGrid(nodeNum);

            IgniteFuture<Void> depFut = ignite.services().deployClusterSingletonAsync(SERVICE_NAME, new DummyService());

            depFut.get(10, TimeUnit.SECONDS);
        }

        ignite.cluster().baselineAutoAdjustEnabled(false);

        return ignite;
    }

    /**
     * @return Test service configuration.
     */
    private ServiceConfiguration getClusterSingletonServiceConfiguration() {
        ServiceConfiguration srvcCfg = new ServiceConfiguration();
        srvcCfg.setName(SERVICE_NAME);
        srvcCfg.setService(new DummyService());
        srvcCfg.setTotalCount(1);

        return srvcCfg;
    }
}
