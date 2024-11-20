/*
 * Copyright 2024 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.cluster;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionExchangeId;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.internal.processors.security.impl.TestSecurityPluginProvider;
import org.apache.ignite.lifecycle.LifecycleBean;
import org.apache.ignite.lifecycle.LifecycleEventType;
import org.apache.ignite.loadtests.colocation.GridTestLifecycleBean;
import org.apache.ignite.plugin.security.SecurityPermissionSet;
import org.apache.ignite.plugin.security.SecurityPermissionSetBuilder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.INACTIVE;
import static org.apache.ignite.plugin.security.SecurityPermission.JOIN_AS_SERVER;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

public class ClusterStateChangeOnNodeJoinTest extends GridCommonAbstractTest {
    /** */
    private LifecycleBean lifecycleBean;

    /** This flags indicates that security feature is enabled. */
    private boolean securityEnabled;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setConsistentId(gridName);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        if (lifecycleBean != null)
            cfg.setLifecycleBeans(lifecycleBean);

        if (securityEnabled) {
            SecurityPermissionSet secPermSet = SecurityPermissionSetBuilder.create()
                .defaultAllowAll(true)
                .appendSystemPermissions(JOIN_AS_SERVER)
                .build();
            cfg.setPluginProviders(new TestSecurityPluginProvider("login", "", secPermSet, true));
        }

        cfg.setFailureHandler(new StopNodeFailureHandler());
        cfg.setConsistentId(gridName);

        cfg.setClusterStateOnStart(INACTIVE);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    @Test
    public void testJoiningClientNodeToClusterInTransitionState() throws Exception {
        joiningNodeToClusterInTransitionState();
    }

    @Test
    public void testJoiningClientNodeToClusterInTransitionStateWithSecurityEnabled() throws Exception {
        securityEnabled = true;

        joiningNodeToClusterInTransitionState();
    }

    public void joiningNodeToClusterInTransitionState() throws Exception {
        IgniteEx g0 = startGrid(0);
        IgniteEx g1 = startGrid(1);

        CountDownLatch latch = new CountDownLatch(1);

        TestRecordingCommunicationSpi spi1 = TestRecordingCommunicationSpi.spi(g1);

        spi1.blockMessages((node, msg) -> {
            if (msg instanceof GridDhtPartitionsSingleMessage) {
                latch.countDown();

                return true;
            }

            return false;
        });

        IgniteInternalFuture<?> actFut = runAsync(() -> {
            g0.cluster().state(ACTIVE);
        });

        latch.await();

        AtomicReference<IgniteEx> clientRef = new AtomicReference<>();
        lifecycleBean = new GridTestLifecycleBean() {
            @Override public void onLifecycleEvent(LifecycleEventType evt) throws IgniteException {
                if (evt == LifecycleEventType.BEFORE_NODE_START)
                    clientRef.set(g);
            }
        };

        IgniteInternalFuture clientFut = runAsync(() -> {
            try {
                startClientGrid(2);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        });

        boolean res = waitForCondition(() -> {
            return clientRef.get() != null
                && clientRef.get().context().state() != null
                && clientRef.get().context().state().clusterState().transitionRequestId() != null;
        }, 30_000);

        assertTrue(res);

        spi1.stopBlock();
        actFut.get();
        clientFut.get();

        // Try to create a new data structure.
        clientRef.get().atomicLong("testAtomicLong", 1L, true);

        // Check that the node is able to execute compute tasks as well.
        clientRef.get().compute().execute(new TestTask(), null);
    }

    /**
     * Task for testing purposes.
     */
    static final class TestTask extends ComputeTaskAdapter<String, Object> {
        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,@Nullable String arg) {
            return Collections.singletonMap(new ComputeJobAdapter() {
                /** {@inheritDoc} */
                @Override public Object execute() throws IgniteException {
                    return null;
                }
            }, subgrid.get(0));
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<ComputeJobResult> results) {
            return null;
        }
    }
}
