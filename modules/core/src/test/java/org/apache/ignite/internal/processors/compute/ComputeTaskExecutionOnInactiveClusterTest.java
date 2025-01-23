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

package org.apache.ignite.internal.processors.compute;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.processors.security.impl.TestSecurityPluginProvider;
import org.apache.ignite.plugin.security.SecurityPermissionSet;
import org.apache.ignite.plugin.security.SecurityPermissionSetBuilder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.INACTIVE;
import static org.apache.ignite.plugin.security.SecurityPermission.JOIN_AS_SERVER;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/**
 * Test class that covers executing task on inactive cluster.
 */
public class ComputeTaskExecutionOnInactiveClusterTest extends GridCommonAbstractTest {
    /** This flags indicates that security feature is enabled. */
    private boolean securityEnabled;

    /** Task execution timeout in seconds. */
    private static final long TASK_EXECUTION_TIMEOUT = 3;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setMaxSize(100 * 1024 * 1024).setPersistenceEnabled(true));

        cfg.setDataStorageConfiguration(memCfg);

        if (securityEnabled) {
            SecurityPermissionSet secPermSet = SecurityPermissionSetBuilder.create()
                .defaultAllowAll(true)
                .appendSystemPermissions(JOIN_AS_SERVER)
                .build();
            cfg.setPluginProviders(new TestSecurityPluginProvider("login", "", secPermSet, true));
        }
        return cfg;
    }

    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    /**
     * Tests that executing a task is possible on inactive cluster when security is disabled.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testStartTaskOnInactiveCluster() throws Exception {
        IgniteEx crd = startGrid(0);

        assertEquals(INACTIVE, crd.cluster().state());

        crd.compute().executeAsync(new TestTask(), null).get(TASK_EXECUTION_TIMEOUT, SECONDS);

        stopGrid(0);
    }

    /**
     * Tests that executing a task on inactive cluster with security enabled will wait for an activation.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testStartTaskOnInactiveSecureCluster() throws Exception {
        securityEnabled = true;

        IgniteEx crd = startGrid(0);

        assertEquals(INACTIVE, crd.cluster().state());

        IgniteInternalFuture<?> taskFut = runAsync(() -> {
            crd.compute().execute(new TestTask(), null);
        });

        assertThrows(
            log,
            () -> taskFut.get(TASK_EXECUTION_TIMEOUT, SECONDS),
            IgniteFutureTimeoutCheckedException.class,
            null
        );

        assertFalse("Task future unexpectedly completed.", taskFut.isDone());

        crd.cluster().state(ACTIVE);

        assertEquals(ACTIVE, crd.cluster().state());

        taskFut.get(TASK_EXECUTION_TIMEOUT, SECONDS);
    }

    /**
     * Tests that executing a task on inactive cluster with security enabled and stopping the cluster
     * does not lead to a deadlock.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testStopNodeWithConcurrentTaskExecutionOnInactiveSecureCluster() throws Exception {
        securityEnabled = true;

        IgniteEx crd = startGrid(0);

        assertEquals(INACTIVE, crd.cluster().state());

        IgniteInternalFuture<?> taskFut = runAsync(() -> {
            crd.compute().execute(new TestTask(), null);
        });

        assertThrows(
            log,
            () -> taskFut.get(TASK_EXECUTION_TIMEOUT, SECONDS),
            IgniteFutureTimeoutCheckedException.class,
            null
        );

        assertFalse("Task future unexpectedly completed.", taskFut.isDone());

        stopGrid(0);

        assertThrowsWithCause(() -> taskFut.get(), NodeStoppingException.class);
    }

    /**
     * Tests that execution of a task after the cluster re-activation successfully completed.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testStartTaskAfterReActivationOnSecureCluster() throws Exception {
        securityEnabled = true;

        IgniteEx crd = startGrid(0);

        assertEquals(INACTIVE, crd.cluster().state());

        crd.cluster().state(ACTIVE);

        assertEquals(ACTIVE, crd.cluster().state());

        crd.cluster().state(INACTIVE);

        assertEquals(INACTIVE, crd.cluster().state());

        IgniteInternalFuture<?> taskFut = runAsync(() -> {
            crd.compute().execute(new TestTask(), null);
        });

        assertThrows(
            log,
            () -> taskFut.get(TASK_EXECUTION_TIMEOUT, SECONDS),
            IgniteFutureTimeoutCheckedException.class,
            null
        );

        crd.cluster().state(ACTIVE);

        assertEquals(ACTIVE, crd.cluster().state());

        crd.compute().execute(new TestTask(), null);
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
