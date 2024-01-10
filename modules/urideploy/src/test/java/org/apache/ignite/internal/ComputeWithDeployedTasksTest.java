/*
 * Copyright 2023 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal;

import java.util.Arrays;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteDeploymentException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.deployment.uri.UriDeploymentSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/**
 * Tests for how compute tasks get executed when their classes are deployed using a deployment SPI.
 */
public class ComputeWithDeployedTasksTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(igniteInstanceName);

        UriDeploymentSpi deploymentSpi = new UriDeploymentSpi();

        if (!igniteInstanceName.startsWith("client")) {
            deploymentSpi.setUriList(
                Arrays.asList(U.resolveIgniteUrl("modules/extdata/uri/target/resources/").toURI().toString()));
        }

        c.setDeploymentSpi(deploymentSpi);

        return c;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * Tests that a task deployed on a server node can be executed by name.
     */
    @Test
    public void taskDeployedOnServerNodeCanBeExecutedByName() throws Exception {
        IgniteEx server = startGrid(1);

        IgniteCompute compute = server.compute(server.cluster().forServers().forRandom());

        assertEquals("ok", compute.execute("SimpleNamedTask", null));
    }

    /**
     * Tests that a task that is deployed on server nodes but NOT deployed on a client node cannot be executed
     * by name via such client.
     */
    @Test
    public void taskNotDeployedOnClientCannotBeExecutedByName() throws Exception {
        startGrid(1);

        IgniteEx client = startClientGrid("client");

        IgniteCompute compute = client.compute(client.cluster().forServers().forRandom());

        assertThrows(
            log,
            () -> compute.execute("SimpleNamedTask", null),
            IgniteDeploymentException.class,
            "Unknown task name or failed to auto-deploy task (was task (re|un)deployed?): SimpleNamedTask"
        );
    }
}
