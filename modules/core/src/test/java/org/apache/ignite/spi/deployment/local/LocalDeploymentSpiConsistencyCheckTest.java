/*
 * Copyright 2021 GridGain Systems, Inc. and Contributors.
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
package org.apache.ignite.spi.deployment.local;

import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiConsistencyChecked;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.IgniteSpiMultipleInstancesSupport;
import org.apache.ignite.spi.deployment.DeploymentListener;
import org.apache.ignite.spi.deployment.DeploymentResource;
import org.apache.ignite.spi.deployment.DeploymentSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.function.Consumer;

/** Checks that LocalDeploymentSpi can be mixed with other DeploymentSpi implementations as expected. */
@RunWith(Parameterized.class)
public class LocalDeploymentSpiConsistencyCheckTest extends GridCommonAbstractTest {

    /** */
    @Parameterized.Parameter(0)
    public DeploymentSpi spi1;

    /** */
    @Parameterized.Parameter(1)
    public DeploymentSpi spi2;

    /** */
    @Parameterized.Parameter(2)
    public boolean isSecondClient;

    /** */
    @Parameterized.Parameter(3)
    public boolean firstWarns;

    /** */
    @Parameterized.Parameter(4)
    public boolean secondFails;

    /**
     * Test parameters.
     * spi1, spi2, isSecondClient - check all combinations.
     * firstWarns, secondFails - values depend on the first three parameters; analyse and set manually.
     */
    @Parameterized.Parameters(name = "spi1={0}, spi2={1}, isSecondClient={2}, firstWarns={3}, secondFails={4}")
    public static Collection<Object[]> testData() {
        return Arrays.asList(new Object[][]{
            { new LocalDeploymentSpi(), new InconsistentDeploymentSpi(), false, false, false },
            { new LocalDeploymentSpi(), new ConsistentDeploymentSpi(), false, false, true },
            { new LocalDeploymentSpi(), new ConsistentNoClientsDeploymentSpi(), false, false, true },
            { new InconsistentDeploymentSpi(), new LocalDeploymentSpi(), false, false, false },
            { new ConsistentDeploymentSpi(), new LocalDeploymentSpi(), false, true, false },
            { new ConsistentNoClientsDeploymentSpi(), new LocalDeploymentSpi(), false, true, false },

            { new LocalDeploymentSpi(), new InconsistentDeploymentSpi(), true, false, false },
            { new LocalDeploymentSpi(), new ConsistentDeploymentSpi(), true, false, true },
            { new LocalDeploymentSpi(), new ConsistentNoClientsDeploymentSpi(), true, false, false },
            { new InconsistentDeploymentSpi(), new LocalDeploymentSpi(), true, false, false },
            { new ConsistentDeploymentSpi(), new LocalDeploymentSpi(), true, true, false },
            { new ConsistentNoClientsDeploymentSpi(), new LocalDeploymentSpi(), true, false, false },
        });
    }

    /** */
    @After
    public void tearDown() {
        stopAllGrids();
    }

    /**
     * 1. Start one server with Deployment SPI implementation 1.
     * 2. Start second node, server or client, with Deployment SPI implementation 2.
     * 3. Check that node 1 prints a warning if and only if its SPI detects an inconsistent configuration.
     * 4. Check that the joining node fails to join if and only if its SPI detects an inconsistent configuration.
     * 5. Check that the joining node never prints the inconsistent configuration warning.
     *
     * Note: SPI detects an inconsistent configuration when it requires consistency, and the joining node has a
     * different implementation (taking {@code checkClient} into account).
     */
    @Test
    @SuppressWarnings("ThrowableNotThrown")
    public void test() throws Exception {
        LogListener listener1 =
            LogListener.matches(">>> Remote SPI with the same name is not configured").build();

        LogListener listener2 =
            LogListener.matches(">>> Remote SPI with the same name is not configured").build();

        startGrid(0, (Consumer<IgniteConfiguration>) cfg ->
            cfg.setDeploymentSpi(spi1).setGridLogger(new ListeningTestLogger(log, listener1)));

        Consumer<IgniteConfiguration> node2Cfg = cfg ->
            cfg.setDeploymentSpi(spi2).setGridLogger(new ListeningTestLogger(log, listener2))
                .setClientMode(isSecondClient);

        if (!secondFails) {
            startGrid(1, node2Cfg);

            assertEquals(2, grid(1).cluster().nodes().size());
        } else {
            GridTestUtils.assertThrowsAnyCause(log, () -> startGrid(1, node2Cfg), IgniteSpiException.class,
                "Remote SPI with the same name is not configured");
        }

        assertEquals(firstWarns, listener1.check());
        assertFalse(listener2.check());
    }

    /** Dummy SPI that doesn't check configuration consistency. */
    @IgniteSpiMultipleInstancesSupport(true)
    private static class InconsistentDeploymentSpi extends TestNoopDeploymentSpi {
    }

    /** Dummy SPI that that checks configuration consistency on servers only. */
    @IgniteSpiMultipleInstancesSupport(true)
    @IgniteSpiConsistencyChecked(optional = false, checkClient = false)
    private static class ConsistentNoClientsDeploymentSpi extends TestNoopDeploymentSpi {
    }

    /** Dummy SPI that that checks configuration consistency on servers and clients. */
    @IgniteSpiMultipleInstancesSupport(true)
    @IgniteSpiConsistencyChecked(optional = false)
    private static class ConsistentDeploymentSpi extends TestNoopDeploymentSpi {
    }

    /** Dummy SPI base. {@code toString()} is overridden, the rest is placeholder code. */
    private abstract static class TestNoopDeploymentSpi extends IgniteSpiAdapter implements DeploymentSpi {

        @Override public void spiStart(String igniteInstanceName) throws IgniteSpiException {
            // No-op.
        }

        @Override public void spiStop() throws IgniteSpiException {
            // No-op.
        }

        @Override public DeploymentResource findResource(String rsrcName) {
            return null;
        }

        @Override public boolean register(ClassLoader ldr, Class<?> rsrc) throws IgniteSpiException {
            return false;
        }

        @Override public boolean unregister(String rsrcName) {
            return false;
        }

        @Override public void setListener(DeploymentListener lsnr) {
            // No-op.
        }


        /** Returns the class simple name - to be used in test case names. */
        @Override public String toString() {
            return this.getClass().getSimpleName();
        }
    }
}
