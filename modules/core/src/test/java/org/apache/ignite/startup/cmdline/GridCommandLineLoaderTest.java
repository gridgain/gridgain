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

package org.apache.ignite.startup.cmdline;

import java.util.concurrent.CountDownLatch;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lifecycle.LifecycleBean;
import org.apache.ignite.lifecycle.LifecycleEventType;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.apache.ignite.testframework.junits.multijvm.IgniteProcessProxy;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_RESTART_CODE;

/**
 * Command line loader test.
 */
@GridCommonTest(group = "Loaders")
public class GridCommandLineLoaderTest extends GridCommonAbstractTest {
    /** */
    private static final String GRID_CFG_PATH = "/modules/core/src/test/config/loaders/grid-cfg.xml";

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLoader() throws Exception {
        String path = U.getIgniteHome() + GRID_CFG_PATH;

        info("Using Grids from configuration file: " + path);

        IgniteProcessProxy proxy = new IgniteProcessProxy(
            new IgniteConfiguration().setIgniteInstanceName("fake"), log, null) {
                @Override protected String igniteNodeRunnerClassName() {
                    return CommandLineStartup.class.getCanonicalName();
                }

                @Override protected String params(IgniteConfiguration cfg, boolean resetDiscovery) {
                    return path;
                }
            };

        try {
            GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    return !proxy.getProcess().getProcess().isAlive();
                }
            }, 150_000);
        }
        finally {
            if (proxy.getProcess().getProcess().isAlive())
                proxy.kill();
        }

        assertEquals(2, proxy.getProcess().getProcess().exitValue());
    }

    /**
     * Kills node after it is started.
     */
    public static class KillerLifecycleBean implements LifecycleBean {
        /**
         * The latch that is used to wait for the start of both nodes from the spring XML config.
         */
        private static final CountDownLatch INIT_TWO_NODES_LATCH = new CountDownLatch(2);

        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /**
         *
         */
        @Override public void onLifecycleEvent(LifecycleEventType evt) throws IgniteException {
            if (evt == LifecycleEventType.AFTER_NODE_START) {
                System.setProperty(IGNITE_RESTART_CODE, Integer.toString(
                    1 + IgniteSystemProperties.getInteger(IGNITE_RESTART_CODE, 0)));

                String name = ignite.name();

                System.out.println("Ignite instance seen, will shut it down. Name=" + name);

                new Thread(new Runnable() {
                    @Override public void run() {
                        try {
                            INIT_TWO_NODES_LATCH.await();
                        }
                        catch (InterruptedException e) {
                            e.printStackTrace();
                        }

                        System.out.println("Shutdown imminent. Name=" + name);

                        Ignition.stop(name, true);
                    }
                }).start();

                INIT_TWO_NODES_LATCH.countDown();
            }
        }
    }
}
