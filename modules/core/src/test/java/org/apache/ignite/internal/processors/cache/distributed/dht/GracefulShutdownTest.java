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
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.ShutdownPolicy;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test checks various cluster shutdown and initiated policy.
 */
public class GracefulShutdownTest extends GridCommonAbstractTest {

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setShutdownPolicy(ShutdownPolicy.GRACEFUL)
            .setPeerClassLoadingEnabled(false)
            .setActiveOnStart(false);
    }

    @Test
    public void test() throws Exception {
        Ignite ignite0 = startGrid(0);

        assertFalse(ignite0.cluster().active());

        for (int i = 0; i < 10; i++) {
            assertFalse(ignite0.cluster().active());

            info("Schutdown: " + ignite0.cluster().shutdownPolicy());

            ignite0.cluster().shutdownPolicy(ShutdownPolicy.GRACEFUL);

            ignite0.cluster().active(true);

            assertTrue(ignite0.cluster().active());

            info("Schutdown: " + ignite0.cluster().shutdownPolicy());

            ignite0.cluster().shutdownPolicy(ShutdownPolicy.IMMEDIATE);

            ignite0.cluster().active(false);
        }

    }

    @Test
    public void testTwoNodesWithDifferenConfuguration() throws Exception {
        Ignite ignite0 = startGrid(0);

        assertSame(ignite0.configuration().getShutdownPolicy(), ShutdownPolicy.GRACEFUL);

        IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName(1))
            .setClientMode(true)
            .setShutdownPolicy(ShutdownPolicy.GRACEFUL)
//            .setCommunicationSpi(new TestRecordingCommunicationSpi())
//            .setPeerClassLoadingEnabled(true)
            ;

        Ignite ignite1 = startGrid(optimize(cfg));

        ignite1.cluster().active(true);

        assertSame(ignite1.cluster().shutdownPolicy(), ShutdownPolicy.GRACEFUL);


    }
}
