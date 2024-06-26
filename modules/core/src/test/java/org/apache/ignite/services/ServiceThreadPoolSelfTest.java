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

package org.apache.ignite.services;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test verifying that services thread pool is properly used.
 */
public class ServiceThreadPoolSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDefaultPoolSize() throws Exception {
        Ignite ignite = startGrid("grid", new IgniteConfiguration());

        IgniteConfiguration cfg = ignite.configuration();

        assertEquals(IgniteConfiguration.DFLT_PUBLIC_THREAD_CNT, cfg.getPublicThreadPoolSize());
        assertEquals(IgniteConfiguration.DFLT_PUBLIC_THREAD_CNT, cfg.getServiceThreadPoolSize());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testInheritedPoolSize() throws Exception {
        Ignite ignite = startGrid("grid", new IgniteConfiguration().setPublicThreadPoolSize(42));

        IgniteConfiguration cfg = ignite.configuration();

        assertEquals(42, cfg.getPublicThreadPoolSize());
        assertEquals(42, cfg.getServiceThreadPoolSize());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCustomPoolSize() throws Exception {
        Ignite ignite = startGrid("grid", new IgniteConfiguration().setServiceThreadPoolSize(42));

        IgniteConfiguration cfg = ignite.configuration();

        assertEquals(IgniteConfiguration.DFLT_PUBLIC_THREAD_CNT, cfg.getPublicThreadPoolSize());
        assertEquals(42, cfg.getServiceThreadPoolSize());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testExecution() throws Exception {
        startGrid(0); // Server.

        Ignition.setClientMode(true);

        Ignite ignite = startGrid(); // Client.

        ignite.services().deployClusterSingleton("my-service", new MyServiceImpl());

        MyService svc = ignite.services().serviceProxy("my-service", MyService.class, false);

        svc.hello();
    }

    /**
     */
    private interface MyService extends Service {
        /**
         * Hello!
         */
        void hello();
    }

    /**
     */
    private static class MyServiceImpl implements MyService {
        /** {@inheritDoc} */
        @Override public void hello() {
            String thread = Thread.currentThread().getName();

            assertTrue("Service is executed in wrong thread: " + thread, thread.startsWith("svc-#"));
        }

        /** {@inheritDoc} */
        @Override public void cancel(ServiceContext ctx) {
        }

        /** {@inheritDoc} */
        @Override public void init(ServiceContext ctx) throws Exception {
        }

        /** {@inheritDoc} */
        @Override public void execute(ServiceContext ctx) throws Exception {
        }
    }
}
