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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.ShutdownPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_WAIT_FOR_BACKUPS_ON_SHUTDOWN;

/**
 * Test checks various cluster shutdown and initiated policy.
 */
@WithSystemProperty(key = IGNITE_WAIT_FOR_BACKUPS_ON_SHUTDOWN, value = "false")
public class GracefulShutdownTest extends GridCacheDhtPreloadWaitForBackupsWithPersistenceTest {

    /** Shutdown policy of static configuration. */
    public ShutdownPolicy policy = ShutdownPolicy.GRACEFUL;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setShutdownPolicy(policy);
    }

    /**
     * Check static configuration of shutdown policy.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRestartWithStaticConfiguredPolicy() throws Exception {
        Ignite ignite0 = startGrid(0);

        assertSame(ignite0.cluster().shutdownPolicy(), ignite0.configuration().getShutdownPolicy());

        ignite0.close();

        policy = ShutdownPolicy.IMMEDIATE;

        ignite0 = startGrid(0);

        assertSame(ignite0.cluster().shutdownPolicy(), ignite0.configuration().getShutdownPolicy());

        assertSame(ignite0.cluster().shutdownPolicy(), ShutdownPolicy.IMMEDIATE);
    }

    /**
     * Test checked exception which is thrown when configuration of nodes different.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testTwoNodesWithDifferenConfuguration() throws Exception {
        Ignite ignite0 = startGrid(0);

        ignite0.cluster().active(true);

        assertSame(ignite0.configuration().getShutdownPolicy(), ShutdownPolicy.GRACEFUL);

        policy = ShutdownPolicy.IMMEDIATE;

        GridTestUtils.assertThrowsAnyCause(log, () -> startGrid(1), IgniteCheckedException.class,
            "Remote node hase shutdoun policy different from local local");
    }

    /**
     * Check dynamic configuration of shutdown policy.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRestartWithDynamicConfiguredPolicy() throws Exception {
        Ignite ignite0 = startGrid(0);

        ignite0.cluster().active(true);

        assertSame(ignite0.cluster().shutdownPolicy(), ignite0.configuration().getShutdownPolicy());

        ShutdownPolicy configuredPolicy = ignite0.cluster().shutdownPolicy();

        ShutdownPolicy policyToChange = null;

        for (ShutdownPolicy policy : ShutdownPolicy.values()) {
            if (policy != ignite0.cluster().shutdownPolicy())
                policyToChange = policy;
        }

        assertNotNull(policyToChange);

        ignite0.cluster().shutdownPolicy(policyToChange);

        ignite0.getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)).put(1, 1);

        forceCheckpoint();

        info("Policy to change: " + policyToChange);

        ignite0.close();

        ignite0 = startGrid(0);

        info("Policy after restart: " + ignite0.cluster().shutdownPolicy());

        assertNotSame(ignite0.cluster().shutdownPolicy(), ignite0.configuration().getShutdownPolicy());

        assertSame(ignite0.cluster().shutdownPolicy(), policyToChange);
    }
}
