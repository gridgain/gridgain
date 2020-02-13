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

package org.apache.ignite.internal.processors.metastorage;

import java.util.function.Predicate;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageImpl;
import org.apache.ignite.internal.util.lang.GridAbsPredicateX;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.TestTcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryCustomEventMessage;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Test for {@link DistributedMetaStorageImpl} with disabled persistence.
 */
public class DistributedMetaStorageStopTest extends GridCommonAbstractTest {
    /** **/
    private TcpDiscoverySpi customTcpDiscoverySpi = null;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setWalSegments(3)
            .setWalSegmentSize(512 * 1024)
        );

        DiscoverySpi discoSpi = cfg.getDiscoverySpi();

        if (discoSpi instanceof TcpDiscoverySpi) {
            if (customTcpDiscoverySpi != null)
                cfg.setDiscoverySpi(
                    customTcpDiscoverySpi
                        .setIpFinder(((TcpDiscoverySpi)cfg.getDiscoverySpi()).getIpFinder())
                );

            ((TcpDiscoverySpi)discoSpi).setNetworkTimeout(1000);
        }

        if (igniteInstanceName.contains("client"))
            cfg.setClientMode(true);

        return cfg;
    }

    /** */
    @Before
    public void before() throws Exception {
        stopAllGrids();
    }

    /** */
    @After
    public void after() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNodeStopBeforeRecordStored() throws Exception {
        ConditionExecutionTcpDiscoverySpi discoverySpi = new ConditionExecutionTcpDiscoverySpi();

        customTcpDiscoverySpi = discoverySpi;

        IgniteEx ignite0 = startGrid(0);

        ignite0.configuration();

        customTcpDiscoverySpi = null;

        IgniteEx ignite1 = startGrid(1);

        ignite0.cluster().active(true);

        discoverySpi.predicate(TcpDiscoveryCustomEventMessage.class::isInstance, ignite1::close);

        try {
            metastorage(1).write("key0", "value0");

            fail("Write should fail cause node should be stopped");
        }
        catch (IgniteCheckedException expected) {
            //Expected that write would be failed because node should be stopped before this message reached the node.
        }

        assertTrue(waitForCondition(new GridAbsPredicateX() {
            @Override public boolean applyx() throws IgniteCheckedException {
                return "value0".equals(metastorage(0).read("key0"));
            }
        }, 10_000));
    }

    /** */
    protected IgniteEx startClient(int idx) throws Exception {
        return startGrid(getConfiguration(getTestIgniteInstanceName(idx)).setClientMode(true));
    }

    /**
     * @return {@link DistributedMetaStorage} instance for i'th node.
     */
    protected DistributedMetaStorage metastorage(int i) {
        return grid(i).context().distributedMetastorage();
    }

    /**
     * Tcp discovery which execute some code by configured condition.
     */
    private static class ConditionExecutionTcpDiscoverySpi extends TestTcpDiscoverySpi {
        /** **/
        private volatile Predicate<TcpDiscoveryAbstractMessage> cond;

        /** **/
        private volatile Runnable runnable;

        /** {@inheritDoc} **/
        @Override protected void startMessageProcess(TcpDiscoveryAbstractMessage msg) {
            if (cond != null && cond.test(msg))
                runnable.run();

            super.startMessageProcess(msg);
        }

        /**
         * Condition by which runnable should be executed.
         *
         * @param cond Triggered condition.
         * @param runnable Code for execution.
         */
        public void predicate(Predicate<TcpDiscoveryAbstractMessage> cond, Runnable runnable) {
            this.runnable = runnable;
            this.cond = cond;
        }
    }
}
