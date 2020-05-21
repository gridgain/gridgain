/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.rest;

import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.processors.rest.request.GridRestCacheRequest;
import org.apache.ignite.lifecycle.LifecycleBean;
import org.apache.ignite.lifecycle.LifecycleEventType;
import org.apache.ignite.spi.discovery.tcp.TestTcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.lifecycle.LifecycleEventType.AFTER_NODE_STOP;


/**
 * Test for rest processor hanging on stop
 */
public class RestProcessorHangTest extends GridCommonAbstractTest {
    /**
     * {@inheritDoc}
     */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ConnectorConfiguration connectorConfiguration = new ConnectorConfiguration();

        cfg.setConnectorConfiguration(connectorConfiguration);

        return cfg;
    }

    /**
     * Test that node doesn't hang if there are rest requests and discovery SPI failed
     * https://ggsystems.atlassian.net/browse/GG-28633
     */
    @Test
    public void nodeStopOnDiscoverySpiFailTest() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);

        IgniteConfiguration failJoinCfg = getConfiguration();

        // discovery spi that never allows connecting
        TestTcpDiscoverySpi discoSpi = new TestTcpDiscoverySpi() {
            @Override protected void writeToSocket(TcpDiscoveryAbstractMessage msg, Socket sock, int res, long timeout) throws IOException {
                try {
                    // wait until request added to rest processor
                    latch.await();
                } catch (InterruptedException ignored) {
                }

                super.writeToSocket(msg, sock, 255, timeout);
            }
        };

        discoSpi.setIpFinder(sharedStaticIpFinder);

        failJoinCfg.setDiscoverySpi(discoSpi);

        startGrid(failJoinCfg);

        String gridName = "impossibleToJoin";

        IgniteConfiguration hangNodeCfg = getConfiguration(gridName);

        final LifecycleEventType[] lastEvent = {null};

        hangNodeCfg.setLifecycleBeans(new LifecycleBean() {
            @Override public void onLifecycleEvent(LifecycleEventType evt) throws IgniteException {
                lastEvent[0] = evt;
            }
        });

        GridTestUtils.runAsync(() -> startGrid(hangNodeCfg));

        GridTestUtils.waitForCondition(() -> {
            try {
                IgniteKernal failingGrid = IgnitionEx.gridx(gridName);

                return failingGrid != null && failingGrid.context().rest() != null;
            } catch (Exception ignored) {
                return false;
            }
        }, 20_000);

        IgniteEx hangGrid = IgnitionEx.gridx(gridName);

        GridRestProcessor rest = hangGrid.context().rest();

        new Thread(() -> {
            GridRestProtocolHandler hnd = GridTestUtils.getFieldValue(rest, "protoHnd");

            GridRestCacheRequest req = new GridRestCacheRequest();

            req.cacheName(DEFAULT_CACHE_NAME);

            req.command(GridRestCommand.CACHE_GET);

            req.key("k1");

            latch.countDown();

            try {
                // submitting cache get request to node that didn't fully start
                // must hang
                hnd.handle(req);
            } catch (IgniteCheckedException ignored) {
            }
        }).start();

        // node should stop correctly
        assertTrue(GridTestUtils.waitForCondition(() -> {
            return lastEvent[0] == AFTER_NODE_STOP;
        }, 10_000));
    }

}
