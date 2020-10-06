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

package org.apache.ignite.spi.discovery;

import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.events.DiscoveryCustomEvent;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.eventstorage.DiscoveryEventListener;
import org.apache.ignite.internal.managers.eventstorage.HighPriorityListener;
import org.apache.ignite.internal.processors.cluster.ChangeGlobalStateMessage;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.events.DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT;
import static org.apache.ignite.testframework.LogListener.matches;

/** */
public class ChangeGlobalStateMessageOrderTest extends GridCommonAbstractTest {
    /** */
    LogListener logLsnr;

    /** */
    @Test
    public void testChangeGlobalStateMessageOrder() throws Exception {
        startGrid(0);

        IgniteEx client = startClientGrid("Client1");

        DiscoveryEventListener myLsnr = new MyListener();

        client.context().event().addDiscoveryEventListener(myLsnr, EVT_DISCOVERY_CUSTOM_EVT);

        GridTestUtils.runAsync(() -> client.cluster().state(ClusterState.ACTIVE));

        doSleep(10000);

        assertFalse(logLsnr.check());
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /** */
    private CacheConfiguration<Object, Object> getCacheConfig(String cacheName) {
        return new CacheConfiguration<>(cacheName)
            .setPartitionLossPolicy(PartitionLossPolicy.READ_WRITE_SAFE);
    }

    /** */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setActiveOnStart(false);

        if (igniteInstanceName.contains("Client1")) {
            System.out.println("!rty");

            logLsnr = matches("AssertionError: DiscoveryDataClusterState").build();

            ListeningTestLogger log = new ListeningTestLogger(cfg.getGridLogger());

            log.registerAllListeners(logLsnr);

            cfg.setGridLogger(log);
        }

        return cfg;
    }

    /** */
    private static class MyListener implements HighPriorityListener, DiscoveryEventListener {
        /** */
        @Override public void onEvent(DiscoveryEvent evt, DiscoCache cache) {
            if (evt.type() == EVT_DISCOVERY_CUSTOM_EVT &&
                (((DiscoveryCustomEvent)evt).customMessage() instanceof ChangeGlobalStateMessage))
                doSleep(5000);
        }

        /** */
        @Override public int order() {
            return 0;
        }
    }
}
