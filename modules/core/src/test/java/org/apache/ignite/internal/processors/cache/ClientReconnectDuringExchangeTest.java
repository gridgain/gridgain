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

package org.apache.ignite.internal.processors.cache;

import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.GridComponent;
import org.apache.ignite.internal.GridKernalContextImpl;
import org.apache.ignite.internal.GridPluginComponent;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.IgniteDiscoverySpi;
import org.apache.ignite.internal.managers.eventstorage.DiscoveryEventListener;
import org.apache.ignite.internal.managers.eventstorage.HighPriorityListener;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.PartitionsExchangeAware;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteTxExceptionNodeFailTest;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.events.EventType.EVT_CLIENT_NODE_RECONNECTED;

/**
 *
 */
public class ClientReconnectDuringExchangeTest extends GridCommonAbstractTest {
    /** Servers. */
    public static final int SERVERS = 3;
    public static final int[] TYPES = {EventType.EVT_NODE_JOINED};
    public static final String CLIENT = "client";

    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName);
    }

    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();
    }

    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    @Test
    public void test() throws Exception {
        IgniteEx ignite0 = startGrid(SERVERS);

        IgniteEx client = startClientGrid(CLIENT);

        CountDownLatch waitPmeLatch = new CountDownLatch(1);
        CountDownLatch startPmeLatch = new CountDownLatch(1);

        client.context().cache().context().exchange().registerExchangeAwareComponent(new PartitionsExchangeAware() {
            @Override public void onInitBeforeTopologyLock(GridDhtPartitionsExchangeFuture fut) {
                try {
                    startPmeLatch.countDown();

                    info("Waiting latch...");

                    waitPmeLatch.await();
                }
                catch (InterruptedException e) {
                    log.error("Exchange pause interrupted.", e);
                }
            }
        });

        LinkedList<GridComponent> comps = U.field(client.context(), "comps");

        CountDownLatch disconStopLatch = new CountDownLatch(1);

        comps.addLast(new NotifyStopperGridPlugin(disconStopLatch));

        IgniteInternalFuture exchangeTriggerFut = GridTestUtils.runAsync(() -> {
            try {
                startClientGrid("client2");
            }
            catch (Exception e) {
                e.printStackTrace();
            }

//            ignite0.getOrCreateCache(DEFAULT_CACHE_NAME);
        });

        startPmeLatch.await();

        ((IgniteDiscoverySpi)client.configuration().getDiscoverySpi()).clientReconnect();

        waitPmeLatch.countDown();

        exchangeTriggerFut.get();

        doSleep(10_000);

        disconStopLatch.countDown();

        doSleep(10_000);
    }

    @Test
    public void testClientReconnectOnNodeJoining() throws Exception {
        IgniteEx ignite0 = startGrids(SERVERS);

        IgniteEx client = startClientGrid(CLIENT);

//        LinkedList<GridComponent> comps = U.field(client.context(), "comps");
//
//        CountDownLatch disconStopLatch = new CountDownLatch(1);
//
//        comps.addLast(new NotifyStopperGridPlugin(disconStopLatch));

        new TestDiscoveryNodeJoinListener(CLIENT);

        startClientGrid("client2");

        doSleep(10_000);

//        disconStopLatch.countDown();
//        IgniteInternalFuture exchangeTriggerFut = GridTestUtils.runAsync(() -> {
//            try {
//                startClientGrid("client2");
//            }
//            catch (Exception e) {
//                e.printStackTrace();
//            }
//
////            ignite0.getOrCreateCache(DEFAULT_CACHE_NAME);
//        });

    }

    @Test
    public void test3() throws Exception {
        IgniteEx ignite0 = startGrid(SERVERS);

        IgniteEx client = startClientGrid(CLIENT);

        CountDownLatch waitPmeLatch = new CountDownLatch(1);
        CountDownLatch startPmeLatch = new CountDownLatch(1);

        client.context().cache().context().exchange().registerExchangeAwareComponent(new PartitionsExchangeAware() {
            @Override public void onInitBeforeTopologyLock(GridDhtPartitionsExchangeFuture fut) {
                try {
                    startPmeLatch.countDown();

                    info("Waiting latch...");

                    waitPmeLatch.await();

                    if (Thread.currentThread().isInterrupted())
                        Thread.currentThread().interrupted();

                    assertFalse(Thread.currentThread().isInterrupted());
                }
                catch (InterruptedException e) {
                    log.error("Exchange pause interrupted.", e);
                }
            }
        });

        IgniteInternalFuture exchangeTriggerFut = GridTestUtils.runAsync(() -> {
            try {
                startClientGrid("client2");
            }
            catch (Exception e) {
                e.printStackTrace();
            }

//            ignite0.getOrCreateCache(DEFAULT_CACHE_NAME);
        });

        startPmeLatch.await();

        new TestDiscoveryNodeJoinListener(CLIENT);

        ((IgniteDiscoverySpi)client.configuration().getDiscoverySpi()).clientReconnect();

        ClientReconnectListener reconnectListener = new ClientReconnectListener(CLIENT);

//        GridTestUtils.waitForCondition(() -> client.cluster().clientReconnectFuture() != null, 10_000);
//
//        client.cluster().clientReconnectFuture().get();

        reconnectListener.waitForEvent();

        waitPmeLatch.countDown();

        exchangeTriggerFut.get();

//        doSleep(10_000);
    }

    @Test
    public void test4() throws Exception {
        IgniteEx ignite0 = startGrid(SERVERS);

        IgniteEx client = startClientGrid(CLIENT);

        LinkedBlockingDeque<CachePartitionExchangeWorkerTask> exWorker = U.field((Object)U.field(client.context().cache().context().exchange(), "exchWorker"), "futQ");

        CountDownLatch latch = new CountDownLatch(1);

        TestClientCacheChangeDummyDiscoveryMessage exchangeTask = new TestClientCacheChangeDummyDiscoveryMessage(latch);

        exWorker.add(exchangeTask);

        IgniteInternalFuture exchangeTriggerFut = GridTestUtils.runAsync(() -> {
            try {
                startClientGrid("client2");
            }
            catch (Exception e) {
                e.printStackTrace();
            }

//            ignite0.getOrCreateCache(DEFAULT_CACHE_NAME);
        });

        exchangeTask.waitForExchange();

        info("Exchange worker is waiting on dummy event.");


        ((IgniteDiscoverySpi)client.configuration().getDiscoverySpi()).clientReconnect();

        latch.countDown();

        exchangeTriggerFut.get();
    }


    /**
     * Plugin for waiting on a stop.
     */
    private static class NotifyStopperGridPlugin extends GridPluginComponent {
        /** Latch to wait on a stop. */
        private final CountDownLatch latch;

        /**
         * @param latch Latch.
         */
        public NotifyStopperGridPlugin(CountDownLatch latch) {
            super(new AbstractTestPluginProvider() {
                @Override public String name() {
                    return "Stopper plugin.";
                }
            });

            this.latch = latch;
        }

        /** {@inheritDoc} */
        @Override public void onDisconnected(IgniteFuture<?> reconnectFut) {
            try {
                log.info("Plugin is stopping.");

                if (latch != null)
                    U.await(latch);
            }
            catch (IgniteInterruptedCheckedException e) {
                log.error("An interruption is happened in waiting on the stop.");
            }

            super.onDisconnected(reconnectFut);
        }
    }

    /**
     * A test discovery listener to freeze handling node join events.
     */
    private class TestDiscoveryNodeJoinListener implements DiscoveryEventListener, HighPriorityListener {
        /** Name node to subscribe listener. */
        private final String nodeToSubscribe;

        /**
         * @param nodeToSubscribe Node to subscribe.
         */
        public TestDiscoveryNodeJoinListener(String nodeToSubscribe) {
            this.nodeToSubscribe = nodeToSubscribe;

            grid(nodeToSubscribe).context().event().addDiscoveryEventListener(this, TYPES);
        }

        /** {@inheritDoc} */
        @Override public void onEvent(DiscoveryEvent evt, DiscoCache discoCache) {
            info("Reconnect client: [name=" + nodeToSubscribe +
                ", nodeEvt=" + evt.node().consistentId() + ']');

            IgniteEx client = grid(nodeToSubscribe);

            GridTestUtils.runAsync(() -> ((IgniteDiscoverySpi)client.configuration().getDiscoverySpi()).clientReconnect());

            client.context().event().removeDiscoveryEventListener(this, TYPES);
        }

        /** {@inheritDoc} */
        @Override public int order() {
            return 0;
        }
    }

    /**
     * A test discovery listener to freeze handling node join events.
     */
    private class ClientReconnectListener implements DiscoveryEventListener {
        /** Name node to subscribe listener. */
        private final String nodeToSubscribe;

        private CountDownLatch evtLatch = new CountDownLatch(1);

        /**
         * @param nodeToSubscribe Node to subscribe.
         */
        public ClientReconnectListener(String nodeToSubscribe) {
            this.nodeToSubscribe = nodeToSubscribe;

            grid(nodeToSubscribe).context().event().addDiscoveryEventListener(this, new int[] {EVT_CLIENT_NODE_RECONNECTED});
        }

        /** {@inheritDoc} */
        @Override public void onEvent(DiscoveryEvent evt, DiscoCache discoCache) {
            info("Reconnect client: [name=" + nodeToSubscribe +
                ", nodeEvt=" + evt.node().consistentId() + ']');

            evtLatch.countDown();

//            IgniteEx client = grid(nodeToSubscribe);
//
//            GridTestUtils.runAsync(() -> ((IgniteDiscoverySpi)client.configuration().getDiscoverySpi()).clientReconnect());
//
            grid(nodeToSubscribe).context().event().removeDiscoveryEventListener(this, new int[] {EVT_CLIENT_NODE_RECONNECTED});
        }

        /**
         * Waits for a first event.
         *
         * @throws InterruptedException If interrupted.
         */
        public void waitForEvent() throws InterruptedException {
            evtLatch.await();
        }
    }

    private static class TestClientCacheChangeDummyDiscoveryMessage implements CachePartitionExchangeWorkerTask {

        private final CountDownLatch latch;

        private final CountDownLatch waitForExchangeLatch = new CountDownLatch(1);

        public TestClientCacheChangeDummyDiscoveryMessage(CountDownLatch latch) {
            this.latch = latch;
        }

        /** {@inheritDoc} */
        @Override public boolean skipForExchangeMerge() {
            return false;
        }

        @Override public String toString() {
            waitForExchangeLatch.countDown();

            try {
                latch.await();
            }
            catch (InterruptedException e) {
                log.info("Interrupted err=" + e.getMessage());

                Thread.currentThread().interrupted();
            }

            return super.toString();
        }

        public void waitForExchange() throws InterruptedException {
            waitForExchangeLatch.await();
        }
    }
}
