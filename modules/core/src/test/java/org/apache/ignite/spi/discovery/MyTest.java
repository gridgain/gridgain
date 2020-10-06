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

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.discovery.CustomMessageWrapper;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.processors.cache.CacheInvalidStateException;
import org.apache.ignite.internal.processors.cluster.BaselineTopology;
import org.apache.ignite.internal.processors.cluster.ChangeGlobalStateFinishMessage;
import org.apache.ignite.internal.processors.cluster.ChangeGlobalStateMessage;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.discovery.tcp.ServerImpl;
import org.apache.ignite.spi.discovery.tcp.TestTcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryCustomEventMessage;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;
import org.junit.Test;

/** */
public class MyTest extends GridCommonAbstractTest {
    /** */
    private static final String TEST_CACHE_NAME = "testcache";

    private volatile ClusterState state;

    ChangeGlobalStateMessage stateMessage;

    ChangeGlobalStateFinishMessage stateFinishMessage;

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    GridTestUtils.DiscoveryHook discoveryHook = new GridTestUtils.DiscoveryHook() {
        @Override public void beforeDiscovery(DiscoveryCustomMessage customMsg) {

            if (stateMessage == null && customMsg instanceof ChangeGlobalStateMessage
                && Thread.currentThread().getName().contains("Test2")) {
                stateMessage = (ChangeGlobalStateMessage) customMsg;
//                System.out.println("!asdf1");
//                doSleep(100);
            }
            if (stateFinishMessage == null && customMsg instanceof ChangeGlobalStateFinishMessage
                && Thread.currentThread().getName().contains("Test2")) {
                stateFinishMessage = (ChangeGlobalStateFinishMessage) customMsg;
//                System.out.println("!asdf1");
//                doSleep(100);
            }
        }

//        @Override public void afterDiscovery(DiscoveryCustomMessage customMsg) {
//            if (customMsg instanceof ChangeGlobalStateMessage) {
//                System.out.println("!asdf2");
//                doSleep(1000);
//            }
//        }
    };

    /** */
    private CacheConfiguration<Object, Object> getCacheConfig(String cacheName) {
        return new CacheConfiguration<>(cacheName)
            .setPartitionLossPolicy(PartitionLossPolicy.READ_WRITE_SAFE);
    }

    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setActiveOnStart(false);

        TestTcpDiscoverySpi discoSpi = new TestTcpDiscoverySpi();

        cfg.setDiscoverySpi(discoSpi);

//        if (igniteInstanceName.endsWith("Test0")) {
            discoSpi.discoveryHook(discoveryHook);
//        }

        return cfg;
    }

    @Test
    public void test1() throws Exception {
        startGrid(0);
        startClientGrid(2);

        IgniteEx node = grid(2);

        node.cluster().active(true);

        System.out.println("!1" + stateMessage);
        System.out.println("!2" + stateFinishMessage);

        node.cluster().active(false);

        BaselineTopology baselineTopology = grid(0).context().state().calculateNewBaselineTopology(ClusterState.ACTIVE, grid(0).cluster().currentBaselineTopology(), false);

//        DiscoveryNotification notification = new DiscoveryNotification(18, 2, grid(0).cluster().localNode(),
//            /*grid(0).context().discovery().topology(2)*/null, null,
//            new CustomMessageWrapper(stateMessage), null);
        //type = 18, topVer = 2, node = coordinator, topSnapshot = сервер и клиент,
        // topHist = коллекция topSnapshot'ов, data = CustomMessageWrapper в котором stateMessage, null

        stateMessage.baselineTopology = baselineTopology;//BaselineTopology.build(grid(0).cluster().currentBaselineTopology(), (int)grid(0).cluster().topologyVersion());
//        stateFinishMessage = BaselineTopology.build(grid(0).cluster().currentBaselineTopology(), (int)grid(0).cluster().topologyVersion());

        IgniteUuid uuid = IgniteUuid.randomUuid();

        stateMessage.reqId = uuid.globalId();
        stateMessage.id = IgniteUuid.randomUuid();
        stateFinishMessage.reqId = uuid.globalId();
        stateFinishMessage.id = IgniteUuid.randomUuid();

        stateMessage.timestamp(System.currentTimeMillis());
        stateMessage.exchangeActions = null;

//        grid(0).context().discovery().getSpi().getMsgWorker();

        TestTcpDiscoverySpi spi0 = (TestTcpDiscoverySpi)(grid(0).context().discovery().getSpi());

//        U.marshal(spi.marshaller(), msg)
        CustomMessageWrapper wrapperStateMessage = new CustomMessageWrapper(stateMessage);
        TcpDiscoveryCustomEventMessage eventStateMessage = new TcpDiscoveryCustomEventMessage(grid(0).localNode().id(), wrapperStateMessage, U.marshal(spi0.marshaller(), wrapperStateMessage));
        eventStateMessage.verify(grid(0).localNode().id());

        CustomMessageWrapper wrapperFinishMessage = new CustomMessageWrapper(stateFinishMessage);
        TcpDiscoveryCustomEventMessage eventFinishMessage = new TcpDiscoveryCustomEventMessage(grid(0).localNode().id(), wrapperFinishMessage, U.marshal(spi0.marshaller(), wrapperFinishMessage));
        eventFinishMessage.verify(grid(0).localNode().id());
//        grid(0).context().discovery().sendCustomEvent(stateMessage);
//        grid(0).context().discovery().sendCustomEvent(stateFinishMessage);


//        grid(0).context().discovery().getSpi();
        //grid(0).context().discovery().getSpi().getImpl().
        //grid(0).context().discovery().getSpi().getImpl().clientMsgWorkers
        //grid(0).context().discovery() event and notifier workers; discoWrk discoNtfWrk
        //grid(0).context().discovery().discoNtfWrk.queue
        //grid(0).context().discovery().discoWrk.evts

//        grid(0).context().discovery().getSpi().getImpl().clientMsgWorkers;

        for (ServerImpl.ClientMessageWorker clientMsgWorker :
            ((ServerImpl)(((TestTcpDiscoverySpi)(grid(0).context().discovery().getSpi())).impl)).clientMsgWorkers.values()) {
            clientMsgWorker.addMessage(eventFinishMessage, null);
        }

        for (ServerImpl.ClientMessageWorker clientMsgWorker :
            ((ServerImpl)(((TestTcpDiscoverySpi)(grid(0).context().discovery().getSpi())).impl)).clientMsgWorkers.values()) {
            clientMsgWorker.addMessage(eventStateMessage, null);
        }



        doSleep(1000);


        Assert.assertTrue(node.cluster().active());
    }

//    @Test //успешно активируется после отправки сообщения в ручную
//    public void test1() throws Exception {
//        startGrid(0);
//        startClientGrid(2);
//
//        IgniteEx node = grid(2);
//
//        node.cluster().active(true);
//
//        System.out.println("!1" + stateMessage);
//        System.out.println("!2" + stateFinishMessage);
//
//        node.cluster().active(false);
//
//        BaselineTopology baselineTopology = grid(0).context().state().calculateNewBaselineTopology(ClusterState.ACTIVE, grid(0).cluster().currentBaselineTopology(), false);
//
////        DiscoveryNotification notification = new DiscoveryNotification(18, 2, grid(0).cluster().localNode(),
////            /*grid(0).context().discovery().topology(2)*/null, null,
////            new CustomMessageWrapper(stateMessage), null);
//        //type = 18, topVer = 2, node = coordinator, topSnapshot = сервер и клиент,
//        // topHist = коллекция topSnapshot'ов, data = CustomMessageWrapper в котором stateMessage, null
//
//        stateMessage.baselineTopology = baselineTopology;//BaselineTopology.build(grid(0).cluster().currentBaselineTopology(), (int)grid(0).cluster().topologyVersion());
////        stateFinishMessage = BaselineTopology.build(grid(0).cluster().currentBaselineTopology(), (int)grid(0).cluster().topologyVersion());
//
//        IgniteUuid uuid = IgniteUuid.randomUuid();
//
//        stateMessage.reqId = uuid.globalId();
//        stateMessage.id = IgniteUuid.randomUuid();
//        stateFinishMessage.reqId = uuid.globalId();
//        stateFinishMessage.id = IgniteUuid.randomUuid();
//
//        stateMessage.timestamp(System.currentTimeMillis());
//        stateMessage.exchangeActions = null;
//
////        grid(0).context().discovery().getSpi().getMsgWorker();
//
//
//        grid(0).context().discovery().sendCustomEvent(stateMessage);
////        grid(0).context().discovery().sendCustomEvent(stateFinishMessage);
//
//
////        grid(0).context().discovery().getSpi();
//        //grid(0).context().discovery().getSpi().getImpl().
//        //grid(0).context().discovery().getSpi().getImpl().clientMsgWorkers
//        //grid(0).context().discovery() event and notifier workers; discoWrk discoNtfWrk
//        //grid(0).context().discovery().discoNtfWrk.queue
//        //grid(0).context().discovery().discoWrk.evts
//
////        grid(0).context().discovery().getSpi().getImpl().clientMsgWorkers;
//
////        for (ServerImpl.ClientMessageWorker clientMsgWorker :
////            ((ServerImpl)(((TestTcpDiscoverySpi)(grid(0).context().discovery().getSpi())).impl)).clientMsgWorkers.values()) {
////            clientMsgWorker.addMessage(stateMessage, null);
////        }
//
//
//
//        doSleep(1000);
//
//
//        Assert.assertTrue(node.cluster().active());
//    }


//    @Test
//    public void test1() throws Exception {
//        startGrid(0);
//        startClientGrid(2);
//
//        IgniteEx node = grid(2);
//
//        node.cluster().active(true);
//
//        System.out.println("!1" + stateMessage);
//        System.out.println("!2" + stateFinishMessage);
//
//        node.cluster().active(false);
//
//        BaselineTopology baselineTopology = grid(0).context().state().calculateNewBaselineTopology(ClusterState.ACTIVE, grid(0).cluster().currentBaselineTopology(), false);
//
//        DiscoveryNotification notification = new DiscoveryNotification(18, 2, grid(0).cluster().localNode(),
//            /*grid(0).context().discovery().topology(2)*/null, null,
//            new CustomMessageWrapper(stateMessage), null);
//        //type = 18, topVer = 2, node = coordinator, topSnapshot = сервер и клиент,
//        // topHist = коллекция topSnapshot'ов, data = CustomMessageWrapper в котором stateMessage, null
//
//        stateMessage.baselineTopology = baselineTopology;//BaselineTopology.build(grid(0).cluster().currentBaselineTopology(), (int)grid(0).cluster().topologyVersion());
////        stateFinishMessage = BaselineTopology.build(grid(0).cluster().currentBaselineTopology(), (int)grid(0).cluster().topologyVersion());
//
//        IgniteUuid uuid = IgniteUuid.randomUuid();
//
//        stateMessage.reqId = uuid.globalId();
//        stateFinishMessage.reqId = uuid.globalId();
//
//        stateMessage.timestamp(System.currentTimeMillis());
//        stateMessage.exchangeActions = null;
//
////        grid(0).context().discovery().getSpi().getMsgWorker();
//
////        grid(0).context().discovery().sendCustomEvent(stateFinishMessage);
////        grid(0).context().discovery().sendCustomEvent(stateMessage);
//
////        grid(0).context().discovery().getSpi();
//        //grid(0).context().discovery().getSpi().getImpl().
//        //grid(0).context().discovery().getSpi().getImpl().clientMsgWorkers
//        //grid(0).context().discovery() event and notifier workers; discoWrk discoNtfWrk
//        //grid(0).context().discovery().discoNtfWrk.queue
//        //grid(0).context().discovery().discoWrk.evts
//
//        GridFutureAdapter<?> notificationFut = new GridFutureAdapter<>();
//
//        grid(0).context().discovery().discoNtfWrk.queue.add(notificationFut, notification);
//
//
//        doSleep(1000);
//
//
////        Assert.assertTrue(node.cluster().active());
//    }

//    @Test
//    public void test1() throws Exception {
////        IgniteEx grid = startGrid(0);
////        grid.cluster().baselineAutoAdjustEnabled(false);
////        startGrid(1);
////        startGrid(2);
//
//
////        startGrid(1);
////        IgniteEx client = startClientGrid(2);
////
////        client.cluster().state(ClusterState.ACTIVE);
//
//        Thread[] a = new Thread[3];
//        System.out.println("q");
//        for(int i = 0; i < 3; i++) {
//            a[i] = new Thread(() -> doSleep(2000));
//            a[i].start();
//            a[i].join();
//        }
//        System.out.println("w");
//
//
////        System.out.println("!6" + client.cluster().state());
//    }

//    @Test
//    public void test1() throws Exception {
//        IgniteEx ig = startGrids(2);
//
//        IgniteEx client = startClientGrid(2);
//
//        client.cluster().active(true);
//    }
}
