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

package org.apache.ignite.spi.discovery.tcp;

import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryDiscardMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeAddFinishedMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeAddedMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeFailedMessage;
import org.apache.ignite.testframework.discovery.BlockedDiscoverySpi;
import org.apache.ignite.testframework.discovery.DiscoveryController;
import org.apache.ignite.testframework.discovery.TestDiscoveryCustomMessage;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.events.DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.discovery.HasOneTimeEachInOrder.hasOneTimeEachInOrder;
import static org.apache.ignite.testframework.discovery.IsDiscoveryCustomMessage.isTestMessage;
import static org.apache.ignite.testframework.discovery.IsDiscoveryEvent.isDiscoveryEvent;
import static org.apache.ignite.testframework.discovery.IsDiscoveryEventMessage.isTestEventMessage;
import static org.apache.ignite.testframework.discovery.IsDiscoveryMessage.isDiscoveryMessage;
import static org.apache.ignite.testframework.discovery.IsNode.isNode;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;

/**
 * Test for {@link TcpDiscoverySpi}.
 */
public class TcpDiscoveryOrderMessagesTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If fails.
     */
    public TcpDiscoveryOrderMessagesTest() {
        super(false);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** **/
    private DiscoveryController start(int ind) throws Exception {
        return start(ind, false);
    }

    /**
     * Custom start grid for this test which included DiscoverySpi configuration and other.
     *
     * @param ind Starting node id.
     * @param connectToCrd {@code true} this node will be connected to coordinator directly.
     * @return Wrapper of node for managing discovery.
     * @throws Exception if failed.
     */
    private DiscoveryController start(int ind, boolean connectToCrd) throws Exception {
        String igniteInstanceName = getTestIgniteInstanceName(ind);

        IgniteConfiguration cfg = getConfiguration(igniteInstanceName);

        //Custom discovery spi for managing messages.
        TcpDiscoverySpi spi = new BlockedDiscoverySpi();

        spi.setIpFinder(new TcpDiscoveryVmIpFinder(false) {{
            setAddresses(Collections.singleton(connectToCrd ? "127.0.0.1:47500" : "127.0.0.1:47500..47509"));
        }});

        cfg.setDiscoverySpi(spi);

        //Add listener to store all processed events for further check.
        ConcurrentLinkedQueue<DiscoveryEvent> processedEvts = new ConcurrentLinkedQueue<>();

        int[] listenedEvts = {EVT_NODE_FAILED, EVT_NODE_LEFT, EVT_NODE_JOINED, EVT_DISCOVERY_CUSTOM_EVT};

        cfg.setLocalEventListeners(new HashMap<IgnitePredicate<? extends Event>, int[]>() {{
            put(event -> processedEvts.add((DiscoveryEvent)event), listenedEvts);
        }});

        cfg.setFailureHandler(new StopNodeFailureHandler());

        IgniteEx ignite = startGrid(optimize(cfg));

        return new DiscoveryController(ignite, processedEvts);
    }

    /**
     * @throws Exception If fail.
     */
    @Test
    public void shouldNotDuplicateMessagesWhenCrdFailed() throws Exception {
        //given:
        DiscoveryController crd = start(0);
        DiscoveryController node1 = start(1);
        DiscoveryController node2 = start(2);
        DiscoveryController node3 = start(3);

        //Successful message
        crd.sendCustomEvent(new TestDiscoveryCustomMessage("successfullyWentAroundWholeRing"));
        node3.awaitProcessedEvent(isTestEventMessage("successfullyWentAroundWholeRing"));

        //Sending a custom message and blocking execution of its discard message on node2
        node2.blockIf(instanceOf(TcpDiscoveryDiscardMessage.class));

        crd.sendCustomEvent(new TestDiscoveryCustomMessage("discardMsgBlockedBeforeNode3"));

        //After this - all nodes except node3 received discard message.
        node2.awaitBlocking();

        crd.blockIf(isTestMessage("blockedOnCrd"));

        node3.sendCustomEvent(new TestDiscoveryCustomMessage("blockedOnCrd"));

        //Failing coordinator before discard message went around the whole ring and when it received 'blockedOnCrd' message.
        node1.blockIf(isTestMessage("blockedOnCrd"));

        crd.failWhenBlocked();

        //Continue execution when node3 has sent all pending messages to node1('blockedOnCrd' is last one)
        node1.releaseWhenBlocked();
        node2.release();

        node1.awaitProcessedEvent(isDiscoveryEvent(EVT_NODE_FAILED));

        //expected: node1 - new crd
        assertThat(node1.processedEvents(), hasOneTimeEachInOrder(
            isDiscoveryEvent(EVT_NODE_JOINED), //node2
            isDiscoveryEvent(EVT_NODE_JOINED), //node3
            isTestEventMessage("successfullyWentAroundWholeRing"), //from crd
            isTestEventMessage("discardMsgBlockedBeforeNode3"), //from crd
            isTestEventMessage("blockedOnCrd"), //from node3 after crd failed and this node became crd
            isDiscoveryEvent(EVT_NODE_FAILED) //from node3 after crd failed
        ));

        assertThat(node1.startProcessingMessages(), hasOneTimeEachInOrder(
            instanceOf(TcpDiscoveryNodeAddedMessage.class), //this node
            instanceOf(TcpDiscoveryNodeAddedMessage.class), //node2
            instanceOf(TcpDiscoveryNodeAddedMessage.class), //node3
            isTestMessage("successfullyWentAroundWholeRing"), //from crd
            isTestMessage("discardMsgBlockedBeforeNode3"), //from crd
            isTestMessage("discardMsgBlockedBeforeNode3"), //from node3 after crd failed due to node3 hasn't received discard message
            isTestMessage("blockedOnCrd"), //from node3 after crd failed and this node became crd
            instanceOf(TcpDiscoveryNodeFailedMessage.class), //from node3 after crd failed
            isTestMessage("blockedOnCrd"), //msg went around the whole ring
            instanceOf(TcpDiscoveryNodeFailedMessage.class) //msg went around the whole ring
        ));

        assertThat(node1.nodes(), contains(
            asList(isNode(node1.localNodeId()), isNode(node2.localNodeId()), isNode(node3.localNodeId()))
        ));

        //expected: node2
        assertThat(node2.processedEvents(), hasOneTimeEachInOrder(
            isDiscoveryEvent(EVT_NODE_JOINED), //node3
            isTestEventMessage("successfullyWentAroundWholeRing"), //from node1
            isTestEventMessage("discardMsgBlockedBeforeNode3"), //from node1
            isTestEventMessage("blockedOnCrd"), //from node1
            isDiscoveryEvent(EVT_NODE_FAILED) //from node1
        ));

        assertThat(node2.startProcessingMessages(), hasOneTimeEachInOrder(
            instanceOf(TcpDiscoveryNodeAddedMessage.class), //this node
            instanceOf(TcpDiscoveryNodeAddedMessage.class), //node3
            isTestMessage("successfullyWentAroundWholeRing"), //from node1
            isTestMessage("discardMsgBlockedBeforeNode3"), ///from node1
            isTestMessage("blockedOnCrd"), //from node1
            instanceOf(TcpDiscoveryNodeFailedMessage.class) //from node1
        ));

        assertThat(node2.nodes(), contains(
            asList(isNode(node1.localNodeId()), isNode(node2.localNodeId()), isNode(node3.localNodeId()))
        ));

        //expected: node3 - last node in ring
        assertThat(node3.processedEvents(), hasOneTimeEachInOrder(
            isTestEventMessage("successfullyWentAroundWholeRing"), //from node2
            isTestEventMessage("discardMsgBlockedBeforeNode3"), //from node2
            isTestEventMessage("blockedOnCrd"), //from node2
            isDiscoveryEvent(EVT_NODE_FAILED) //from node2
        ));

        assertThat(node3.startProcessingMessages(), hasOneTimeEachInOrder(
            instanceOf(TcpDiscoveryNodeAddedMessage.class), //this node
            isTestMessage("successfullyWentAroundWholeRing"), //from node1
            isTestMessage("discardMsgBlockedBeforeNode3"), ///from node1
            isTestMessage("blockedOnCrd"), //unvalidated message from this node
            instanceOf(TcpDiscoveryNodeFailedMessage.class), //unvalidated message from this node
            isTestMessage("blockedOnCrd"), //validated message from node2
            instanceOf(TcpDiscoveryNodeFailedMessage.class) //validated message from node2
        ));

        assertThat(node3.nodes(), contains(
            asList(isNode(node1.localNodeId()), isNode(node2.localNodeId()), isNode(node3.localNodeId()))
        ));
    }

    /**
     * @throws Exception If fail.
     */
    @Test
    public void shouldCorrectlyResendPendingMsgLocallyWhenCrdFailed() throws Exception {
        //given:
        DiscoveryController crd = start(0);

        //Sending custom message and awaiting it finished.
        crd.sendCustomEvent(new TestDiscoveryCustomMessage("handledOnlyOnCrd"));
        crd.awaitProcessingMessage(instanceOf(TcpDiscoveryDiscardMessage.class));

        //Starting one more node
        DiscoveryController node1 = start(1);

        //Sending new message from node1 and failing coordinator before discard message would be send
        crd.blockIf(instanceOf(TcpDiscoveryDiscardMessage.class));
        node1.sendCustomEvent(new TestDiscoveryCustomMessage("discardMsgFailed"));

        crd.awaitBlocking();

        //One more message also blocked on crd
        node1.sendCustomEvent(new TestDiscoveryCustomMessage("msgAfterDiscardMsgFailed"));

        crd.awaitReceivedMessage(isTestMessage("msgAfterDiscardMsgFailed"));

        crd.failWhenBlocked();

        node1.awaitProcessedEvent(isTestEventMessage("msgAfterDiscardMsgFailed"));

        //expected: Last node which became to coordinator resend all invalidated messages to itself
        assertThat(node1.processedEvents(), hasOneTimeEachInOrder(
            isTestEventMessage("discardMsgFailed"),
//            isDiscoveryEvent(EVT_NODE_FAILED),
            isTestEventMessage("msgAfterDiscardMsgFailed")
        ));

        assertThat(node1.startProcessingMessages(), hasOneTimeEachInOrder(
            instanceOf(TcpDiscoveryNodeAddedMessage.class), //this node
            isTestMessage("discardMsgFailed"), //unvalidated message from this node
            isTestMessage("discardMsgFailed"), //validated message from crd
            isTestMessage("msgAfterDiscardMsgFailed"), //unvalidated message from this node
//            instanceOf(TcpDiscoveryNodeFailedMessage.class), //from this node
            isTestMessage("discardMsgFailed"), //from this node due to discard msg hasn't received
            isTestMessage("msgAfterDiscardMsgFailed") //validated message from this node
        ));

        //Node1 shouldn't has 'handledOnlyOnCrd' message
        assertThat(node1.startProcessingMessages(), not(hasItem(isTestMessage("handledOnlyOnCrd"))));

        assertThat(node1.nodes(), contains(isNode(node1.localNodeId())));
    }

    /**
     * @throws Exception If fail.
     */
    @Test
    public void shouldCorrectlyJoinWhenPreviousNodeFail() throws Exception {
        //given:
        DiscoveryController crd = start(0);
        DiscoveryController node1 = start(1);
        DiscoveryController node2 = start(2);

        //This message would be sent before one more node would be joined so new node shouldn't handle this.
        node2.blockIf(isTestMessage("handledOnlyOnCrdAndNode1"));

        crd.sendCustomEvent(new TestDiscoveryCustomMessage("handledOnlyOnCrdAndNode1"));

        node2.awaitBlocking();

        node1.blockIf(instanceOf(TcpDiscoveryNodeAddedMessage.class));

        IgniteInternalFuture<DiscoveryController> start3Fut = runAsync(() -> start(3, true));

        node1.awaitBlocking();

        //This messages should be handled after the new node would join.
        node1.sendCustomEvent(new TestDiscoveryCustomMessage("handledEverywhere1"));
        node1.sendCustomEvent(new TestDiscoveryCustomMessage("handledEverywhere2"));

        node1.release();

        node2.awaitReceivedMessage(isDiscoveryMessage(
            TcpDiscoveryNodeAddedMessage.class,
            msg -> msg.node().id().toString().endsWith("03") //Ensure that NodeAddedMessage from last node received.
        ));

        //When three custom messages and NodeAddedMessage stuck on this node then fail this node.
        node2.failWhenBlocked();

        DiscoveryController node3 = start3Fut.get(10_000);

        node3.awaitProcessedEvent(isTestEventMessage("handledEverywhere2"));

        //expected: All messages successfully handled.
        assertThat(crd.processedEvents(), hasOneTimeEachInOrder(
            isDiscoveryEvent(EVT_NODE_JOINED), //node1
            isDiscoveryEvent(EVT_NODE_JOINED), //node2
            isTestEventMessage("handledOnlyOnCrdAndNode1"), //from this node
            isDiscoveryEvent(EVT_NODE_JOINED), //node3
            isTestEventMessage("handledEverywhere1"), //after node3 joined
            isTestEventMessage("handledEverywhere2") //after node3 joined according to send order
//            isDiscoveryEvent(EVT_NODE_FAILED) //last because all custom messages have been in queue before this message
        ));

        assertThat(crd.startProcessingMessages(), hasOneTimeEachInOrder(
            instanceOf(TcpDiscoveryNodeAddedMessage.class), //node1
            instanceOf(TcpDiscoveryNodeAddedMessage.class), //node2
            isTestMessage("handledOnlyOnCrdAndNode1"), //from node2
            isTestMessage("handledOnlyOnCrdAndNode1"), //went around the whole ring
            instanceOf(TcpDiscoveryNodeAddedMessage.class), //node3
            isTestMessage("handledEverywhere1"), //from node1
            isTestMessage("handledEverywhere2"), //went around the whole ring
//            instanceOf(TcpDiscoveryNodeFailedMessage.class), //node2
            isTestMessage("handledEverywhere1"), //from node2
            isTestMessage("handledEverywhere2") //went around the whole ring
//            instanceOf(TcpDiscoveryNodeFailedMessage.class) //went around the whole ring
        ));

        assertThat(crd.nodes(), contains(
            asList(isNode(crd.localNodeId()), isNode(node1.localNodeId()), isNode(node3.localNodeId()))
        ));

        //expected: Almost same as coordinator except one event on join and messages which went around the whole ring
        assertThat(node1.processedEvents(), hasOneTimeEachInOrder(
            isDiscoveryEvent(EVT_NODE_JOINED), //node2
            isTestEventMessage("handledOnlyOnCrdAndNode1"),
            isDiscoveryEvent(EVT_NODE_JOINED), //node3
            isTestEventMessage("handledEverywhere1"),
            isTestEventMessage("handledEverywhere2"),
            isDiscoveryEvent(EVT_NODE_FAILED)  //node2
        ));

        assertThat(node1.startProcessingMessages(), hasOneTimeEachInOrder(
            instanceOf(TcpDiscoveryNodeAddedMessage.class), //this node
            instanceOf(TcpDiscoveryNodeAddedMessage.class), //node2
            isTestMessage("handledOnlyOnCrdAndNode1"), //validated message from crd
            instanceOf(TcpDiscoveryNodeAddedMessage.class), //node2
            isTestMessage("handledEverywhere1"), //unvalidated message from this node
            isTestMessage("handledEverywhere2"), //unvalidated message from this node
            instanceOf(TcpDiscoveryNodeFailedMessage.class), //unvalidated generated on this node
            isTestMessage("handledEverywhere1"), //validated message from crd
            isTestMessage("handledEverywhere2"), //validated message from crd
            instanceOf(TcpDiscoveryNodeFailedMessage.class) //from crd
        ));

        assertThat(node1.nodes(), contains(
            asList(isNode(crd.localNodeId()), isNode(node1.localNodeId()), isNode(node3.localNodeId()))
        ));

        //expected: Almost same as node1 except join messages and 'handledOnlyOnCrdAndNode1'
        assertThat(node3.processedEvents(), hasOneTimeEachInOrder(
            isTestEventMessage("handledEverywhere1"),
            isTestEventMessage("handledEverywhere2"),
            isDiscoveryEvent(EVT_NODE_FAILED) //node2
        ));

        //Node3 shouldn't has 'handledOnlyOnCrdAndNode1' message
        assertThat(node3.processedEvents(), not(hasItem(isTestEventMessage("handledOnlyOnCrdAndNode1"))));

        assertThat(node3.receivedMessages(), hasOneTimeEachInOrder(
            isTestMessage("handledOnlyOnCrdAndNode1"), //validated message from node1 which would be sent to crd without local handling
            instanceOf(TcpDiscoveryNodeAddedMessage.class), //this node
            isTestMessage("handledEverywhere1"), //unvalidated message from node1 after node2 failed
            isTestMessage("handledEverywhere2"), //unvalidated message from node1 after node2 failed
            instanceOf(TcpDiscoveryNodeFailedMessage.class), //unvalidated message from node1 after node2 failed
            isTestMessage("handledEverywhere1"), //validated message from crd
            isTestMessage("handledEverywhere2"), //validated message from crd
            instanceOf(TcpDiscoveryNodeFailedMessage.class) //validated message from crd
        ));

        assertThat(node3.nodes(), contains(
            asList(isNode(crd.localNodeId()), isNode(node1.localNodeId()), isNode(node3.localNodeId()))
        ));
    }

    /**
     * @throws Exception If fail.
     */
    @Test
    public void shouldCorrectlyJoinWhenCrdFail() throws Exception {
        //given:
        DiscoveryController crd = start(0);
        DiscoveryController node1 = start(1);

        //This message would be sent before joining new node but would be executed on all nodes after join
        // because of delay between AddedMsg and AddedFinishMsg.
        node1.blockIf(isTestMessage("handledEverywhereAfterJoin"));

        node1.sendCustomEvent(new TestDiscoveryCustomMessage("handledEverywhereAfterJoin"));

        node1.awaitBlocking();

        IgniteInternalFuture<DiscoveryController> start2Fut = runAsync(() -> start(2, true));

        node1.awaitReceivedMessage(isDiscoveryMessage(
            TcpDiscoveryNodeAddedMessage.class,
            msg -> msg.node().id().toString().endsWith("02") //Ensure that NodeAddedMessage from last node received.
        ));

        //This messages should be handled after the new node would join.
        node1.sendCustomEvent(new TestDiscoveryCustomMessage("handledEverywhere1"));
        node1.sendCustomEvent(new TestDiscoveryCustomMessage("handledEverywhere2"));

        //When crd received a unvalidated message it would failed.
        crd.blockIf(isTestMessage("handledEverywhereAfterJoin"));

        node1.release();

        crd.failWhenBlocked();

        DiscoveryController node2 = start2Fut.get(10_000);

        node2.awaitProcessedEvent(isDiscoveryEvent(EVT_NODE_FAILED));

        //expected: New coordinator should handled all messages.
        assertThat(node1.processedEvents(), hasOneTimeEachInOrder(
            isDiscoveryEvent(EVT_NODE_JOINED), //node2
            isTestEventMessage("handledEverywhereAfterJoin"),
            isTestEventMessage("handledEverywhere1"),
            isTestEventMessage("handledEverywhere2"),
            isDiscoveryEvent(EVT_NODE_FAILED)  //crd
        ));

        assertThat(node1.startProcessingMessages(), hasOneTimeEachInOrder(
            instanceOf(TcpDiscoveryNodeAddedMessage.class), //this node
            instanceOf(TcpDiscoveryNodeAddFinishedMessage.class), //this node
            isTestMessage("handledEverywhereAfterJoin"), //unvalidated message from this node
            instanceOf(TcpDiscoveryNodeAddedMessage.class), //node2 from crd
            isTestMessage("handledEverywhere1"), //unvalidated message from this node
            isTestMessage("handledEverywhere2"), //unvalidated message from this node
            instanceOf(TcpDiscoveryNodeAddedMessage.class), //node2 from node2 after crd is failed
//            instanceOf(TcpDiscoveryNodeAddFinishedMessage.class) //AddFinishedMessage handled implicitly on crd
            isTestMessage("handledEverywhereAfterJoin"), //unvalidated message from node2 after crd is failed
            isTestMessage("handledEverywhere1"), //unvalidated message from node2 after crd is failed
            isTestMessage("handledEverywhere2"), //unvalidated message from node2 after crd is failed
            instanceOf(TcpDiscoveryNodeAddFinishedMessage.class), //went around the whole ring
            isTestMessage("handledEverywhereAfterJoin"), //went around the whole ring
            isTestMessage("handledEverywhere1"), //went around the whole ring
            isTestMessage("handledEverywhere2") //went around the whole ring

        ));

        assertThat(node1.nodes(), contains(asList(isNode(node1.localNodeId()), isNode(node2.localNodeId()))));

        //expected: Almost same as node1 except join messages and 'handledEverywhereAfterJoin'
        assertThat(node2.processedEvents(), hasOneTimeEachInOrder(
            isTestEventMessage("handledEverywhereAfterJoin"),
            isTestEventMessage("handledEverywhere1"),
            isTestEventMessage("handledEverywhere2"),
            isDiscoveryEvent(EVT_NODE_FAILED) //crd
        ));

        assertThat(node2.receivedMessages(), hasOneTimeEachInOrder(
            isTestMessage("handledEverywhereAfterJoin"), //unvalidated message from node1 which will be reordered on new crd with NodeAddedMessage so eventually it would be handled on this node
            instanceOf(TcpDiscoveryNodeAddedMessage.class), //this node
            isTestMessage("handledEverywhere1"), //unvalidated message from node1
            isTestMessage("handledEverywhere2"), //unvalidated message from node1
            instanceOf(TcpDiscoveryNodeAddFinishedMessage.class), //node2 from node1(new crd)
            isTestMessage("handledEverywhereAfterJoin"), //validated message from node1(new crd)
            isTestMessage("handledEverywhere1"), //validated message from node1(new crd)
            isTestMessage("handledEverywhere2"), //validated message from node1(new crd)
            instanceOf(TcpDiscoveryNodeFailedMessage.class) //crd from node1(new crd)
        ));

        assertThat(node2.nodes(), contains(asList(isNode(node1.localNodeId()), isNode(node2.localNodeId()))));
    }

}