/*
 * Copyright 2023 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.client.thin.events;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.EnumSet;
import java.util.EventListener;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.events.ConnectionClosedEvent;
import org.apache.ignite.client.events.ConnectionEvent;
import org.apache.ignite.client.events.ConnectionEventListener;
import org.apache.ignite.client.events.HandshakeFailEvent;
import org.apache.ignite.client.events.HandshakeStartEvent;
import org.apache.ignite.client.events.HandshakeSuccessEvent;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.client.thin.ProtocolVersion;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests connection event listeners of a thin client.
 */
public class IgniteClientConnectionEventListenerTest extends GridCommonAbstractTest {
    /** */
    private static final InetAddress LOCALHOST;

    /** */
    private static final int SRV_PORT = 10800;

    static {
        try {
            LOCALHOST = InetAddress.getByName("127.0.0.1");
        }
        catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    /** */
    @Test
    public void testBasic() throws Exception {
        ProtocolVersion srvVer = ProtocolVersion.V1_6_0;
        try (FakeIgniteServer srv = new FakeIgniteServer(LOCALHOST, SRV_PORT, log(), srvVer)) {
            srv.start();

            Map<Class<? extends ConnectionEvent>, ConnectionEvent> evSet = new ConcurrentHashMap<>();
            ConnectionEventListener lsnr = new ConnectionEventListener() {
                @Override public void onHandshakeStart(HandshakeStartEvent event) {
                    evSet.put(event.getClass(), event);
                }

                @Override public void onHandshakeSuccess(HandshakeSuccessEvent event) {
                    evSet.put(event.getClass(), event);
                }

                @Override public void onConnectionClosed(ConnectionClosedEvent event) {
                    evSet.put(event.getClass(), event);
                }
            };

            long startNano = System.nanoTime();
            try (IgniteClient ignored = startClient(lsnr)) {
                GridTestUtils.waitForCondition(() -> evSet.size() == 2, 5000);

                HandshakeStartEvent hsStartEv = (HandshakeStartEvent)evSet.get(HandshakeStartEvent.class);

                assertEquals(hsStartEv.connectionDescription().protocol(), "ProtocolContext [version=" + ProtocolVersion.CURRENT_VER
                    + ", features=[]]");
                assertEquals(LOCALHOST, hsStartEv.connectionDescription().remoteAddress().getAddress());
                assertEquals(SRV_PORT, hsStartEv.connectionDescription().remoteAddress().getPort());
                assertEquals(LOCALHOST, hsStartEv.connectionDescription().localAddress().getAddress());
                assertEquals(null, hsStartEv.connectionDescription().serverNodeId());

                HandshakeSuccessEvent hsSuccEv = (HandshakeSuccessEvent)evSet.get(HandshakeSuccessEvent.class);

                assertEquals(hsSuccEv.connectionDescription().protocol(), "ProtocolContext [version=" + srvVer + ", features=[]]");
                assertEquals(LOCALHOST, hsSuccEv.connectionDescription().remoteAddress().getAddress());
                assertEquals(SRV_PORT, hsSuccEv.connectionDescription().remoteAddress().getPort());
                assertEquals(LOCALHOST, hsSuccEv.connectionDescription().localAddress().getAddress());
                assertEquals(srv.nodeId(), hsSuccEv.connectionDescription().serverNodeId());
                assertTrue(System.nanoTime() - startNano >= hsSuccEv.elapsedTime(TimeUnit.NANOSECONDS));
            }

            GridTestUtils.waitForCondition(() -> evSet.size() == 3, 5000);

            ConnectionClosedEvent closedEv = (ConnectionClosedEvent)evSet.get(ConnectionClosedEvent.class);

            assertEquals(closedEv.connectionDescription().protocol(), "ProtocolContext [version=" + srvVer + ", features=[]]");
            assertEquals(LOCALHOST, closedEv.connectionDescription().remoteAddress().getAddress());
            assertEquals(SRV_PORT, closedEv.connectionDescription().remoteAddress().getPort());
            assertEquals(LOCALHOST, closedEv.connectionDescription().localAddress().getAddress());
            assertEquals(srv.nodeId(), closedEv.connectionDescription().serverNodeId());
        }
    }

    /** */
    @Test
    public void testUnsupportedProtocolFail() {
        ProtocolVersion unsupportedProto = new ProtocolVersion((short)255, (short)0, (short)0);
        assertTrue(unsupportedProto.compareTo(ProtocolVersion.CURRENT_VER) > 0);

        long startNano = System.nanoTime();
        testFail(
            () -> new FakeIgniteServer(LOCALHOST, SRV_PORT, log(), unsupportedProto),
            (HandshakeFailEvent event, Throwable hsErr) -> {
                assertTrue(System.nanoTime() - startNano >= event.elapsedTime(TimeUnit.NANOSECONDS));
                assertEquals(hsErr, event.throwable());
            },
            HandshakeFailEvent.class,
            ProtocolVersion.V1_7_1
        );
    }

    /** */
    @Test
    public void testHandshakeFail() {
        Stream.of(FakeIgniteServer.ErrorType.HANDSHAKE_CONNECTION_ERROR, FakeIgniteServer.ErrorType.HANDSHAKE_ERROR,
            FakeIgniteServer.ErrorType.AUTHENTICATION_ERROR).forEach(errType -> {
                System.out.println(errType);
                AtomicLong startNano = new AtomicLong(System.nanoTime());

                ProtocolVersion expectedVer = errType == FakeIgniteServer.ErrorType.HANDSHAKE_ERROR
                        ? ProtocolVersion.V1_7_0
                        : ProtocolVersion.V1_7_1;

                testFail(
                    () -> new FakeIgniteServer(LOCALHOST, SRV_PORT, log(), EnumSet.of(errType)),
                    (HandshakeFailEvent event, Throwable hsErr) -> {
                        assertTrue(System.nanoTime() - startNano.get() >= event.elapsedTime(TimeUnit.NANOSECONDS));
                        assertEquals(hsErr, event.throwable());
                    },
                    HandshakeFailEvent.class,
                    expectedVer
                );
            });
    }

    /** */
    @Test
    public void testConnectionLost() {
        testFail(
            () -> new FakeIgniteServer(LOCALHOST, SRV_PORT, log(), EnumSet.of(FakeIgniteServer.ErrorType.CONNECTION_ERROR)),
            IgniteClient::cacheNames,
            (ev, t) -> {},
            ConnectionClosedEvent.class,
            ProtocolVersion.V1_7_0
        );
    }

    /** */
    private <Event extends ConnectionEvent> void testFail(
        Supplier<FakeIgniteServer> srvFactory,
        BiConsumer<Event, Throwable> checkEventAction,
        Class<Event> eventCls,
        ProtocolVersion expectedProtoVer
    ) {
        testFail(srvFactory, client -> fail(), checkEventAction, eventCls, expectedProtoVer);
    }

    /** */
    private <Event extends ConnectionEvent> void testFail(
        Supplier<FakeIgniteServer> srvFactory,
        Consumer<IgniteClient> clientAction,
        BiConsumer<Event, Throwable> checkEventAction,
        Class<Event> eventCls,
        ProtocolVersion expectedProtoVer
    ) {
        try (FakeIgniteServer srv = srvFactory.get()) {
            srv.start();

            Throwable hsErr = null;
            Map<Class<? extends ConnectionEvent>, ConnectionEvent> evSet = new ConcurrentHashMap<>();
            ConnectionEventListener lsnr = new ConnectionEventListener() {
                @Override public void onConnectionClosed(ConnectionClosedEvent event) {
                    evSet.put(event.getClass(), event);
                }

                @Override public void onHandshakeFail(HandshakeFailEvent event) {
                    evSet.put(event.getClass(), event);
                }
            };

            try (IgniteClient cli = startClient(lsnr)) {
                clientAction.accept(cli);
            }
            catch (Throwable e) {
                hsErr = e;
            }

            GridTestUtils.waitForCondition(() -> !evSet.isEmpty(), 5000);
            assertEquals(1, evSet.size());

            Event failEv = (Event)evSet.get(eventCls);

            assertNotNull(failEv);
            assertEquals("ProtocolContext [version=" + expectedProtoVer + ", features=[]]",
                    failEv.connectionDescription().protocol());
            assertEquals(LOCALHOST, failEv.connectionDescription().remoteAddress().getAddress());
            assertEquals(SRV_PORT, failEv.connectionDescription().remoteAddress().getPort());
            assertEquals(LOCALHOST, failEv.connectionDescription().localAddress().getAddress());

            if (failEv.connectionDescription().serverNodeId() != null)
                assertEquals(srv.nodeId(), failEv.connectionDescription().serverNodeId());

            checkEventAction.accept(failEv, hsErr);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed event test", e);
        }
    }

    /** */
    private IgniteClient startClient(EventListener... listeners) {
        String addr = LOCALHOST.getHostName() + ":" + SRV_PORT;

        return Ignition.startClient(new ClientConfiguration().setAddresses(addr).setEventListeners(listeners));
    }
}
