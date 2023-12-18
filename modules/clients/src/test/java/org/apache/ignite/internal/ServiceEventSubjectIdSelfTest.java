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

package org.apache.ignite.internal;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteServices;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientServices;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.events.ServiceEvent;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.events.EventType.EVTS_SERVICE_EXECUTION;
import static org.apache.ignite.events.EventType.EVT_SERVICE_METHOD_EXECUTION_FAILED;
import static org.apache.ignite.events.EventType.EVT_SERVICE_METHOD_EXECUTION_FINISHED;
import static org.apache.ignite.events.EventType.EVT_SERVICE_METHOD_EXECUTION_STARTED;
import static org.apache.ignite.internal.processors.odbc.ClientListenerProcessor.CLIENT_LISTENER_PORT;

/**
 * Tests for security subject ID in service events.
 */
public class ServiceEventSubjectIdSelfTest extends GridCommonAbstractTest {
    /** */
    private static final String SVC_NAME = "simpleService";

    /** */
    private static final Collection<ServiceEvent> evts = new ArrayList<>();

    /** */
    private static UUID nodeId;

    /** */
    private static Ignite thickClient;

    /** */
    private static IgniteClient thinClient;

    /** */
    private static CountDownLatch latch;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConnectorConfiguration(new ConnectorConfiguration());

        cfg.setIncludeEventTypes(EventType.EVTS_SERVICE_EXECUTION);

        cfg.setServiceConfiguration(
                new ServiceConfiguration()
                        .setName(SVC_NAME)
                        .setService(new SimpleServiceImpl())
                        .setTotalCount(1)
        );

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        Ignite g = startGrid("srv_1");

        g.events().localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                assert evt instanceof ServiceEvent;

                evts.add((ServiceEvent)evt);

                latch.countDown();

                return true;
            }
        }, EVTS_SERVICE_EXECUTION);

        thickClient = startClientGrid("cli_1");
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        evts.clear();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSimpleServiceThinClient() throws Exception {
        latch = new CountDownLatch(2);

        ClientConfiguration cfg = new ClientConfiguration();

        cfg.setAddresses("127.0.0.1:10801");

        thinClient = Ignition.startClient(cfg);

        nodeId = grid("srv_1").localNode()
                .id();

        ClientServices services = thinClient.services();

        SimpleService simpleSvc = services.serviceProxy("simpleService", SimpleService.class, 1_000);

        String simpleValue = simpleSvc.simpleMethod("simpleValue");

        assertEquals("simpleValue", simpleValue);

        assertTrue("Failed to wait for service execution.", latch.await(1, SECONDS));

        assertEquals(2, evts.size());

        Iterator<ServiceEvent> it = evts.iterator();

        assert it.hasNext();

        ServiceEvent evt = it.next();

        assert evt != null;

        assertEquals(EVT_SERVICE_METHOD_EXECUTION_STARTED, evt.type());
        assertEquals(nodeId, evt.subjectId());
        assertEquals("simpleService", evt.serviceName());
        assertEquals("simpleMethod", evt.methodName());

        assert it.hasNext();

        evt = it.next();

        assert evt != null;

        assertEquals(EVT_SERVICE_METHOD_EXECUTION_FINISHED, evt.type());
        assertEquals(nodeId, evt.subjectId());
        assertEquals("simpleService", evt.serviceName());
        assertEquals("simpleMethod", evt.methodName());

        thinClient.close();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSimpleService() throws Exception {
        latch = new CountDownLatch(2);

        nodeId = thickClient.cluster()
                .localNode()
                .id();

        IgniteServices services = thickClient.services();

        SimpleService simpleSvc = services.serviceProxy("simpleService", SimpleService.class, true);

        String simpleValue = simpleSvc.simpleMethod("simpleValue");

        assertEquals("simpleValue", simpleValue);

        assertTrue("Failed to wait for service execution.", latch.await(1, SECONDS));

        assertEquals(2, evts.size());

        Iterator<ServiceEvent> it = evts.iterator();

        assert it.hasNext();

        ServiceEvent evt = it.next();

        assert evt != null;

        assertEquals(EVT_SERVICE_METHOD_EXECUTION_STARTED, evt.type());
        assertEquals(nodeId, evt.subjectId());
        assertEquals("simpleService", evt.serviceName());
        assertEquals("simpleMethod", evt.methodName());

        assert it.hasNext();

        evt = it.next();

        assert evt != null;

        assertEquals(EVT_SERVICE_METHOD_EXECUTION_FINISHED, evt.type());
        assertEquals(nodeId, evt.subjectId());
        assertEquals("simpleService", evt.serviceName());
        assertEquals("simpleMethod", evt.methodName());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSimpleFailureService() throws Exception {
        latch = new CountDownLatch(2);

        nodeId = thickClient.cluster()
                .localNode()
                .id();

        IgniteServices services = thickClient.services();

        SimpleService simpleSvc = services.serviceProxy("simpleService", SimpleService.class, true);

        GridTestUtils.assertThrows(null, simpleSvc::simpleFailureMethod, RuntimeException.class, "test exception");

        assertTrue("Failed to wait for service execution.", latch.await(1, SECONDS));

        assertEquals(2, evts.size());

        Iterator<ServiceEvent> it = evts.iterator();

        assert it.hasNext();

        ServiceEvent evt = it.next();

        assert evt != null;

        assertEquals(EVT_SERVICE_METHOD_EXECUTION_STARTED, evt.type());
        assertEquals(nodeId, evt.subjectId());
        assertEquals("simpleService", evt.serviceName());
        assertEquals("simpleFailureMethod", evt.methodName());

        assert it.hasNext();

        evt = it.next();

        assert evt != null;

        assertEquals(EVT_SERVICE_METHOD_EXECUTION_FAILED, evt.type());
        assertEquals(nodeId, evt.subjectId());
        assertEquals("simpleService", evt.serviceName());
        assertEquals("simpleFailureMethod", evt.methodName());
    }

    /** */
    public interface SimpleService extends Service {

        /** */
        String simpleMethod(String simpleArg);

        /** */
        String simpleFailureMethod();
    }

    /** */
    public static class SimpleServiceImpl implements SimpleService {
        /** */
        @Override public String simpleMethod(String simpleArg) {
            return simpleArg;
        }

        /** */
        @Override public String simpleFailureMethod() {
            throw new RuntimeException("test exception");
        }

        /** {@inheritDoc} */
        @Override public void init(ServiceContext ctx) throws Exception {}

        /** {@inheritDoc} */
        @Override public void execute(ServiceContext ctx) throws Exception {}

        /** {@inheritDoc} */
        @Override public void cancel(ServiceContext ctx) {}
    }
}
