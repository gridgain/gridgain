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

package org.apache.ignite.console.web.socket;

import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.console.TestGridConfiguration;
import org.apache.ignite.console.common.SessionAttribute;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.json.JsonObject;
import org.apache.ignite.console.services.SessionsService;
import org.apache.ignite.console.websocket.TopologySnapshot;
import org.apache.ignite.console.websocket.WebSocketRequest;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.security.web.authentication.preauth.PreAuthenticatedAuthenticationToken;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.web.socket.WebSocketSession;

import static org.apache.ignite.console.utils.TestUtils.cleanPersistenceDir;
import static org.apache.ignite.console.utils.TestUtils.stopAllGrids;
import static org.apache.ignite.console.utils.Utils.entriesToMap;
import static org.apache.ignite.console.utils.Utils.entry;
import static org.apache.ignite.console.utils.Utils.fromJson;
import static org.apache.ignite.console.utils.Utils.toJson;
import static org.apache.ignite.console.websocket.WebSocketEvents.CLUSTER_LOGOUT;
import static org.apache.ignite.console.websocket.WebSocketEvents.NODE_REST;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 *  Transition service test.
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = {TestGridConfiguration.class})
public class BrowsersServiceSelfTest {
    /** Browsers service. */
    @Autowired
    private BrowsersService browsersSrvc;

    /** Browsers service. */
    @MockBean
    private AgentsService agentsSrvc;

    /** Sessions service. */
    @MockBean
    private SessionsService sesSrvc;

    /** Clusters service. */
    @MockBean
    private ClustersRepository clustersRepo;

    /** Ignite instance. */
    @Autowired
    private Ignite ignite;

    /**
     * @throws Exception If failed.
     */
    @BeforeClass
    public static void setup() throws Exception {
        stopAllGrids();
        cleanPersistenceDir();
    }

    /**
     * @throws Exception If failed.
     */
    @AfterClass
    public static void tearDown() throws Exception {
        stopAllGrids();
        cleanPersistenceDir();
    }

    /** */
    @Test
    public void testSendToAgent() throws Exception {
        String clusterId = UUID.randomUUID().toString();

        WebSocketRequest req = new WebSocketRequest();

        req.setRequestId(UUID.randomUUID().toString());
        req.setEventType(NODE_REST);

        req.setPayload(toJson(new JsonObject(
            Stream.<Map.Entry<String, Object>>of(
                entry("clusterId", clusterId)
            ).collect(entriesToMap()))
        ));

        AgentKey key = new AgentKey(UUID.randomUUID(), clusterId);

        browsersSrvc.sendToAgent(key, req);

        ArgumentCaptor<AgentRequest> captor = ArgumentCaptor.forClass(AgentRequest.class);

        verify(agentsSrvc, times(1)).sendLocally(captor.capture());

        assertEquals(ignite.cluster().localNode().id(), captor.getValue().getSrcNid());
        assertEquals(key, captor.getValue().getKey());
        assertEquals(req, captor.getValue().getEvent());
    }

    /** */
    @Test
    public void shouldRemoveSessionAttributeOnClusterLogout() {
        String clusterId = UUID.randomUUID().toString();

        WebSocketRequest req = new WebSocketRequest();

        req.setRequestId(UUID.randomUUID().toString());
        req.setEventType(CLUSTER_LOGOUT);

        req.setPayload(clusterId);

        WebSocketSession ses = mock(WebSocketSession.class);

        when(ses.getPrincipal()).thenReturn(new PreAuthenticatedAuthenticationToken(
            new Account(null, null, null, null, null, null, null), null));

        browsersSrvc.handleEvent(ses, req);

        verify(sesSrvc, times(1)).remove(any(SessionAttribute.class));
    }

    /** */
    @Test
    public void shouldAddSessionTokenToRequest() throws Exception {
        String clusterId = UUID.randomUUID().toString();

        WebSocketRequest req = new WebSocketRequest();

        req.setRequestId(UUID.randomUUID().toString());
        req.setEventType(NODE_REST);

        req.setPayload(toJson(new JsonObject(
            Stream.<Map.Entry<String, Object>>of(
                entry("clusterId", clusterId),
                entry("params", F.asMap("user", "user", "password", "password"))
            ).collect(entriesToMap()))
        ));

        WebSocketSession ses = mock(WebSocketSession.class);

        Account acc = new Account(null, null, null, null, null, null, null);

        when(sesSrvc.get(any(SessionAttribute.class))).thenReturn("testToken");

        when(ses.getPrincipal()).thenReturn(new PreAuthenticatedAuthenticationToken(acc, null));

        TopologySnapshot top = new TopologySnapshot();
        top.setSecured(true);

        when(clustersRepo.get(eq(clusterId))).thenReturn(top);

        browsersSrvc.handleEvent(ses, req);

        ArgumentCaptor<AgentRequest> captor = ArgumentCaptor.forClass(AgentRequest.class);

        verify(agentsSrvc, times(1)).sendLocally(captor.capture());

        assertEquals(ignite.cluster().localNode().id(), captor.getValue().getSrcNid());
        assertEquals(new AgentKey(acc.getId(), clusterId), captor.getValue().getKey());

        WebSocketRequest req0 = captor.getValue().getEvent();

        assertEquals(req.getRequestId(), req0.getRequestId());
        assertEquals(req.getEventType(), req0.getEventType());

        JsonObject payload = fromJson(req0.getPayload());
        JsonObject params = payload.getJsonObject("params");

        assertEquals("user", params.get("user"));
        assertEquals("password", params.get("password"));
        assertEquals("testToken", params.get("sessionToken"));
    }
}
