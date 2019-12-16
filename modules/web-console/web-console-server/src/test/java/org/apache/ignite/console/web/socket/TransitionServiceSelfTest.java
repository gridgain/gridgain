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

import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.console.AbstractSelfTest;
import org.apache.ignite.console.dto.Announcement;
import org.apache.ignite.console.websocket.WebSocketEvent;
import org.apache.ignite.console.websocket.WebSocketRequest;
import org.apache.ignite.console.websocket.WebSocketResponse;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;

import static java.util.Collections.singleton;
import static org.apache.ignite.console.messages.WebConsoleMessageSource.message;
import static org.apache.ignite.console.websocket.WebSocketEvents.ADMIN_ANNOUNCEMENT;
import static org.apache.ignite.console.websocket.WebSocketEvents.ERROR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 *  Transition service test.
 */
public class TransitionServiceSelfTest extends AbstractSelfTest {
    /** Transition service. */
    @Autowired
    private TransitionService transitionSrvc;

    /** Browsers service. */
    @MockBean
    private BrowsersService browsersSrvc;

    /** Agents repository. */
    @MockBean
    private AgentsRepository agentsRepo;

    /** Announcement captor. */
    @Captor
    private ArgumentCaptor<WebSocketEvent<Announcement>> annCaptor;

    /** */
    @Test
    public void testSendAnnouncement() {
        Announcement ann = new Announcement(UUID.randomUUID(), "test", true);

        transitionSrvc.broadcastToBrowsers(ann);

        verify(browsersSrvc, times(1)).sendToBrowsers(isNull(UserKey.class), annCaptor.capture());

        WebSocketEvent<Announcement> evt = annCaptor.getValue();

        assertNotNull(evt.getRequestId());
        assertEquals(ADMIN_ANNOUNCEMENT, evt.getEventType());
        assertEquals(ann, evt.getPayload());
    }

    /** */
    @Test
    public void sendToNotConnectedCluster() {
        WebSocketRequest req = new WebSocketRequest();

        req.setRequestId(UUID.randomUUID().toString());

        transitionSrvc.sendToAgent(new AgentKey(UUID.randomUUID(), UUID.randomUUID().toString()), req);

        ArgumentCaptor<WebSocketResponse> resCaptor = ArgumentCaptor.forClass(WebSocketResponse.class);

        verify(browsersSrvc, times(1)).processResponse(resCaptor.capture());

        WebSocketResponse res = resCaptor.getValue();

        assertEquals(req.getRequestId(), res.getRequestId());
        assertEquals(ERROR, res.getEventType());
        assertEquals(message("err.agent-not-found"), ((Map<String, String>)res.getPayload()).get("message"));
    }

    /** */
    @Test
    public void sendToClusterWithBrokenIndex() {
        AgentKey key = new AgentKey(UUID.randomUUID(), UUID.randomUUID().toString());

        UUID nid = UUID.randomUUID();

        when(agentsRepo.get(key)).thenReturn(singleton(nid), Collections.emptySet());

        WebSocketRequest req = new WebSocketRequest();

        req.setRequestId(UUID.randomUUID().toString());

        transitionSrvc.sendToAgent(key, req);

        verify(agentsRepo, times(2)).get(eq(key));

        ArgumentCaptor<WebSocketResponse> resCaptor = ArgumentCaptor.forClass(WebSocketResponse.class);

        verify(browsersSrvc, times(1)).processResponse(resCaptor.capture());

        WebSocketResponse res = resCaptor.getValue();

        assertEquals(req.getRequestId(), res.getRequestId());
        assertEquals(ERROR, res.getEventType());
        assertEquals(message("err.agent-not-found"), ((Map<String, String>)res.getPayload()).get("message"));
    }
}
