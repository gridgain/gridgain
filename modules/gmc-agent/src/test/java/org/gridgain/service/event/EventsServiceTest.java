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

package org.gridgain.service.event;

import java.util.List;
import java.util.UUID;
import com.google.common.collect.Lists;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.visor.event.VisorGridEvent;
import org.apache.ignite.internal.visor.util.VisorEventMapper;
import org.apache.ignite.testframework.GridTestNode;
import org.gridgain.dto.event.ClusterNodeBean;
import org.gridgain.service.AbstractServiceTest;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.gridgain.agent.StompDestinationsUtils.buildEventsDest;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

/**
 * Events service test.
 */
public class EventsServiceTest extends AbstractServiceTest {
    /**
     * Should register handler and export events.
     */
    @Test
    public void shouldSendEventsList() {
        EventsService srvc = new EventsService(getMockContext(), mgr);

        List<VisorGridEvent> evts = getEvents();

        srvc.onEvents(UUID.randomUUID(), evts);

        ArgumentCaptor<String> destCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Object> payloadCaptor = ArgumentCaptor.forClass(Object.class);
        verify(mgr, timeout(100).times(1)).send(destCaptor.capture(), payloadCaptor.capture());

        List<VisorGridEvent> actualEvts = (List<VisorGridEvent>) payloadCaptor.getValue();

        Assert.assertEquals(buildEventsDest(UUID.fromString("a-a-a-a-a")), destCaptor.getValue());
        Assert.assertEquals(evts.size(), actualEvts.size());
    }

    /**
     * @return Events list.
     */
    private List<VisorGridEvent> getEvents() {
        ClusterNode rmv = new ClusterNodeBean(new GridTestNode(UUID.randomUUID()));
        DiscoveryEvent evt = new DiscoveryEvent(rmv, "msg", EVT_NODE_LEFT, rmv);

        return Lists.newArrayList(new VisorEventMapper().apply(evt));
    }
}
