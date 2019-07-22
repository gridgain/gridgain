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

import org.apache.ignite.console.websocket.WebSocketEvent;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Event to agent.
 */
public class AgentEvent {
    /** Agent key. */
    private AgentKey agentKey;

    /** Event. */
    private WebSocketEvent evt;

    /**
     * @param cluster Cluster.
     * @param evt Event.
     */
    public AgentEvent(AgentKey cluster, WebSocketEvent evt) {
        agentKey = cluster;
        this.evt = evt;
    }

    /**
     * @return value of agent key.
     */
    public AgentKey getAgentKey() {
        return agentKey;
    }

    /**
     * @return value of event.
     */
    public WebSocketEvent getEvent() {
        return evt;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(AgentEvent.class, this);
    }
}
