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

package org.apache.ignite.console.common;

import org.apache.ignite.internal.util.typedef.internal.S;
import org.springframework.web.socket.WebSocketSession;

import static org.springframework.web.socket.server.support.HttpSessionHandshakeInterceptor.HTTP_SESSION_ID_ATTR_NAME;

/**
 * Session attribute description.
 */
public class SessionAttribute {
    /** Session id. */
    private String sesId;

    /** Attribute name. */
    private String name;

    /**
     * @param sesId Session id.
     * @param name Attribute name.
     */
    public SessionAttribute(String sesId, String name) {
        this.sesId = sesId;
        this.name = name;
    }

    /**
     * @param ses A WebSocket session abstraction.
     * @param name Attribute name.
     */
    public SessionAttribute(WebSocketSession ses, String name) {
        this((String)ses.getAttributes().get(HTTP_SESSION_ID_ATTR_NAME), name);
    }

    /**
     * @return Session id.
     */
    public String getSessionId() {
        return sesId;
    }

    /**
     * @return Attribute name.
     */
    public String getName() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SessionAttribute.class, this);
    }
}
