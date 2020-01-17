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

package org.apache.ignite.agent.config;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.ignite.IgniteException;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.WebSocketHandler;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.WebSocketHandlerDecorator;
import org.springframework.web.socket.handler.WebSocketHandlerDecoratorFactory;

/**
 * Factory of decorated websockets for tests.
 */
public class TestWebsocketDecoratedFactory implements WebSocketHandlerDecoratorFactory {
    /** Sessions. */
    private Map<String, WebSocketSession> sessions = new HashMap<>();

    /** {@inheritDoc} */
    @Override public WebSocketHandler decorate(WebSocketHandler hnd) {
        return new WebSocketHandlerDecorator(hnd) {
            /** {@inheritDoc} */
            @Override public void afterConnectionEstablished(final WebSocketSession ses) throws Exception {
                sessions.put(ses.getId(), ses);

                super.afterConnectionEstablished(ses);
            }
        };
    }

    /**
     * Disconnect all clients.
     */
    public void disconnectAllClients() {
        Iterator<Entry<String, WebSocketSession>> it = sessions.entrySet().iterator();

        while (it.hasNext()) {
            Entry<String, WebSocketSession> entry = it.next();

            try {
                entry.getValue().close(CloseStatus.NOT_ACCEPTABLE);
            }
            catch (IOException e) {
                throw new IgniteException(e);
            }

            it.remove();
        }
    }

    /**
     * @return Number of connected clients.
     */
    public int getConnectedSessionsCount() {
        return sessions.entrySet().size();
    }
}
