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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

/**
 * Browsers web sockets handler.
 */
@Service
public class BrowsersHandler extends TextWebSocketHandler {
    /** */
    private static final Logger log = LoggerFactory.getLogger(BrowsersHandler.class);

    /** */
    private final WebSocketsManager wsm;

    /**
     * @param wsm Web sockets manager.
     */
    public BrowsersHandler(WebSocketsManager wsm) {
        this.wsm = wsm;
    }

    /** {@inheritDoc} */
    @Override public void handleTextMessage(WebSocketSession ws, TextMessage msg) {
        try {
            wsm.handleBrowserEvents(ws, msg);
        }
        catch (Throwable e) {
            log.error("Failed to process message from browser [session=" + ws + ", msg=" + msg + "]", e);
        }
    }

    /** {@inheritDoc} */
    @Override public void afterConnectionEstablished(WebSocketSession ws) {
        log.info("Browser session opened [socket=" + ws + "]");

        ws.setTextMessageSizeLimit(10 * 1024 * 1024);

        wsm.onBrowserConnect(ws);
    }

    /** {@inheritDoc} */
    @Override public void afterConnectionClosed(WebSocketSession ws, CloseStatus status) {
        log.info("Browser session closed [socket=" + ws + ", status=" + status + "]");

        wsm.closeBrowserSession(ws);
    }
}
