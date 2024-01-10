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

package org.apache.ignite.client.events;

import java.util.EventListener;

/**
 * Listener for connection events.
 */
public interface ConnectionEventListener extends EventListener {
    /**
     * @param event Handshake start event.
     */
    default void onHandshakeStart(HandshakeStartEvent event) {
        // No-op.
    }

    /**
     * @param event Handshake success event.
     */
    default void onHandshakeSuccess(HandshakeSuccessEvent event) {
        // No-op.
    }

    /**
     * @param event Handshake fail event.
     */
    default void onHandshakeFail(HandshakeFailEvent event) {
        // No-op.
    }

    /**
     * @param event Connection closed event (with or without exception).
     */
    default void onConnectionClosed(ConnectionClosedEvent event) {
        // No-op.
    }
}
