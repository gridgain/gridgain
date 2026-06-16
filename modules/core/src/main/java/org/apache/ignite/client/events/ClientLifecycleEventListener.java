/*
 * Copyright 2026 GridGain Systems, Inc. and Contributors.
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
 * Listener for client lifecycle events.
 */
public interface ClientLifecycleEventListener extends EventListener {
    /**
     * @param evt Client start event.
     */
    default void onClientStart(ClientStartEvent evt) {
        // No-op.
    }

    /**
     * @param evt Client fail event.
     */
    default void onClientFail(ClientFailEvent evt) {
        // No-op.
    }

    /**
     * @param evt Client stop event.
     */
    default void onClientStop(ClientStopEvent evt) {
        // No-op.
    }
}
