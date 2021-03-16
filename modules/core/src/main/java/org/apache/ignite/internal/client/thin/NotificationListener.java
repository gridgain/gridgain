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

package org.apache.ignite.internal.client.thin;

import java.nio.ByteBuffer;

/**
 * Server to client notification listener.
 */
interface NotificationListener {
    /**
     * Accept notification.
     *
     * @param ch Client channel which was notified.
     * @param op Client operation.
     * @param rsrcId Resource id.
     * @param payload Notification payload or {@code null} if there is no payload.
     * @param err Error.
     */
    public void acceptNotification(ClientChannel ch, ClientOperation op, long rsrcId, ByteBuffer payload, Exception err);
}
