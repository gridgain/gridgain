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

/** */
public abstract class ConnectionEvent {
    /** */
    private final ConnectionDescription conn;

    /**
     * @param conn Connection description.
     */
    protected ConnectionEvent(ConnectionDescription conn) {
        this.conn = conn;
    }

    /**
     * @return Connection description.
     */
    public ConnectionDescription connectionDescription() {
        return conn;
    }
}
