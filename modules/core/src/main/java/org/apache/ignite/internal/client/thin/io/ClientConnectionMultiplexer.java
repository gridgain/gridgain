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

package org.apache.ignite.internal.client.thin.io;

import java.net.InetSocketAddress;

import org.apache.ignite.client.ClientConnectionException;

/**
 * Client connection multiplexer: manages multiple connections with a shared resource pool (worker threads, etc).
 */
public interface ClientConnectionMultiplexer {
    /**
     * Initializes this instance.
     */
    void start();

    /**
     * Stops this instance.
     */
    void stop();

    /**
     * Opens a new connection.
     *
     * @param addr Address.
     * @param msgHnd Incoming message handler.
     * @param stateHnd Connection state handler.
     * @return Created connection.
     * @throws ClientConnectionException when connection can't be established.
     */
    ClientConnection open(
            InetSocketAddress addr,
            ClientMessageHandler msgHnd,
            ClientConnectionStateHandler stateHnd)
            throws ClientConnectionException;
}
