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
import java.nio.ByteBuffer;

import org.apache.ignite.IgniteCheckedException;
import org.jetbrains.annotations.Nullable;

/**
 * Client connection: abstracts away sending and receiving messages.
 */
public interface ClientConnection extends AutoCloseable {
    /**
     * Sends a message.
     *
     * @param msg Message buffer.
     * @param onDone Callback to be invoked when asynchronous send operation completes.
     */
    void send(ByteBuffer msg, @Nullable Runnable onDone) throws IgniteCheckedException;

    /**
     * Gets local address of this session.
     *
     * @return Local network address or {@code null} if non-socket communication is used.
     */
    @Nullable InetSocketAddress localAddress();

    /**
     * Gets address of remote peer on this session.
     *
     * @return Address of remote peer or {@code null} if non-socket communication is used.
     */
    @Nullable InetSocketAddress remoteAddress();

    /**
     * Closes the connection.
     */
    @Override void close();
}
